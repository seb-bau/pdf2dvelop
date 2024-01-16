import configparser
import os.path
import PyPDF2
import re
import shutil
import logging
import sys
import time
from pathlib import Path
from dvelopdmspy.dvelopdmspy import DvelopDmsPy
from wowicache.models import WowiCache, Building


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


logger = logging.getLogger('root')
sys.excepthook = handle_unhandled_exception


def cleanup_backup_folder(path: str, after_days: int):
    if after_days == 0:
        return True
    try:
        files = os.listdir(path)
        current_time = time.time()
        day = 86400
        for file in files:
            if file.endswith("*.pdf"):
                file_path = os.path.join(path, file)
                file_time = os.stat(file_path).st_mtime
                if file_time < (current_time - (day * after_days)):
                    os.remove(file_path)
    except (OSError, IOError) as e:
        logger.error(f"Error while cleaning up backup path {path}: {str(e)}")
        return False
    return True


def remove_leading_zeroes(input_str: str):
    output_str = re.sub(r'(\D)0*(\d+)', r'\1\2', input_str)
    if output_str is None:
        return input_str

    return output_str


def address_to_building(paddr: str, cache: WowiCache, config: configparser.ConfigParser):
    paddr = paddr.replace(" ", "").strip().lower()
    buildings = cache.session.query(Building).all()
    building_min = config.getint("cache_settings", "building_min", fallback=1)
    building_max = config.getint("cache_settings", "building_max", fallback=0)
    building_delimiter = config.get("cache_settings", "building_delimiter", fallback=None)
    for entry in buildings:
        if entry.id_num is None:
            continue
        if building_delimiter is not None and building_min > 1 and building_max > 0:
            try:
                building_number = int(entry.id_num.split(building_delimiter)[-1])
            except ValueError:
                continue
            if building_number < building_min or (building_number > 0 and building_number > building_max):
                continue
        if entry.street_complete is not None:
            street = entry.street_complete.replace(" ", "").strip().lower()
            if paddr in street:
                return entry
            elif paddr.replace("str.", "straße") in street:
                return entry

    return None


def get_props_from_doc(pdoctext: str, pprops: list, cache: WowiCache, pconfig: configparser.ConfigParser,
                       dms: DvelopDmsPy):
    ret_props = []
    stored_vals = {}
    for item in pprops:
        prop_value = None
        item_id = item.get("prop_id")
        item_type = item.get("type").lower()
        item_lookup = item.get("lookup")
        if item_type == "static":
            prop_value = item.get("value")
        elif item_type == "dynamic":
            logger.debug(f"dynamic_item: {item_id} regex: {item.get('regex')}")
            prop_regex = item.get('regex')
            prop_value = re.search(prop_regex, pdoctext)
            logger.debug(f"regex_groups: {prop_value}")
            regex_group = int(item.get("regex_group"))
            if prop_value:
                logger.debug(f"regex_match: {prop_value.group(regex_group).strip()}")
            if prop_value is not None:
                replace_value = item.get("replace")
                if replace_value is not None:
                    prop_value = replace_value
                else:
                    prop_value = prop_value.group(regex_group).strip()
        elif item_type == "combine":
            item_value = str(item.get("value"))
            rvars = re.findall('<(.*?)>', item_value, re.DOTALL)
            if rvars is not None:
                for var_match in rvars:
                    stored_val = stored_vals.get(var_match)
                    if stored_val is None:
                        stored_val = ""
                    logger.debug(f"Match: {[var_match]}")
                    logger.debug(f"Stored: {stored_val}")

                    item_value = item_value.replace(f"<{var_match}>", stored_val)
            item_value = item_value.replace("  ", " ").strip()
            prop_value = item_value

        if item_lookup is not None and prop_value is not None and len(prop_value.strip()) > 0:
            logger.debug(f"item_lookup_val: {item_lookup}")
            item_raw_dvelop = item.get("dvelop_raw_guid")
            if item_raw_dvelop is not None and len(item_raw_dvelop) > 30:
                dms.add_upload_property("", prop_value, item_raw_dvelop, ret_props)
            if item_lookup.lower() == "building_address":
                prop_value = remove_leading_zeroes(prop_value)
                prop_value = prop_value.replace("STRABE", "STRAßE")
                prop_lookup_item: Building
                logger.debug(f"address_to_building input: {prop_value}")
                prop_lookup_item = address_to_building(prop_value, cache=cache, config=pconfig)
                logger.debug(f"address_to_building output: {prop_lookup_item}")
                if prop_lookup_item is not None:
                    # print(f"{prop_value} --> {prop_lookup_item.id_num}")

                    parent_guid_wie = pconfig.get("dvelop_fields", "wie")
                    parent_guid_vwg = pconfig.get("dvelop_fields", "vwg")
                    if parent_guid_wie is not None and len(parent_guid_wie) > 30:
                        dms.add_upload_property(prop_guid=parent_guid_wie,
                                                pvalue=prop_lookup_item.economic_unit.id_num,
                                                plist=ret_props,
                                                display_name="Wirtschaftseinheiten")
                    if parent_guid_vwg is not None and len(parent_guid_vwg) > 30:
                        dms.add_upload_property(prop_guid=parent_guid_vwg,
                                                pvalue=prop_lookup_item.company_id,
                                                plist=ret_props,
                                                display_name="VWG")
                    prop_value = prop_lookup_item.id_num
                else:
                    # print(f"{prop_value} --> ((NONE))")
                    prop_value = None

        item_guid = item.get("dvelop_guid")
        item_name = item.get("dvelop_name")

        if item_guid is None and item_name is None:
            stored_vals[item_id] = prop_value
        else:
            dms.add_upload_property(item_name, prop_value, item_guid, ret_props)

    return ret_props


def get_mapping_props(profile_prop_path: str) -> dict:
    ret_dict = {}
    current_prop_id = ""
    current_dict = {}
    line_count = 0
    with open(profile_prop_path, 'r', encoding='utf-8') as pr_file:
        lines = pr_file.readlines()
        for line in lines:
            line_count += 1
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            if line.startswith("["):
                if current_prop_id is not None and len(current_prop_id) > 0:
                    ret_dict[current_prop_id] = current_dict
                current_prop_id = re.search(r'\[(.*)\]', line).group(1)
                current_dict = {"prop_id": current_prop_id}
            else:
                str_parts = line.split('=', 1)
                if len(str_parts) != 2:
                    logger.error(f"Illegal param count in get_profile_props. Param {line} line {line_count}")
                    continue  # Wirklich? Oder abbrechen?
                str_key = str_parts[0]
                str_value = str_parts[1]
                current_dict[str_key] = str_value

        if current_prop_id is not None and len(current_prop_id) > 0:
            ret_dict[current_prop_id] = current_dict
    return ret_dict


def get_mappings(mapping_path: str, profile_props: dict) -> dict:
    ret_dict = {}
    current_mapping_id = ""
    current_dict = {"prop": [],
                    "keyword": [],
                    "completion": []}
    with open(mapping_path, 'r', encoding='utf-8') as pr_file:
        lines = pr_file.readlines()
        line_number = 0
        for line in lines:
            line_number += 1
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            if line.startswith("["):
                if len(current_mapping_id) > 0:
                    ret_dict[current_mapping_id] = current_dict
                current_mapping_id = re.search(r'\[(.*)\]', line).group(1)
                current_dict = {"prop": [],
                                "keyword": [],
                                "completion": []}
                continue
            str_parts = line.split('=', 1)
            if len(str_parts) != 2:
                logger.error(f"Illegal param count in get_mappings. Param {line} line {line_number}")
                continue  # Wirklich? Oder abbrechen?

            str_key = str_parts[0]
            str_value = str_parts[1]
            # print(f"{str_key}:{str_value}")

            if str_key.lower() == "prop":
                if str_value.lower() not in profile_props.keys():
                    logger.error(f"get_mappings: Line {line_number} prop {str_value} does not exist.")
                    continue  # Oder gleich abbrechen?
                current_dict["prop"].append(profile_props.get(str_value.lower()))
                continue

            if str_key.lower().startswith("category"):
                current_dict[str_key] = str_value
                continue

            current_dict[str_key].append(str_value)

        if current_mapping_id is not None and len(current_mapping_id) > 0:
            ret_dict[current_mapping_id] = current_dict
    # print(ret_dict)
    return ret_dict


def keywords_in_text(p_text: str, keywordlist: list, all_words: bool = True):
    for keyw in keywordlist:
        keyw = keyw.upper().strip()
        and_parts = keyw.split("|")
        # logger.info(and_parts)
        if all_words:
            if all([x in p_text.upper() for x in and_parts]):
                return True
        else:
            if any([x in p_text.upper() for x in and_parts]):
                return True
    return False


def get_mapping_id(pdf_text: str, mapping_dict: dict):
    for pkey in mapping_dict.keys():
        if keywords_in_text(pdf_text, mapping_dict.get(pkey).get("keyword")):
            return pkey
    return None


def text_without_spaces(pdf_text: str) -> str:
    pdf_text = pdf_text.strip()
    pdf_text = pdf_text.replace(" ", "")
    pdf_text = pdf_text.replace("  ", "")
    return pdf_text


def write_part(temp_path: str, basename: str, pdf_stream, part_number, current_text: str, map_id: str):
    dest_file_path = os.path.join(temp_path, f"{basename}_p{part_number}.pdf")
    with open(dest_file_path, 'wb') as output_file:
        pdf_stream.write(output_file)
        return {
            "file": dest_file_path,
            "text": current_text,
            "map_id": map_id
        }


def split_and_get_text(source_file: str, page_map: dict, temp_path: str, basename: str):
    ret_part_map = {}
    current_doc = PyPDF2.PdfWriter()
    current_text = None
    last_part_num = 0
    page_counter = 0
    with open(source_file, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        for page_num in range(len(pdf_reader.pages)):
            page_counter += 1
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            map_entry = page_map.get(page_counter)
            if map_entry is None:
                continue
            if last_part_num != map_entry.get("part_num"):
                ret_part_map[last_part_num] = write_part(temp_path=temp_path,
                                                         basename=basename,
                                                         pdf_stream=current_doc,
                                                         part_number=last_part_num,
                                                         current_text=current_text,
                                                         map_id=map_entry.get("map_id"))
                current_doc = PyPDF2.PdfWriter()
                current_text = None
            current_doc.add_page(page)
            if current_text is not None:
                current_text = f"{current_text}\n{page_text}"
            else:
                current_text = page_text
            last_part_num = map_entry.get("part_num")
        ret_part_map[last_part_num] = write_part(temp_path=temp_path,
                                                 basename=basename,
                                                 pdf_stream=current_doc,
                                                 part_number=last_part_num,
                                                 current_text=current_text,
                                                 map_id=map_entry.get("map_id"))
        return ret_part_map


def process_pdf_file(input_pdf_file: str, mapping_dict: dict, temp_path: str, ignore_word_list: list,
                     cache: WowiCache, dms: DvelopDmsPy, pconfig: configparser.ConfigParser,
                     mapping_persistence: bool = False, mapping_persistence_sticky: bool = False):
    logger.debug(f"Processing {input_pdf_file}")
    basename = Path(input_pdf_file).stem
    ret_dict = {}
    with (open(input_pdf_file, 'rb') as pdf_file):
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        num_pages = len(pdf_reader.pages)
        logger.debug(f"Number of pages: {num_pages}")

        file_num = 0
        last_cr_id = None
        last_was_complete = True
        page_counter = 0
        page_map = {}
        for page_num in range(num_pages):
            page_counter += 1
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            page_text_no_space = text_without_spaces(page_text)
            if keywords_in_text(page_text_no_space, ignore_word_list, False):
                page_map[page_counter] = None
                logger.warning(f"Page {page_counter} ignored because of blacklist.")
                continue
            if len(page_text.strip()) < 20:
                blank_handling = pconfig.get("general", "blank_page_handling", fallback="add").lower()
                if blank_handling == "add":
                    page_map[page_counter] = {
                        "map_id": last_cr_id,
                        "complete": last_was_complete,
                        "part_num": file_num
                    }
                elif blank_handling == "ignore":
                    page_map[page_counter] = None
                elif blank_handling == "fail":
                    logger.error(f"No text on page {page_counter} of file {input_pdf_file}. Exiting.")
                    return None
                continue

            logger.debug(f"Extracted text from page {page_counter}:\n{page_text}")

            cr_id = get_mapping_id(page_text_no_space, mapping_dict)
            needs_separation = False
            if last_cr_id and cr_id != last_cr_id and not last_was_complete:
                if mapping_persistence_sticky:
                    cr_id = last_cr_id
                else:
                    needs_separation = True
            if cr_id is None and not last_was_complete and mapping_persistence:
                cr_id = last_cr_id

            if cr_id is not None:
                cr_comp = keywords_in_text(page_text_no_space, mapping_dict.get(cr_id).get("completion"))

            if cr_id is None and "fallback" in mapping_dict.keys():
                needs_separation = True
                cr_id = "fallback"
                cr_comp = True

            if cr_id is None:
                logger.error(f"Could not determin mapping for file {input_pdf_file} page {page_counter}")
                logger.error(page_text)
                return None

            if needs_separation and not last_was_complete:
                file_num += 1

            pagemap_entry = {
                "map_id": cr_id,
                "complete": cr_comp,
                "part_num": file_num
            }
            if cr_comp:
                file_num += 1
            last_cr_id = cr_id
            last_was_complete = cr_comp
            page_map[page_counter] = pagemap_entry

    logger.debug(f"page_map:{page_map}")
    file_map = split_and_get_text(source_file=input_pdf_file, page_map=page_map, temp_path=temp_path, basename=basename)
    # logger.debug(f"file_map:{file_map}")
    for entry_id in file_map.keys():
        entry = file_map.get(entry_id)
        map_id = entry.get("map_id")
        dest_props = get_props_from_doc(pdoctext=entry.get("text"),
                                        pprops=mapping_dict.get(map_id).get("prop"),
                                        cache=cache,
                                        pconfig=pconfig,
                                        dms=dms)
        dest_cat_guid = mapping_dict.get(map_id).get("category_id")
        dest_cat_name = mapping_dict.get(map_id).get("category_name")
        ret_dict[entry.get("file")] = {"profile_id": map_id,
                                       "dest_props": dest_props,
                                       "cat_name": dest_cat_name,
                                       "cat_id": dest_cat_guid}

    return ret_dict


def upload_file(upl_file_path: str, dvelop_obj: DvelopDmsPy, dest_cat_name: str, dest_cat_id: str, dest_props: list):
    scats = dvelop_obj.add_category(display_name=dest_cat_name, category_guid=dest_cat_id)
    doc_id = dvelop_obj.archive_file(upl_file_path, scats[0], dest_props)
    return doc_id


def process_profile(profile_filepath: str, dms: DvelopDmsPy, cache: WowiCache):
    config = configparser.ConfigParser(delimiters=('=',))
    config.read(profile_filepath, encoding='utf-8')

    current_dir = os.path.abspath(os.path.dirname(__file__))

    # Sanity checks
    profile_name = os.path.basename(profile_filepath)
    profile_basename = os.path.splitext(profile_name)[0]
    profile_folder = os.path.dirname(profile_filepath).rstrip(os.path.sep)
    profile_maps = os.path.join(profile_folder, f"{profile_basename}.map")
    profile_props = os.path.join(profile_folder, f"{profile_basename}.prop")
    if not config.getboolean("general", "enabled", fallback=True):
        logger.warning(f"Profile {profile_name} is disabled. Skipping.")
        return None
    logger.debug(f"Processing profile {profile_name}")
    if not os.path.exists(profile_maps):
        logger.error(f"File {profile_maps} does not exist")
        return None
    if not os.path.exists(profile_props):
        logger.error(f"File {profile_props} does not exist.")
        return None
    input_path = config.get("general", "input_path")
    backup_path = config.get("general", "backup_path", fallback=None)
    error_path = config.get("general", "error_path", fallback=None)
    if not os.path.exists(input_path):
        logger.error(f"Path {input_path} does not exist.")
        return None
    if backup_path and not os.path.exists(backup_path):
        logger.error(f"Path {backup_path} does not exist.")
        return None
    if error_path and not os.path.exists(error_path):
        logger.error(f"Path {error_path} does not exist.")
        return None

    # Diese Option steuert, ob eine Seite, zu der kein Mapping gefunden werden kann, automatisch zum vorherigen
    # Mapping gezählt werden soll. Das beißt sich ggf. mit einem Fallback-Eintrag
    mapping_persist = config.getboolean("general", "mapping_persistence", fallback=False)
    mapping_persist_sticky = config.getboolean("general", "mapping_persistence_sticky", fallback=False)

    dry_run = config.getboolean("general", "dry_run", fallback=False)
    if dry_run:
        logger.info("Dry run!")

    proplist = get_mapping_props(profile_prop_path=profile_props)
    logger.debug(f"Got {len(proplist)} properties from file.")
    proflist = get_mappings(profile_maps, proplist)
    logger.debug(f"Got {len(proflist)} profiles from file.")

    ignore_keywords_str = config.get("general", "ignore_keywords", fallback=None)
    ignore_keywords = []
    if ignore_keywords_str is not None and len(ignore_keywords_str) > 0:
        ignore_keywords = ignore_keywords_str.split("|")

    logger.debug(f"ignore_keywords: {ignore_keywords}")

    pathlist = Path(input_path).rglob('*.pdf')
    file_counter = 0

    for sfile in pathlist:
        file_counter += 1
        # Split files and math creditors
        logger.info(f"Processing {sfile}")
        splitted_files = process_pdf_file(input_pdf_file=str(sfile),
                                          mapping_dict=proflist,
                                          temp_path=os.path.join(current_dir, "temp"),
                                          ignore_word_list=ignore_keywords,
                                          cache=cache,
                                          dms=dms,
                                          mapping_persistence=mapping_persist,
                                          mapping_persistence_sticky=mapping_persist_sticky,
                                          pconfig=config)

        if splitted_files is None or len(splitted_files) == 0:
            err_file_path = os.path.join(error_path, f"{sfile.name}")
            logger.error(f"Processing of file cancelled. Moving to {err_file_path}")

            if not dry_run:
                shutil.move(sfile, err_file_path)
            continue
        else:
            backup_file_path = os.path.join(backup_path, "ocr", f"{sfile.name}")
            logger.debug(f"Moving splitted ocr file to {backup_file_path}.")
            if not dry_run:
                shutil.move(sfile, backup_file_path)

        # Uploading files to archive
        logger.info(f"Splitted file in {len(splitted_files.keys())} parts. Uploading...")

        for file_part in splitted_files.keys():
            upload_file_settings = splitted_files.get(file_part)
            logger.info(f"Uploading {file_part} ({upload_file_settings['profile_id']})...")
            logger.info(upload_file_settings['dest_props'])
            if dry_run:
                continue
            upl_result = upload_file(upl_file_path=file_part,
                                     dvelop_obj=dms,
                                     dest_cat_name=upload_file_settings['cat_name'],
                                     dest_cat_id=upload_file_settings['cat_id'],
                                     dest_props=upload_file_settings['dest_props'])
            if upl_result is not None:
                logger.info(f"Upload successful (Document id {upl_result}")
                backup_file_path = os.path.join(backup_path, "uploaded", Path(file_part).name)
                shutil.move(file_part, backup_file_path)
            else:
                err_file_path = os.path.join(error_path, f"{file_part}")
                logger.error(f"Upload failed! Moving file to {err_file_path}")
                shutil.move(file_part, err_file_path)
                continue
        logger.debug(f"Processing of file {sfile} finished.")
    cleanup_after = config.getint("general", "delete_backup_after_days", fallback=0)
    cleanup_backup_folder(path=backup_path, after_days=cleanup_after)
