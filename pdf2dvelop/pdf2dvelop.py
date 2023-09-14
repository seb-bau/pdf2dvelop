import graypy
import os
import sys
import logging
import PyPDF2
import re
import shutil
from dotenv import dotenv_values
from datetime import datetime
from pathlib import Path
from wowipy.wowipy import WowiPy, BuildingLand
from dvelopdmspy.dvelopdmspy import DvelopDmsPy

# Weitermachen: Profile um eine Einstellung zur Ziel-Kategorie erweitern, damit mehrere Prozesse mit einer Anwendungs-
# instanz auskommen können.


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def remove_leading_zeroes(input_str: str):
    output_str = re.sub(r'(\D)0*(\d+)', r'\1\2', input_str)
    if output_str is None:
        return input_str

    return output_str


def address_to_building(paddr: str, wowi: WowiPy):
    found_building = wowi.search_building(paddr, 899, 1)
    if found_building is not None and len(found_building) > 0:
        return found_building[0]
    else:
        return None


def get_props_from_doc(pdoctext: str, pprops: list, wowi: WowiPy, psettings: dict):
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
            prop_value = re.search(item.get("regex"), pdoctext)
            regex_group = int(item.get("regex_group"))
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
            item_raw_dvelop = item.get("dvelop_raw_guid")
            if item_raw_dvelop is not None and len(item_raw_dvelop) > 30:
                dms.add_upload_property("", prop_value, item_raw_dvelop, ret_props)
            if item_lookup.lower() == "building_address":
                prop_value = remove_leading_zeroes(prop_value)
                prop_value = prop_value.replace("STRABE", "STRAßE")
                prop_lookup_item: BuildingLand
                prop_lookup_item = address_to_building(prop_value, wowi)
                if prop_lookup_item is not None:
                    # print(f"{prop_value} --> {prop_lookup_item.id_num}")

                    parent_guid_wie = psettings.get("dvelop_guid_wie")
                    parent_guid_vwg = psettings.get("dvelop_guid_vwg")
                    if parent_guid_wie is not None and len(parent_guid_wie) > 30:
                        dms.add_upload_property(prop_guid=parent_guid_wie,
                                                pvalue=prop_lookup_item.economic_unit.id_num,
                                                plist=ret_props,
                                                display_name="Wirtschaftseinheiten")
                    if parent_guid_vwg is not None and len(parent_guid_vwg) > 30:
                        dms.add_upload_property(prop_guid=parent_guid_vwg,
                                                pvalue=prop_lookup_item.company_code.code,
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


def get_profile_props(profile_prop_path: str) -> dict:
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


def get_profiles(profile_path: str, profile_props: dict) -> dict:
    ret_dict = {}
    current_profile_id = ""
    current_dict = {"prop": [],
                    "keyword": [],
                    "completion": []}
    with open(profile_path, 'r', encoding='utf-8') as pr_file:
        lines = pr_file.readlines()
        line_number = 0
        for line in lines:
            line_number += 1
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            if line.startswith("["):
                if len(current_profile_id) > 0:
                    ret_dict[current_profile_id] = current_dict
                current_profile_id = re.search(r'\[(.*)\]', line).group(1)
                current_dict = {"prop": [],
                                "keyword": [],
                                "completion": []}
                continue
            str_parts = line.split('=', 1)
            if len(str_parts) != 2:
                logger.error(f"Illegal param count in get_profiles. Param {line} line {line_number}")
                continue  # Wirklich? Oder abbrechen?

            str_key = str_parts[0]
            str_value = str_parts[1]
            # print(f"{str_key}:{str_value}")

            if str_key.lower() == "prop":
                if str_value.lower() not in profile_props.keys():
                    logger.error(f"get_profiles: Line {line_number} prop {str_value} does not exist.")
                    continue  # Oder gleich abbrechen?
                current_dict["prop"].append(profile_props.get(str_value.lower()))
                continue

            if str_key.lower().startswith("category"):
                current_dict[str_key] = str_value
                continue

            current_dict[str_key].append(str_value)

        if current_profile_id is not None and len(current_profile_id) > 0:
            ret_dict[current_profile_id] = current_dict
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


def get_profile_id(pdf_text: str, profile_dict: dict):
    for pkey in profile_dict.keys():
        if keywords_in_text(pdf_text, profile_dict.get(pkey).get("keyword")):
            return pkey
    return None


def get_profile_id_and_completion(pdf_text: str, profile_dict: dict, old_profile: str = None,
                                  profile_persistence: bool = False):
    pdf_text = text_without_spaces(pdf_text)
    ret_prof_comp = False
    ret_prof_id = get_profile_id(pdf_text, profile_dict)
    # If profile_persistence is active, assume that the current page belongs to the previous profile, even
    # if the keywords are not present on this page.
    if ret_prof_id is None and profile_persistence:
        ret_prof_id = old_profile
    if old_profile is not None and profile_persistence:
        ret_prof_id = old_profile
    if ret_prof_id is not None:
        ret_prof_comp = keywords_in_text(pdf_text, profile_dict.get(ret_prof_id).get("completion"))
    return ret_prof_id, ret_prof_comp


def text_without_spaces(pdf_text: str) -> str:
    pdf_text = pdf_text.strip()
    pdf_text = pdf_text.replace(" ", "")
    pdf_text = pdf_text.replace("  ", "")
    return pdf_text


def process_pdf_file(input_pdf_file: str, profile_dict: dict, temp_path: str, ignore_word_list: list,
                     wowi: WowiPy, profile_persistence: bool = False):
    logger.debug(f"Processing {input_pdf_file}")
    basename = Path(input_pdf_file).stem
    ret_dict = {}
    pdf_reader = PyPDF2.PdfReader(input_pdf_file)
    num_pages = len(pdf_reader.pages)
    logger.debug(f"Number of pages: {num_pages}")

    file_num = 0
    current_doc = None
    current_doc_text = ""
    current_page_count = 0
    current_cr_id = None
    for page_num in range(num_pages):
        page = pdf_reader.pages[page_num]
        page_text = page.extract_text()
        if keywords_in_text(text_without_spaces(page_text), ignore_word_list, False):
            logger.warning(f"Page {page_num + 1} ignored because of blacklist.")
            continue
        if len(page_text.strip()) < 20:
            # logger.warning(f"No text on page {page_num + 1} of file {input_pdf_file}")
            continue

        logger.debug(f"Extracted text from page {page_num + 1}:\n{page_text}")
        cr_id, cr_comp = get_profile_id_and_completion(page_text, profile_dict, current_cr_id, profile_persistence)
        current_cr_id = cr_id
        logger.debug(f"Profile: {cr_id}. Doc completed: {cr_comp}")

        if cr_id is None:
            logger.error(f"Could not determin profile for file {input_pdf_file} page {page_num + 1}")
            logger.error(page_text)
            return None

        if current_doc is None:
            logger.debug("current_doc is None, creating new.")
            current_doc = PyPDF2.PdfWriter()
            current_doc_text = ""
            current_page_count = 0

        logger.debug("Adding page to document.")
        current_doc.add_page(page)
        current_page_count += 1
        current_doc_text += page_text

        if cr_comp:
            dest_props = get_props_from_doc(current_doc_text, profile_dict.get(cr_id).get("prop"), wowi, settings)
            dest_cat_guid = profile_dict.get(cr_id).get("category_id")
            dest_cat_name = profile_dict.get(cr_id).get("category_name")
            logger.debug(f"Page {page_num} dest_props: {dest_props}")
            logger.debug("End of doc, closing.")
            file_num += 1
            dest_file_path = os.path.join(temp_path, f"{basename}_p{file_num}.pdf")
            if current_page_count > 3:
                print(f"{input_pdf_file} page {page_num} pagecount {current_page_count}")
            with open(dest_file_path, 'wb') as output_file:
                current_doc.write(output_file)
                ret_dict[dest_file_path] = {"profile_id": cr_id,
                                            "dest_props": dest_props,
                                            "cat_name": dest_cat_name,
                                            "cat_id": dest_cat_guid}
            current_doc = None
            current_page_count = 0
            current_cr_id = None
    return ret_dict


def upload_file(upl_file_path: str, dvelop_obj: DvelopDmsPy, dest_cat_name: str, dest_cat_id: str, dest_props: list):
    scats = dvelop_obj.add_category(display_name=dest_cat_name, category_guid=dest_cat_id)
    doc_id = dvelop_obj.archive_file(upl_file_path, scats[0], dest_props)
    return doc_id


current_dir = os.path.abspath(os.path.dirname(__file__))
settings = dotenv_values(os.path.join(current_dir, ".env"))

log_method = settings.get("log_method", "file").lower()
log_level = settings.get("log_level", "info").lower()
graylog_host = settings.get("graylog_host")
graylog_port = int(settings.get("graylog_port", 12201))
source_dir = settings.get("source_dir", os.path.join(current_dir, "ocr"))
backup_path = settings.get("backup_path", os.path.join(current_dir, "backup"))

dvelop_host = settings.get("dvelop_host")
dvelop_key = settings.get("dvelop_key")
dvelop_creditor_prop_name = settings.get("dvelop_creditor_prop_name")

profiles_persist_raw = settings.get("profile_persistence")
if profiles_persist_raw is not None and profiles_persist_raw.lower() == "true":
    profiles_persist = True
else:
    profiles_persist = False

# Logging
logger = logging.getLogger(__name__)
log_levels = {'debug': 10, 'info': 20, 'warning': 30, 'error': 40, 'critical': 50}
logger.setLevel(log_levels.get(log_level, 20))

# Catch unhandled exceptions
sys.excepthook = handle_unhandled_exception

# Dry Run
dry_run = False
if dry_run:
    logger.info("Dry run!")

if log_method == "file":
    log_file_name = f"pdf2dvelop_{datetime.now().strftime('%Y_%m_%d')}.log"
    log_path = os.path.join(current_dir, "log", log_file_name)
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        filename=log_path,
                        filemode='a')
elif log_method == "graylog":
    handler = graypy.GELFUDPHandler(graylog_host, graylog_port)
    logger.addHandler(handler)

# Catch unhandled exceptions
sys.excepthook = handle_unhandled_exception

logger.info(f"pdf2dvelop started. Reading files in {source_dir}")

prof_file = os.path.join(current_dir, ".profiles")
prof_prop_file = os.path.join(current_dir, ".props")
error_dir = os.path.join(current_dir, "errors")
proplist = get_profile_props(prof_prop_file)
# print(proplist)
logger.debug(f"Got {len(proplist)} properties from file.")
proflist = get_profiles(prof_file, proplist)
logger.debug(f"Got {len(proflist)} profiles from file.")

ignore_keywords_str = settings.get("ignore_keywords")
ignore_keywords = []
if ignore_keywords_str is not None and len(ignore_keywords_str) > 0:
    ignore_keywords = ignore_keywords_str.split("|")

# print(ignore_keywords)

pathlist = Path(source_dir).rglob('*.pdf')
file_counter = 0
# Connect to d.velop
dms = DvelopDmsPy(hostname=dvelop_host, api_key=dvelop_key)

# Connect to OPENWOWI
wowi_host = settings.get("wowi_host")
wowi_user = settings.get("wowi_user")
wowi_pass = settings.get("wowi_pass")
wowi_key = settings.get("wowi_key")
wowi_cache_buildings = settings.get("wowi_cache_buildings")
openwowi = WowiPy(wowi_host, wowi_user, wowi_pass, wowi_key)
openwowi.cache_from_disk(openwowi.CACHE_BUILDING_LANDS, wowi_cache_buildings)

for sfile in pathlist:
    file_counter += 1
    # Split files and math creditors
    logger.info(f"Processing {sfile}")
    splitted_files = process_pdf_file(str(sfile), proflist, os.path.join(current_dir, "temp"), ignore_keywords,
                                      openwowi, profiles_persist)

    if splitted_files is None or len(splitted_files) == 0:
        err_file_path = os.path.join(error_dir, f"{sfile.name}")
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
            err_file_path = os.path.join(error_dir, f"{file_part}")
            logger.error(f"Upload failed! Moving file to {err_file_path}")
            shutil.move(file_part, err_file_path)
            continue
    logger.debug(f"Processing of file {sfile} finished.")
