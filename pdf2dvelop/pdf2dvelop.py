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
from dvelopdmspy.dvelopdmspy import DvelopDmsPy


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def pdf2txtpages(pdf_file_path: str):
    ret_txt = []
    try:
        with open(pdf_file_path, "rb") as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            num_pages = len(pdf_reader.pages)

            for page_num in range(num_pages):
                page = pdf_reader.pages[page_num]
                page_text = page.extract_text()
                ret_txt.append(page_text)
    except PyPDF2.errors.PyPdfError as e:
        logger.error(f"PyPdf exception: {e.args}")
    return ret_txt, pdf_reader


def get_profiles(profile_path: str) -> dict:
    ret_dict = {}
    current_creditor_idnum = ""
    current_dict = {}
    with open(profile_path, 'r', encoding='utf-8') as pr_file:
        lines = pr_file.readlines()
        for line in lines:
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            if line.startswith("["):
                if current_creditor_idnum is not None and len(current_creditor_idnum) > 0:
                    ret_dict[current_creditor_idnum] = current_dict
                current_creditor_idnum = re.search(r'\[(.*)\]', line).group(1)
                current_dict = {}
            elif 'keywords' in line.lower():
                str_keys = line.split('=')[1]
                current_dict["keywords"] = str_keys.split('|')
            elif line.lower().startswith('completion'):
                str_comp = line.split('=')[1]
                current_dict["completion"] = str_comp.split('|')
        if current_creditor_idnum is not None and len(current_creditor_idnum) > 0:
            ret_dict[current_creditor_idnum] = current_dict
    return ret_dict


def get_cred_idnum(pdf_text: str, cred_prof_dict: dict):
    pdf_text = pdf_text.strip()
    pdf_text = pdf_text.replace(" ", "")
    pdf_text = pdf_text.replace("  ", "")
    ret_cred_id = None
    ret_cred_comp = False

    for pkey in cred_prof_dict.keys():
        if ret_cred_id is not None:
            break
        for keyw in cred_prof_dict.get(pkey).get("keywords"):
            keyw = keyw.upper().strip()
            if keyw in pdf_text.upper():
                ret_cred_id = pkey
                break

    if ret_cred_id is not None:
        for keyw in cred_prof_dict.get(ret_cred_id).get("completion"):
            keyw = keyw.upper().strip()
            if keyw in pdf_text.upper():
                ret_cred_comp = True
                break

    return ret_cred_id, ret_cred_comp


def process_pdf_file(input_pdf_file: str, profile_dict: dict, temp_path: str):
    logger.debug(f"Processing {input_pdf_file}")
    basename = Path(input_pdf_file).stem
    ret_dict = {}
    with open(input_pdf_file, "rb") as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        num_pages = len(pdf_reader.pages)
        logger.debug(f"Number of pages: {num_pages}")
        file_num = 0
        current_doc = None
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            if len(page_text.strip()) == 0:
                logger.warning(f"No text on page {page_num + 1} of file {input_pdf_file}")
                continue

            logger.debug(f"Extracted text from page {page_num + 1}:\n{page_text}")
            cr_id, cr_comp = get_cred_idnum(page_text, profile_dict)
            logger.debug(f"Creditor: {cr_id}. Doc completed: {cr_comp}")
            if cr_id is None:
                logger.error(f"Could not determin creditor for file {input_pdf_file} page {page_num + 1}")
                logger.error(page_text)
                return None

            if current_doc is None:
                logger.debug("current_doc is None, creating new.")
                current_doc = PyPDF2.PdfWriter()

            logger.debug("Adding page to document.")
            current_doc.add_page(page)

            if cr_comp:
                logger.debug("End of doc, closing.")
                file_num += 1
                dest_file_path = os.path.join(temp_path, f"{basename}_p{file_num}.pdf")
                with open(dest_file_path, 'wb') as output_file:
                    current_doc.write(output_file)
                    ret_dict[dest_file_path] = cr_id
                current_doc = None
    return ret_dict


def upload_file(upl_file_path: str, upl_creditor: str, dvelop_obj: DvelopDmsPy, dest_cat: str, dest_prop: str):
    scats = dvelop_obj.add_category(dest_cat)
    sprops = dvelop_obj.add_upload_property(dest_prop, upl_creditor)
    doc_id = dvelop_obj.archive_file(upl_file_path, scats[0], sprops)
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
dvelop_cat_name = settings.get("dvelop_cat_name")
dvelop_creditor_prop_name = settings.get("dvelop_creditor_prop_name")

# Logging
logger = logging.getLogger(__name__)
log_levels = {'debug': 10, 'info': 20, 'warning': 30, 'error': 40, 'critical': 50}
logger.setLevel(log_levels.get(log_level, 20))

# Catch unhandled exceptions
sys.excepthook = handle_unhandled_exception

if log_method == "file":
    log_file_name = f"sftptransfer_{datetime.now().strftime('%Y_%m_%d')}.log"
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
error_dir = os.path.join(current_dir, "errors")
credprofs = get_profiles(prof_file)

pathlist = Path(source_dir).rglob('*.pdf')
file_counter = 0
# Connect to d.velop
dms = DvelopDmsPy(hostname=dvelop_host, api_key=dvelop_key)

for sfile in pathlist:
    file_counter += 1
    # Split files and math creditors
    logger.info(f"Processing {sfile}")
    splitted_files = process_pdf_file(str(sfile), credprofs, os.path.join(current_dir, "temp"))

    if splitted_files is None or len(splitted_files) == 0:
        err_file_path = os.path.join(error_dir, f"{sfile.name}")
        logger.error(f"Processing of file cancelled. Moving to {err_file_path}")
        shutil.move(sfile, err_file_path)
        continue
    else:
        backup_file_path = os.path.join(backup_path, "ocr", f"{sfile.name}")
        logger.debug(f"Moving splitted ocr file to {backup_file_path}.")
        shutil.move(sfile, backup_file_path)

    # Uploading files to archive
    logger.info(f"Splitted file in {len(splitted_files.keys())} parts. Uploading...")

    for file_part in splitted_files.keys():
        upload_file_creditor = splitted_files.get(file_part)
        logger.debug(f"Uploading {file_part} ({upload_file_creditor})...")
        upl_result = upload_file(upl_file_path=file_part,
                                 upl_creditor=upload_file_creditor,
                                 dvelop_obj=dms,
                                 dest_cat=dvelop_cat_name,
                                 dest_prop=dvelop_creditor_prop_name)
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
