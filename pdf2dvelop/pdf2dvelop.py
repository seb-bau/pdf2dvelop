import graypy
import os
import sys
import logging
import PyPDF2
from dotenv import dotenv_values
from datetime import datetime


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def pdf2txtpages(pdf_file_path: str) -> list[str]:
    ret_txt = []
    with open(pdf_file_path, "rb") as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        num_pages = len(pdf_reader.pages)
        print("HHfbdr")
        print(num_pages)

        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text(0)
            print(page)
            print(page_text)
            ret_txt.append(page_text)

        return ret_txt


current_dir = os.path.abspath(os.path.dirname(__file__))
settings = dotenv_values(os.path.join(current_dir, ".env"))

log_method = settings.get("log_method", "file").lower()
log_level = settings.get("log_level", "info").lower()
graylog_host = settings.get("graylog_host")
graylog_port = int(settings.get("graylog_port", 12201))
source_dir = settings.get("source_dir", os.path.join(current_dir, "input"))
do_backup_int = int(settings.get("do_backup", 1))
backup_path = settings.get("backup_path", os.path.join(current_dir, "backup"))
if do_backup_int == 0:
    do_backup = False
else:
    do_backup = True

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

logger.info(f"pdf2dvelop started.")

# Log message if backup is disabled
if not do_backup:
    logger.info("Note: Backups are disabled.")

pdf_path = "U:\\sw_lieferscheine\\test-ls.pdf"
pagtxt = pdf2txtpages(pdf_path)
print(pagtxt[0])
