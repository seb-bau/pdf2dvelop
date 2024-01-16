import os
import sys
import configparser
import log
from processing import process_profile
from dvelopdmspy.dvelopdmspy import DvelopDmsPy
from wowicache.models import WowiCache


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def clear_temp_files(temp_folder: str):
    try:
        tempfiles = os.listdir(temp_folder)
        for tempfile in tempfiles:
            if tempfile.endswith(".pdf"):
                os.remove(os.path.join(temp_folder, tempfile))
    except (OSError, IOError) as e:
        logger.error(f"Error while clearing temp folder: {str(e)}")


sys.excepthook = handle_unhandled_exception
current_dir = os.path.abspath(os.path.dirname(__file__))
config = configparser.ConfigParser(delimiters=('=',))
config.read(os.path.join(current_dir, "config.ini"), encoding='utf-8')

logger = log.setup_custom_logger('root', config.get('Logging', 'method', fallback='file'),
                                 config.get('Logging', 'level', fallback='info'),
                                 graylog_host=config.get('Logging', 'graylog_host', fallback=None),
                                 graylog_port=config.getint('Logging', 'graylog_port', fallback=0),
                                 graylog_app_name=config.get('Logging', 'graylog_app_name', fallback=None))

if config.has_section("general") and config.has_option("general", "profile_path"):
    profile_path = config.get("general", "profile_path")
else:
    profile_path = os.path.join(current_dir, "profiles")

if not os.path.exists(profile_path):
    logger.error(f"Profile path {profile_path} does not exist")
    exit()

profile_files = os.listdir(profile_path)
file_count = sum(1 for file_name in profile_files if file_name.endswith("ini"))

if file_count == 0:
    logger.error(f"There are no profile files in {profile_path}. Exiting.")
    exit()

dms = DvelopDmsPy(hostname=config.get("dvelop", "host"),
                  api_key=config.get("dvelop", "key"),
                  repository=config.get("dvelop", "repository", fallback=None))

cache = WowiCache(config.get("openwowi", "cache_connection"))

for file_name in profile_files:
    profile_filepath = os.path.join(profile_path, file_name)
    if not file_name.lower().endswith("ini"):
        continue
    if not os.path.isfile(profile_filepath):
        logger.warning(f"Skipping {profile_filepath} as this is a folder.")
        continue

    process_profile(profile_filepath=profile_filepath,
                    dms=dms,
                    cache=cache)
if config.getboolean("general", "remove_temp_files", fallback=True):
    clear_temp_files(temp_folder=os.path.join(current_dir, "temp"))
logger.info("pdf2dvelop finished")
