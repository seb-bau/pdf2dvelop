import logging
import graypy
import os
from datetime import datetime


class AppNameFilter(logging.Filter):
    def __init__(self, app_name):
        # In an actual use case would dynamically get this
        # (e.g. from memcache)
        super().__init__()
        self.app_name = app_name

    def filter(self, record):
        record.app_name = self.app_name
        return True


def setup_custom_logger(name, log_method: str, log_level: str, graylog_host: str = None, graylog_port: int = None,
                        graylog_app_name: str = None):
    logger = logging.getLogger(name)
    log_levels = {'debug': 10, 'info': 20, 'warning': 30, 'error': 40, 'critical': 50}
    logger.setLevel(log_levels.get(log_level, 20))

    if log_method == "file":
        log_file_name = f"log_{datetime.now().strftime('%Y_%m_%d')}.log"
        log_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "logs", log_file_name)

        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            filename=log_path,
                            filemode='a')

    elif log_method == "graylog":
        graylog_host = graylog_host
        graylog_port = graylog_port
        handler = graypy.GELFUDPHandler(graylog_host, graylog_port)
        logger.addHandler(handler)
        logger.addFilter(AppNameFilter(app_name=graylog_app_name))

    return logger
