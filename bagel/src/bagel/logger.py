import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BagelLogger:

    logger = logger

    def __init__(self):
        BagelLogger._add_stream_handler()

    def info(self, msg: any):
        self.logger.info(msg)

    def error(self, msg: any):
        self.logger.error(msg)

    def warning(self, msg: any):
        self.logger.warning(msg)

    def debug(self, msg: any):
        self.logger.debug(msg)

    @staticmethod
    def new_log_file(log_name) -> None:
        if len(logger.handlers) == 2:
            handler = logger.handlers[1]
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
            else:
                logger.removeHandler(logger.handlers[0])

        fh_formatter = '{"timestamp":"%(asctime)s", "level_name":"%(levelname)s", "function_name":"%(funcName)s", "line_number":"%(lineno)d", "message":"%(message)s"}'

        log_dir = os.path.dirname(log_name)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        fh = logging.FileHandler(log_name)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(fmt=fh_formatter))

        logger.addHandler(fh)

    @staticmethod
    def _add_stream_handler() -> None:
        ch_formatter = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter(fmt=ch_formatter))

        logger.addHandler(ch)
