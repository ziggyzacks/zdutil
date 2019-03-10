"""Logging mixin for use around module"""

import logging
import os
import time
from functools import wraps, partial
from logging.config import fileConfig


class LogMixin(object):
    PATH = os.path.abspath(__file__)
    DIR = os.path.dirname(PATH)
    CONF = os.path.join(DIR, 'logging_config.ini')

    fileConfig(CONF)

    logging.getLogger('s3fs').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('chardet').setLevel(logging.WARNING)

    @staticmethod
    def get_logger(name):
        return logging.getLogger(name)

    @property
    def logger(self):
        name = '.'.join([self.__module__, self.__class__.__name__])
        logger = logging.getLogger(name)
        return logger


def timeit(method=None, *, wargs=False):
    """takes method and wraps it in a timer"""
    if method is None:
        return partial(timeit, wargs=wargs)

    log = LogMixin()

    @wraps(method)
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        log.logger.info(f'{method.__qualname__} {args if wargs else ""} took {round(te - ts, 3)}s seconds')
        return result

    return timed
