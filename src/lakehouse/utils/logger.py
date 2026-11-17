"""Logging setup shared by every module.

Using one logger factory keeps the format identical across the pipeline, which
makes logs easy to search when a run fails.
"""

from __future__ import annotations

import logging
import sys

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a logger that writes to stdout.

    Safe to call many times with the same name: the handler is attached once.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        logger.addHandler(handler)
        # Do not pass records to the root logger, or each line prints twice.
        logger.propagate = False

    return logger
