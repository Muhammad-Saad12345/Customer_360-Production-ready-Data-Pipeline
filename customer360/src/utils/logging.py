from __future__ import annotations

import logging
import sys
from typing import Optional


def setup_logging(name: str = "customer360", level: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    fmt = (
        "%(asctime)s %(levelname)s %(name)s run_id=%(run_id)s source=%(source)s object=%(object)s %(message)s"
    )
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level or "INFO")

    # Add contextual defaults to LoggerAdapter-compatible dict
    logger = logging.LoggerAdapter(logger, {"run_id": "-", "source": "-", "object": "-"})
    return logger
