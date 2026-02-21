"""Basic logger setup for ingestion."""

import logging

LOGGER_NAME = "ingestion"


def get_logger(name: str = LOGGER_NAME, level: str = "INFO") -> logging.Logger:
    """Return a configured logger instance without duplicating handlers."""
    logger = logging.getLogger(name)
    if not getattr(logger, "_ingestion_configured", False):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
        setattr(logger, "_ingestion_configured", True)

    logger.setLevel(level.upper())
    return logger
