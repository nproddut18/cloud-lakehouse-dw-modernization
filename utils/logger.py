"""
Centralized structured logger using loguru.
Import get_logger() in any module instead of using the stdlib logging directly.
"""

import sys
from loguru import logger as _loguru_logger

_CONFIGURED = False


def _configure() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    _loguru_logger.remove()  # Remove default handler

    # Console — human-readable
    _loguru_logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> — "
               "<level>{message}</level>",
        level="INFO",
        colorize=True,
    )

    # File — JSON structured for CloudWatch / log aggregators
    _loguru_logger.add(
        "logs/pipeline_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DDTHH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message}",
        level="DEBUG",
        rotation="00:00",   # new file each day
        retention="14 days",
        compression="gz",
    )

    _CONFIGURED = True


def get_logger(name: str):
    """Return a loguru logger bound to the calling module name."""
    _configure()
    return _loguru_logger.bind(name=name)
