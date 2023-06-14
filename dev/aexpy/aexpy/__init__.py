from datetime import datetime
import logging
import os
import pathlib

__version__ = "0.2.0"


LOGGING_FORMAT = "%(levelname)s %(asctime)s %(name)s [%(pathname)s:%(lineno)d:%(funcName)s]\n%(message)s\n"
LOGGING_DATEFMT = "%Y-%m-%d,%H:%M:%S"


def initializeLogging(level: int = logging.WARNING) -> None:
    root = logging.getLogger()
    root.setLevel(logging.NOTSET)
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT))
    root.addHandler(handler)


def getAppDirectory() -> pathlib.Path:
    return pathlib.Path(__file__).parent.resolve()


def getWorkingDirectory() -> pathlib.Path:
    return pathlib.Path(os.getcwd()).resolve()


def getCommitId() -> str:
    return os.getenv("GIT_COMMIT", "unknown")


def getBuildDate() -> datetime:
    try:
        return datetime.fromisoformat(os.getenv("BUILD_DATE", "unknown"))
    except:
        return datetime.now()
