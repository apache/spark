import inspect
import logging
import os
import pathlib
from contextlib import contextmanager
from datetime import timedelta
from timeit import default_timer
from typing import IO


def isFunction(obj) -> "bool":
    return (
        inspect.isfunction(obj)
        or inspect.ismethod(obj)
        or inspect.iscoroutinefunction(obj)
        or inspect.isasyncgenfunction(obj)
        or inspect.isgeneratorfunction(obj)
    )


def getModuleName(obj) -> "str | None":
    module = inspect.getmodule(obj)
    if module:
        return module.__name__
    else:
        return getattr(obj, "__module__", None)


def getObjectId(obj) -> "str":
    if inspect.ismodule(obj):
        return obj.__name__

    moduleName = getModuleName(obj)
    qualname = getattr(obj, "__qualname__", None)
    if qualname is None:
        qualname = getattr(obj, "__name__", None)

    if inspect.isclass(obj):
        if qualname is None:
            qualname = f"<class ({type(obj)})>"
    elif isFunction(obj):
        if qualname is None:
            qualname = f"<function ({type(obj)})>"
    else:
        if qualname is None:
            qualname = f"<instance ({type(obj)})>"

    if moduleName:
        return f"{moduleName}.{qualname}"
    else:
        return qualname


class TeeFile(object):
    """Combine multiple file-like objects into one for multi-writing."""

    def __init__(self, *files: "IO[str]"):
        self.files = files

    def write(self, txt):
        for fp in self.files:
            fp.write(txt)


def ensureDirectory(path: "pathlib.Path") -> None:
    """Ensure that the directory exists."""

    path = path.absolute()
    if path.exists() and path.is_dir():
        return

    os.makedirs(path, exist_ok=True)


def ensureFile(path: "pathlib.Path", content: "str | None" = None) -> None:
    """Ensure that the file exists and has the given content."""

    path = path.absolute()
    if path.exists() and path.is_file():
        if content is not None:
            path.write_text(content)
        return

    ensureDirectory(path.parent)

    if content is None:
        content = ""

    path.write_text(content)


@contextmanager
def elapsedTimer():
    """Provide a context with a timer."""

    start = default_timer()

    def elapser():
        return timedelta(seconds=default_timer() - start)

    try:
        yield lambda: elapser()
    finally:
        end = default_timer()

        def elapser():
            return timedelta(seconds=end - start)


@contextmanager
def logWithStream(
    logger: "logging.Logger", stream: "IO", level: "int" = logging.NOTSET
):
    """Provide a context with the logger writing to a file."""
    from . import LOGGING_DATEFMT, LOGGING_FORMAT

    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT))
    logger.addHandler(handler)

    try:
        yield logger
    finally:
        logger.removeHandler(handler)


@contextmanager
def logWithFile(
    logger: "logging.Logger",
    path: "pathlib.Path | None" = None,
    level: "int" = logging.NOTSET,
):
    """Provide a context with the logger writing to a file."""

    if path is None:
        yield logger
    else:
        with path.open("w") as fp:
            with logWithStream(logger, fp, level) as logger:
                yield logger
