#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from contextlib import contextmanager
import inspect
import io
import logging
import os
import sys
import time
from typing import BinaryIO, Callable, Generator, Iterable, Iterator, Optional, TextIO, Union
from types import FrameType, TracebackType

from pyspark.logger.logger import JSONFormatter


class DelegatingTextIOWrapper(TextIO):
    """A TextIO that delegates all operations to another TextIO object."""

    def __init__(self, delegate: TextIO):
        self._delegate = delegate

    # Required TextIO properties
    @property
    def encoding(self) -> str:
        return self._delegate.encoding

    @property
    def errors(self) -> Optional[str]:
        return self._delegate.errors

    @property
    def newlines(self) -> Optional[Union[str, tuple[str, ...]]]:
        return self._delegate.newlines

    @property
    def buffer(self) -> BinaryIO:
        return self._delegate.buffer

    @property
    def mode(self) -> str:
        return self._delegate.mode

    @property
    def name(self) -> str:
        return self._delegate.name

    @property
    def line_buffering(self) -> int:
        return self._delegate.line_buffering

    @property
    def closed(self) -> bool:
        return self._delegate.closed

    # Iterator protocol
    def __iter__(self) -> Iterator[str]:
        return iter(self._delegate)

    def __next__(self) -> str:
        return next(self._delegate)

    # Context manager protocol
    def __enter__(self) -> TextIO:
        return self._delegate.__enter__()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return self._delegate.__exit__(exc_type, exc_val, exc_tb)

    # Core I/O methods
    def write(self, s: str) -> int:
        return self._delegate.write(s)

    def writelines(self, lines: Iterable[str]) -> None:
        return self._delegate.writelines(lines)

    def read(self, size: int = -1) -> str:
        return self._delegate.read(size)

    def readline(self, size: int = -1) -> str:
        return self._delegate.readline(size)

    def readlines(self, hint: int = -1) -> list[str]:
        return self._delegate.readlines(hint)

    # Stream control methods
    def close(self) -> None:
        return self._delegate.close()

    def flush(self) -> None:
        return self._delegate.flush()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._delegate.seek(offset, whence)

    def tell(self) -> int:
        return self._delegate.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        return self._delegate.truncate(size)

    # Stream capability methods
    def fileno(self) -> int:
        return self._delegate.fileno()

    def isatty(self) -> bool:
        return self._delegate.isatty()

    def readable(self) -> bool:
        return self._delegate.readable()

    def seekable(self) -> bool:
        return self._delegate.seekable()

    def writable(self) -> bool:
        return self._delegate.writable()


class JSONFormatterWithMarker(JSONFormatter):
    default_microsec_format = "%s.%06d"

    def __init__(self, marker: str, worker_id: str, context_provider: Callable[[], dict[str, str]]):
        super().__init__(ensure_ascii=True)
        self._marker = marker
        self._worker_id = worker_id
        self._context_provider = context_provider

    def format(self, record: logging.LogRecord) -> str:
        context = self._context_provider()
        if context:
            context.update(record.__dict__.get("context", {}))
            record.__dict__["context"] = context
        return f"{self._marker}:{self._worker_id}:{super().format(record)}"

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            s = time.strftime(self.default_time_format, ct)
            if self.default_microsec_format:
                s = self.default_microsec_format % (
                    s,
                    int((record.created - int(record.created)) * 1000000),
                )
            elif self.default_msec_format:
                s = self.default_msec_format % (s, record.msecs)
            s = f"{s}{time.strftime('%z', ct)}"
        return s


class JsonOutput(DelegatingTextIOWrapper):
    def __init__(
        self,
        delegate: TextIO,
        json_out: TextIO,
        logger_name: str,
        log_level: int,
        marker: str,
        worker_id: str,
        context_provider: Callable[[], dict[str, str]],
    ):
        super().__init__(delegate)
        self._json_out = json_out
        self._logger_name = logger_name
        self._log_level = log_level
        self._formatter = JSONFormatterWithMarker(marker, worker_id, context_provider)

    def write(self, s: str) -> int:
        if s.strip():
            log_record = logging.LogRecord(
                name=self._logger_name,
                level=self._log_level,
                pathname=None,  # type: ignore[arg-type]
                lineno=None,  # type: ignore[arg-type]
                msg=s.strip(),
                args=None,
                exc_info=None,
                func=None,
                sinfo=None,
            )
            self._json_out.write(f"{self._formatter.format(log_record)}\n")
            self._json_out.flush()
        return self._delegate.write(s)

    def writelines(self, lines: Iterable[str]) -> None:
        # Process each line through our JSON logging logic
        for line in lines:
            self.write(line)

    def close(self) -> None:
        pass


def context_provider() -> dict[str, str]:
    """
    Provides context information for logging, including caller function name.
    Finds the function name from the bottom of the stack, ignoring Python builtin
    libraries and PySpark modules. Test packages are included.

    Returns:
        dict[str, str]: A dictionary containing context information including:
            - func_name: Name of the function that initiated the logging
            - class_name: Name of the class that initiated the logging if available
    """

    def is_pyspark_module(module_name: str) -> bool:
        return module_name.startswith("pyspark.") and ".tests." not in module_name

    bottom: Optional[FrameType] = None

    # Get caller function information using inspect
    try:
        frame = inspect.currentframe()
        is_in_pyspark_module = False

        if frame:
            while frame.f_back:
                f_back = frame.f_back
                module_name = f_back.f_globals.get("__name__", "")

                if is_pyspark_module(module_name):
                    if not is_in_pyspark_module:
                        bottom = frame
                        is_in_pyspark_module = True
                else:
                    is_in_pyspark_module = False

                frame = f_back
    except Exception:
        # If anything goes wrong with introspection, don't fail the logging
        # Just continue without caller information
        pass

    context = {}
    if bottom:
        context["func_name"] = bottom.f_code.co_name
        if "self" in bottom.f_locals:
            context["class_name"] = bottom.f_locals["self"].__class__.__name__
        elif "cls" in bottom.f_locals:
            context["class_name"] = bottom.f_locals["cls"].__name__
    return context


@contextmanager
def capture_outputs(
    context_provider: Callable[[], dict[str, str]] = context_provider
) -> Generator[None, None, None]:
    if "PYSPARK_SPARK_SESSION_UUID" in os.environ:
        marker: str = "PYTHON_WORKER_LOGGING"
        worker_id: str = str(os.getpid())
        json_out = original_stdout = sys.stdout
        delegate = original_stderr = sys.stderr

        handler = logging.StreamHandler(json_out)
        handler.setFormatter(JSONFormatterWithMarker(marker, worker_id, context_provider))
        logger = logging.getLogger()
        try:
            sys.stdout = JsonOutput(
                delegate, json_out, "stdout", logging.INFO, marker, worker_id, context_provider
            )
            sys.stderr = JsonOutput(
                delegate, json_out, "stderr", logging.ERROR, marker, worker_id, context_provider
            )
            logger.addHandler(handler)
            try:
                yield
            finally:
                # Send an empty line to indicate the end of the outputs.
                json_out.write(f"{marker}:{worker_id}:\n")
                json_out.flush()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            logger.removeHandler(handler)
            handler.close()
    else:
        yield
