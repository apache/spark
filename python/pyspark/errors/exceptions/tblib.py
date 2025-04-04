"""
Class for parsing Python tracebacks.

This module was adapted from the `tblib` package https://github.com/ionelmc/python-tblib
modified to also recover line content from the traceback.

BSD 2-Clause License

Copyright (c) 2013-2023, Ionel Cristian Mărieș. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of
    conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list
    of conditions and the following disclaimer in the documentation and/or other materials
    provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import re
import sys
from types import CodeType, FrameType, TracebackType
from typing import Any, Dict, List, Optional

__version__ = "3.0.0"
__all__ = "Traceback", "TracebackParseError", "Frame", "Code"

FRAME_RE = re.compile(
    r'^\s*File "(?P<co_filename>.+)", line (?P<tb_lineno>\d+)(, in (?P<co_name>.+))?$'
)


class _AttrDict(dict):
    __slots__ = ()

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


# noinspection PyPep8Naming
class __traceback_maker(Exception):
    pass


class TracebackParseError(Exception):
    pass


class Code:
    """
    Class that replicates just enough of the builtin Code object to enable serialization
    and traceback rendering.
    """

    co_code: Optional[bytes] = None

    def __init__(self, code: CodeType) -> None:
        self.co_filename = code.co_filename
        self.co_name: Optional[str] = code.co_name
        self.co_argcount = 0
        self.co_kwonlyargcount = 0
        self.co_varnames = ()
        self.co_nlocals = 0
        self.co_stacksize = 0
        self.co_flags = 64
        self.co_firstlineno = 0


class Frame:
    """
    Class that replicates just enough of the builtin Frame object to enable serialization
    and traceback rendering.

    Args:

        get_locals (callable): A function that take a frame argument and returns a dict.

            See :class:`Traceback` class for example.
    """

    def __init__(self, frame: FrameType, *, get_locals: Any = None) -> None:
        self.f_locals = {} if get_locals is None else get_locals(frame)
        self.f_globals = {k: v for k, v in frame.f_globals.items() if k in ("__file__", "__name__")}
        self.f_code = Code(frame.f_code)
        self.f_lineno = frame.f_lineno

    def clear(self) -> None:
        """
        For compatibility with PyPy 3.5;
        clear() was added to frame in Python 3.4
        and is called by traceback.clear_frames(), which
        in turn is called by unittest.TestCase.assertRaises
        """


class LineCacheEntry(list):
    """
    The list of lines in a file where only some of the lines are available.
    """

    def set_line(self, lineno: int, line: str) -> None:
        self.extend([""] * (lineno - len(self)))
        self[lineno - 1] = line


class Traceback:
    """
    Class that wraps builtin Traceback objects.

    Args:
        get_locals (callable): A function that take a frame argument and returns a dict.

            Ideally you will only return exactly what you need, and only with simple types
            that can be json serializable.

            Example:

            .. code:: python

                def get_locals(frame):
                    if frame.f_locals.get("__tracebackhide__"):
                        return {"__tracebackhide__": True}
                    else:
                        return {}
    """

    tb_next: Optional["Traceback"] = None

    def __init__(self, tb: TracebackType, *, get_locals: Any = None):
        self.tb_frame = Frame(tb.tb_frame, get_locals=get_locals)
        self.tb_lineno = int(tb.tb_lineno)
        self.cached_lines: Dict[str, Dict[int, str]] = {}  # filename -> lineno -> line
        """
        Lines shown in the parsed traceback.
        """

        # Build in place to avoid exceeding the recursion limit
        _tb = tb.tb_next
        prev_traceback = self
        cls = type(self)
        while _tb is not None:
            traceback = object.__new__(cls)
            traceback.tb_frame = Frame(_tb.tb_frame, get_locals=get_locals)
            traceback.tb_lineno = int(_tb.tb_lineno)
            prev_traceback.tb_next = traceback
            prev_traceback = traceback
            _tb = _tb.tb_next

    def populate_linecache(self) -> None:
        """
        For each cached line, update the linecache if the file is not present.
        This helps us show the original lines even if the source file is not available,
        for example when the parsed traceback comes from a different host.
        """
        import linecache

        for filename, lines in self.cached_lines.items():
            entry: list[str] = linecache.getlines(filename, module_globals=None)
            if entry:
                if not isinstance(entry, LineCacheEntry):
                    # no need to update the cache if the file is present
                    continue
            else:
                entry = LineCacheEntry()
                linecache.cache[filename] = (1, None, entry, filename)
            for lineno, line in lines.items():
                entry.set_line(lineno, line)

    def as_traceback(self) -> Optional[TracebackType]:
        """
        Convert to a builtin Traceback object that is usable for raising or rendering a stacktrace.
        """
        current: Optional[Traceback] = self
        top_tb = None
        tb = None
        stub = compile(
            "raise __traceback_maker",
            "<string>",
            "exec",
        )
        while current:
            f_code = current.tb_frame.f_code
            code = stub.replace(
                co_firstlineno=current.tb_lineno,
                co_argcount=0,
                co_filename=f_code.co_filename,
                co_name=f_code.co_name or stub.co_name,
                co_freevars=(),
                co_cellvars=(),
            )

            # noinspection PyBroadException
            try:
                exec(
                    code, dict(current.tb_frame.f_globals), dict(current.tb_frame.f_locals)
                )  # noqa: S102
            except Exception:
                next_tb = sys.exc_info()[2].tb_next  # type: ignore
                if top_tb is None:
                    top_tb = next_tb
                if tb is not None:
                    tb.tb_next = next_tb
                tb = next_tb
                del next_tb

            current = current.tb_next
        try:
            return top_tb
        finally:
            del top_tb
            del tb

    to_traceback = as_traceback

    def as_dict(self) -> dict:
        """
        Converts to a dictionary representation. You can serialize the result to JSON
        as it only has builtin objects like dicts, lists, ints or strings.
        """
        if self.tb_next is None:
            tb_next = None
        else:
            tb_next = self.tb_next.as_dict()

        code = {
            "co_filename": self.tb_frame.f_code.co_filename,
            "co_name": self.tb_frame.f_code.co_name,
        }
        frame = {
            "f_globals": self.tb_frame.f_globals,
            "f_locals": self.tb_frame.f_locals,
            "f_code": code,
            "f_lineno": self.tb_frame.f_lineno,
        }
        return {
            "tb_frame": frame,
            "tb_lineno": self.tb_lineno,
            "tb_next": tb_next,
        }

    to_dict = as_dict

    @classmethod
    def from_dict(cls, dct: dict) -> "Traceback":
        """
        Creates an instance from a dictionary with the same structure as ``.as_dict()`` returns.
        """
        if dct["tb_next"]:
            tb_next = cls.from_dict(dct["tb_next"])
        else:
            tb_next = None

        code = _AttrDict(
            co_filename=dct["tb_frame"]["f_code"]["co_filename"],
            co_name=dct["tb_frame"]["f_code"]["co_name"],
        )
        frame = _AttrDict(
            f_globals=dct["tb_frame"]["f_globals"],
            f_locals=dct["tb_frame"].get("f_locals", {}),
            f_code=code,
            f_lineno=dct["tb_frame"]["f_lineno"],
        )
        tb = _AttrDict(
            tb_frame=frame,
            tb_lineno=dct["tb_lineno"],
            tb_next=tb_next,
        )
        return cls(tb, get_locals=get_all_locals)  # type: ignore

    @classmethod
    def from_string(cls, string: str, strict: bool = True) -> "Traceback":
        """
        Creates an instance by parsing a stacktrace.
        Strict means that parsing stops when lines are not indented by at least two spaces anymore.
        """

        frames: List[Dict[str, str]] = []
        cached_lines: Dict[str, Dict[int, str]] = {}

        lines = string.splitlines()[::-1]
        if strict:  # skip the header
            while lines:
                line = lines.pop()
                if line == "Traceback (most recent call last):":
                    break

        while lines:
            line = lines.pop()
            frame_match = FRAME_RE.match(line)
            if frame_match:
                frames.append(frame_match.groupdict())
                if lines and lines[-1].startswith("    "):  # code for the frame
                    code = lines.pop().strip()
                    filename = frame_match.group("co_filename")
                    lineno = int(frame_match.group("tb_lineno"))
                    cached_lines.setdefault(filename, {}).setdefault(lineno, code)
            elif line.startswith("  "):
                pass
            elif strict:
                break  # traceback ended

        if frames:
            previous = None
            for frame in reversed(frames):
                previous = _AttrDict(
                    frame,
                    tb_frame=_AttrDict(
                        frame,
                        f_globals=_AttrDict(
                            __file__=frame["co_filename"],
                            __name__="?",
                        ),
                        f_locals={},
                        f_code=_AttrDict(frame),
                        f_lineno=int(frame["tb_lineno"]),
                    ),
                    tb_next=previous,
                )
            self = cls(previous)  # type: ignore
            self.cached_lines = cached_lines
            return self
        else:
            raise TracebackParseError("Could not find any frames in %r." % string)


def get_all_locals(frame: FrameType) -> dict:
    return dict(frame.f_locals)
