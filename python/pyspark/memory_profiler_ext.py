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

from types import CodeType
from typing import Any, Optional, List, Iterator, Tuple, Type, TYPE_CHECKING, Callable
import inspect
import warnings

if TYPE_CHECKING:
    has_memory_profiler: bool
    try:
        from memory_profiler import CodeMap, LineProfiler

        CodeMapForUDF: Type[CodeMap]
        CodeMapForUDFV2: Type[CodeMap]
        UDFLineProfiler: Type[LineProfiler]
        UDFLineProfilerV2: Type[LineProfiler]
    except Exception:
        pass


__all__ = [
    "has_memory_profiler",
    "CodeMapForUDF",
    "CodeMapForUDFV2",
    "UDFLineProfiler",
    "UDFLineProfilerV2",
]

_module_initialized = False
_has_memory_profiler = None


def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    if not _module_initialized:
        _init_module()
    if name == "has_memory_profiler":
        return _has_memory_profiler
    elif name in globals():
        return globals()[name]
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _init_module() -> None:
    global _has_memory_profiler
    global _module_initialized

    try:
        from memory_profiler import CodeMap, LineProfiler

        _has_memory_profiler = True
    except Exception:
        _has_memory_profiler = False

    if not _has_memory_profiler:
        _module_initialized = True
        return

    class CodeMapForUDF(CodeMap):
        def add(
            self,
            code: Any,
            toplevel_code: Optional[Any] = None,
            *,
            sub_lines: Optional[List] = None,
            start_line: Optional[int] = None,
        ) -> None:
            if code in self:
                return

            if toplevel_code is None:
                toplevel_code = code
                filename = code.co_filename
                if sub_lines is None or start_line is None:
                    sub_lines, start_line = inspect.getsourcelines(code)
                linenos = range(start_line, start_line + len(sub_lines))
                self._toplevel.append((filename, code, linenos))
                self[code] = {}
            else:
                self[code] = self[toplevel_code]
            for subcode in filter(inspect.iscode, code.co_consts):
                self.add(subcode, toplevel_code=toplevel_code)

    class CodeMapForUDFV2(CodeMap):
        def add(
            self,
            code: Any,
            toplevel_code: Optional[Any] = None,
        ) -> None:
            if code in self:
                return

            if toplevel_code is None:
                toplevel_code = code
                filename = code.co_filename
                self._toplevel.append((filename, code))
                self[code] = {}
            else:
                self[code] = self[toplevel_code]
            for subcode in filter(inspect.iscode, code.co_consts):
                self.add(subcode, toplevel_code=toplevel_code)

        def items(self) -> Iterator[Tuple[str, Iterator[Tuple[int, Any]]]]:
            """Iterate on the toplevel code blocks."""
            for filename, code in self._toplevel:
                measures = self[code]
                if not measures:
                    continue  # skip if no measurement
                line_iterator = ((line, measures[line]) for line in measures)
                yield (filename, line_iterator)

    class UDFLineProfiler(LineProfiler):
        def __init__(self, **kw: Any) -> None:
            super().__init__(**kw)
            include_children = kw.get("include_children", False)
            backend = kw.get("backend", "psutil")
            self.code_map = CodeMapForUDF(include_children=include_children, backend=backend)

        def __call__(
            self,
            func: Optional[Callable[..., Any]] = None,
            precision: int = 1,
            *,
            sub_lines: Optional[List] = None,
            start_line: Optional[int] = None,
        ) -> Callable[..., Any]:
            if func is not None:
                self.add_function(func, sub_lines=sub_lines, start_line=start_line)
                f = self.wrap_function(func)
                f.__module__ = func.__module__
                f.__name__ = func.__name__
                f.__doc__ = func.__doc__
                f.__dict__.update(getattr(func, "__dict__", {}))
                return f
            else:

                def inner_partial(f: Callable[..., Any]) -> Any:
                    return self.__call__(f, precision=precision)

                return inner_partial

        def add_function(
            self,
            func: Callable[..., Any],
            *,
            sub_lines: Optional[List] = None,
            start_line: Optional[int] = None,
        ) -> None:
            """Record line profiling information for the given Python function."""
            try:
                # func_code does not exist in Python3
                code = func.__code__
            except AttributeError:
                warnings.warn("Could not extract a code object for the object %r" % func)
            else:
                self.code_map.add(code, sub_lines=sub_lines, start_line=start_line)

    class UDFLineProfilerV2(LineProfiler):
        def __init__(self, **kw: Any) -> None:
            super().__init__(**kw)
            include_children = kw.get("include_children", False)
            backend = kw.get("backend", "psutil")
            self.code_map = CodeMapForUDFV2(include_children=include_children, backend=backend)

        def add_code(self, code: CodeType) -> None:
            """Record line profiling information for the given code object."""
            self.code_map.add(code)

    for name in __all__:
        if name != "has_memory_profiler":
            globals()[name] = locals()[name]

    _module_initialized = True
