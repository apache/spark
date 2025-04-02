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

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
    cast,
)
import cProfile
import inspect
import pstats
import linecache
import os
import atexit
import sys
import warnings

try:
    from memory_profiler import CodeMap, LineProfiler

    has_memory_profiler = True
except Exception:
    has_memory_profiler = False

from pyspark.accumulators import AccumulatorParam
from pyspark.errors import PySparkRuntimeError, PySparkValueError

if TYPE_CHECKING:
    from pyspark.core.context import SparkContext

MemoryTuple = Tuple[float, float, int]
LineProfile = Tuple[int, Optional[MemoryTuple]]
CodeMapDict = Dict[str, List[LineProfile]]


class ProfilerCollector:
    """
    This class keeps track of different profilers on a per
    stage/UDF basis. Also this is used to create new profilers for
    the different stages/UDFs.
    """

    def __init__(
        self,
        profiler_cls: Type["Profiler"],
        udf_profiler_cls: Type["Profiler"],
        memory_profiler_cls: Type["Profiler"],
        dump_path: Optional[str] = None,
    ):
        self.profiler_cls: Type[Profiler] = profiler_cls
        self.udf_profiler_cls: Type[Profiler] = udf_profiler_cls
        self.memory_profiler_cls: Type[Profiler] = memory_profiler_cls
        self.profile_dump_path: Optional[str] = dump_path
        self.profilers: List[List[Any]] = []

    def new_profiler(self, ctx: "SparkContext") -> "Profiler":
        """Create a new profiler using class `profiler_cls`"""
        return self.profiler_cls(ctx)

    def new_udf_profiler(self, ctx: "SparkContext") -> "Profiler":
        """Create a new profiler using class `udf_profiler_cls`"""
        return self.udf_profiler_cls(ctx)

    def new_memory_profiler(self, ctx: "SparkContext") -> "Profiler":
        """Create a new profiler using class `memory_profiler_cls`"""
        return self.memory_profiler_cls(ctx)

    def add_profiler(self, id: int, profiler: "Profiler") -> None:
        """Add a profiler for RDD/UDF `id`"""
        if not self.profilers:
            if self.profile_dump_path:
                atexit.register(self.dump_profiles, self.profile_dump_path)
            else:
                atexit.register(self.show_profiles)

        self.profilers.append([id, profiler, False])

    def dump_profiles(self, path: str) -> None:
        """Dump the profile stats into directory `path`"""
        for id, profiler, _ in self.profilers:
            profiler.dump(id, path)
        self.profilers = []

    def show_profiles(self) -> None:
        """Print the profile stats to stdout"""
        for i, (id, profiler, showed) in enumerate(self.profilers):
            if not showed and profiler:
                profiler.show(id)
                # mark it as showed
                self.profilers[i][2] = True


class Profiler:
    """
    PySpark supports custom profilers, this is to allow for different profilers to
    be used as well as outputting to different formats than what is provided in the
    BasicProfiler.

    A custom profiler has to define or inherit the following methods:
        profile - will produce a system profile of some sort.
        stats - return the collected stats.
        dump - dumps the profiles to a path
        add - adds a profile to the existing accumulated profile

    The profiler class is chosen when creating a SparkContext

    Examples
    --------
    >>> from pyspark import SparkConf, SparkContext
    >>> from pyspark import BasicProfiler
    >>> class MyCustomProfiler(BasicProfiler):
    ...     def show(self, id):
    ...         print("My custom profiles for RDD:%s" % id)
    ...
    >>> conf = SparkConf().set("spark.python.profile", "true")
    >>> sc = SparkContext('local', 'test', conf=conf, profiler_cls=MyCustomProfiler)
    >>> sc.parallelize(range(1000)).map(lambda x: 2 * x).take(10)
    [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    >>> sc.parallelize(range(1000)).count()
    1000
    >>> sc.show_profiles()
    My custom profiles for RDD:1
    My custom profiles for RDD:3
    >>> sc.stop()

    Notes
    -----
    This API is a developer API.
    """

    def __init__(self, ctx: "SparkContext") -> None:
        pass

    def profile(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Do profiling on the function `func`"""
        raise NotImplementedError

    def stats(self) -> Union[pstats.Stats, Dict]:
        """Return the collected profiling stats"""
        raise NotImplementedError

    def show(self, id: int) -> None:
        """Print the profile stats to stdout"""
        raise NotImplementedError

    def dump(self, id: int, path: str) -> None:
        """Dump the profile into path"""
        raise NotImplementedError


if has_memory_profiler:

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
                    (sub_lines, start_line) = inspect.getsourcelines(code)
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
                line_iterator = ((line, measures[line]) for line in measures.keys())
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


class PStatsParam(AccumulatorParam[Optional[pstats.Stats]]):
    """PStatsParam is used to merge pstats.Stats"""

    @staticmethod
    def zero(value: Optional[pstats.Stats]) -> None:
        return None

    @staticmethod
    def addInPlace(
        value1: Optional[pstats.Stats], value2: Optional[pstats.Stats]
    ) -> Optional[pstats.Stats]:
        if value1 is None:
            return value2
        value1.add(value2)
        return value1


class MemUsageParam(AccumulatorParam[Optional[CodeMapDict]]):
    """MemUsageParam is used to merge memory usage code map"""

    @staticmethod
    def zero(value: Optional[CodeMapDict]) -> None:
        return None

    @staticmethod
    def addInPlace(
        value1: Optional[CodeMapDict], value2: Optional[CodeMapDict]
    ) -> Optional[CodeMapDict]:
        # An example value looks as below
        # {'<command-1598004922717618>': [(3, (144.2578125, 144.2578125, 1)),
        #   (4, (0.0, 144.2578125, 1))]}
        if value1 is None or len(value1) == 0:
            return value2
        if value2 is None or len(value2) == 0:
            return value1

        # value1, value2 should have same keys - file name
        for filename in value1:
            l1 = cast(List[LineProfile], value1.get(filename))
            l2 = cast(List[LineProfile], value2.get(filename))
            c1 = dict((k, v) for k, v in l1)
            c2 = dict((k, v) for k, v in l2)
            udf_code_map: Dict[int, Optional[MemoryTuple]] = {}
            all_line_numbers = set(c1.keys()) | set(c2.keys())
            for lineno in all_line_numbers:
                c1_line = c1.get(lineno)
                c2_line = c2.get(lineno)
                if c1_line and c2_line:
                    # c1, c2 should have same keys - line number
                    udf_code_map[lineno] = (
                        cast(MemoryTuple, c1_line)[0] + cast(MemoryTuple, c2_line)[0],  # increment
                        cast(MemoryTuple, c1_line)[1] + cast(MemoryTuple, c2_line)[1],  # mem_usage
                        cast(MemoryTuple, c1_line)[2]
                        + cast(MemoryTuple, c2_line)[2],  # occurrences
                    )
                elif c1_line:
                    udf_code_map[lineno] = cast(MemoryTuple, c1_line)
                elif c2_line:
                    udf_code_map[lineno] = cast(MemoryTuple, c2_line)
                else:
                    udf_code_map[lineno] = None
            value1[filename] = [(k, v) for k, v in udf_code_map.items()]
        return value1


class BasicProfiler(Profiler):
    """
    BasicProfiler is the default profiler, which is implemented based on
    cProfile and Accumulator
    """

    def __init__(self, ctx: "SparkContext") -> None:
        super().__init__(ctx)
        # Creates a new accumulator for combining the profiles of different
        # partitions of a stage
        self._accumulator = ctx.accumulator(None, PStatsParam)  # type: ignore[arg-type]

    def profile(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Runs and profiles the method to_profile passed in. A profile object is returned."""
        pr = cProfile.Profile()
        ret = pr.runcall(func, *args, **kwargs)
        st = pstats.Stats(pr)
        st.stream = None  # type: ignore[attr-defined]  # make it picklable
        st.strip_dirs()

        # Adds a new profile to the existing accumulated value
        self._accumulator.add(st)  # type: ignore[arg-type]

        return ret

    def stats(self) -> pstats.Stats:
        return cast(pstats.Stats, self._accumulator.value)

    def show(self, id: int) -> None:
        """Print the profile stats to stdout, id is the RDD id"""
        stats = self.stats()
        if stats:
            print("=" * 60)
            print("Profile of RDD<id=%d>" % id)
            print("=" * 60)
            stats.sort_stats("time", "cumulative").print_stats()

    def dump(self, id: int, path: str) -> None:
        """Dump the profile into path, id is the RDD id"""
        if not os.path.exists(path):
            os.makedirs(path)
        stats = self.stats()
        if stats:
            p = os.path.join(path, "rdd_%d.pstats" % id)
            stats.dump_stats(p)


class UDFBasicProfiler(BasicProfiler):
    """
    UDFBasicProfiler is the profiler for Python/Pandas UDFs.
    """

    def show(self, id: int) -> None:
        """Print the profile stats to stdout, id is the PythonUDF id"""
        stats = self.stats()
        if stats:
            print("=" * 60)
            print("Profile of UDF<id=%d>" % id)
            print("=" * 60)
            stats.sort_stats("time", "cumulative").print_stats()

    def dump(self, id: int, path: str) -> None:
        """Dump the profile into path, id is the PythonUDF id"""
        if not os.path.exists(path):
            os.makedirs(path)
        stats = self.stats()
        if stats:
            p = os.path.join(path, "udf_%d.pstats" % id)
            stats.dump_stats(p)


class MemoryProfiler(Profiler):
    """
    MemoryProfiler, which is implemented based on memory profiler and Accumulator
    """

    def __init__(self, ctx: "SparkContext") -> None:
        super().__init__(ctx)
        # Creates a new accumulator for combining the profiles
        self._accumulator = ctx.accumulator(None, MemUsageParam)  # type: ignore[arg-type]

    def profile(  # type: ignore
        self,
        sub_lines: Optional[List],
        start_line: Optional[int],
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Runs and profiles the method func passed in. A profile object is returned."""
        if has_memory_profiler:
            profiler = UDFLineProfiler()
            wrapped = profiler(func, sub_lines=sub_lines, start_line=start_line)
            ret = wrapped(*args, **kwargs)
            codemap_dict = {
                filename: list(line_iterator)
                for filename, line_iterator in profiler.code_map.items()
            }
            # Adds a new profile to the existing accumulated value
            self._accumulator.add(codemap_dict)  # type: ignore[arg-type]
            return ret
        else:
            raise PySparkRuntimeError(
                errorClass="MISSING_LIBRARY_FOR_PROFILER",
                messageParameters={},
            )

    def stats(self) -> CodeMapDict:
        """Return the collected memory profiles"""
        return cast(CodeMapDict, self._accumulator.value)

    @staticmethod
    def _show_results(
        code_map: CodeMapDict, stream: Optional[Any] = None, precision: int = 1
    ) -> None:
        if stream is None:
            stream = sys.stdout
        template = "{0:>6} {1:>12} {2:>12}  {3:>10}   {4:<}"

        for filename, lines in code_map.items():
            header = template.format(
                "Line #", "Mem usage", "Increment", "Occurrences", "Line Contents"
            )

            stream.write("Filename: " + filename + "\n\n")
            stream.write(header + "\n")
            stream.write("=" * len(header) + "\n")

            all_lines = linecache.getlines(filename)
            if len(all_lines) == 0:
                raise PySparkValueError(
                    errorClass="MEMORY_PROFILE_INVALID_SOURCE", messageParameters={}
                )

            float_format = "{0}.{1}f".format(precision + 4, precision)
            template_mem = "{0:" + float_format + "} MiB"

            lines_dict = {line[0]: line[1] for line in lines}
            linenos = range(min(lines_dict), max(lines_dict) + 1)
            for lineno in linenos:
                total_mem: Union[float, str]
                inc: Union[float, str]
                occurrences: Union[float, str]
                mem = lines_dict.get(lineno)
                if mem:
                    inc = mem[0]
                    total_mem = mem[1]
                    total_mem = template_mem.format(total_mem)
                    occurrences = mem[2]
                    inc = template_mem.format(inc)
                else:
                    total_mem = ""
                    inc = ""
                    occurrences = ""
                tmp = template.format(lineno, total_mem, inc, occurrences, all_lines[lineno - 1])
                stream.write(tmp)
            stream.write("\n\n")

    def show(self, id: int) -> None:
        """Print the profile stats to stdout, id is the PythonUDF id"""
        code_map = self.stats()
        if code_map:
            print("=" * 60)
            print("Profile of UDF<id=%d>" % id)
            print("=" * 60)
            self._show_results(code_map)

    def dump(self, id: int, path: str) -> None:
        """Dump the memory profile into path, id is the PythonUDF id"""
        if not os.path.exists(path):
            os.makedirs(path)
        stats = self.stats()  # dict
        if stats:
            p = os.path.join(path, "udf_%d_memory.txt" % id)
            with open(p, "w+") as f:
                self._show_results(stats, stream=f)


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
