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
import re
import functools
import inspect
import itertools
import os
import threading
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Match,
    TypeVar,
    Type,
    Optional,
    Union,
    overload,
    cast,
)
import types
from types import FrameType

import pyspark
from pyspark.errors.error_classes import ERROR_CLASSES_MAP

T = TypeVar("T")
FuncT = TypeVar("FuncT", bound=Callable[..., Any])

_current_origin = threading.local()

# Providing DataFrame debugging options to reduce performance slowdown.
# Default is True.
_enable_debugging_cache = None

# `spark.sql.stackTracesInDataFrameContext` cached as a (session, value) pair, so that it is
# not read from the JVM on every decorated API call.
_stack_traces_in_context_cache: Optional[Any] = None


def is_debugging_enabled() -> bool:
    global _enable_debugging_cache

    if _enable_debugging_cache is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            _enable_debugging_cache = (
                spark.conf.get(
                    "spark.python.sql.dataFrameDebugging.enabled",
                    "true",  # type: ignore[union-attr]
                ).lower()
                == "true"
            )
        else:
            _enable_debugging_cache = False

    return _enable_debugging_cache


def _get_stack_traces_in_context(spark: Any) -> int:
    """
    Return `spark.sql.stackTracesInDataFrameContext`, cached per session.

    Reading it is a JVM call (an RPC with Spark Connect), and it is read on every decorated
    API call, so the value is cached against the session it was read from.
    """
    global _stack_traces_in_context_cache

    cached = _stack_traces_in_context_cache
    if cached is not None and cached[0] is spark:
        return cached[1]

    depth = int(spark.conf.get("spark.sql.stackTracesInDataFrameContext"))
    _stack_traces_in_context_cache = (spark, depth)
    return depth


def current_origin() -> threading.local:
    global _current_origin

    if not hasattr(_current_origin, "fragment"):
        _current_origin.fragment = None
    if not hasattr(_current_origin, "call_site"):
        _current_origin.call_site = None
    return _current_origin


def set_current_origin(fragment: Optional[str], call_site: Optional[str]) -> None:
    global _current_origin

    _current_origin.fragment = fragment
    _current_origin.call_site = call_site


class ErrorClassesReader:
    """
    A reader to load error information from error-conditions.json.
    """

    def __init__(self) -> None:
        self.error_info_map = ERROR_CLASSES_MAP

    def get_sqlstate(self, errorClass: Optional[str]) -> Optional[str]:
        """
        Returns the SQL state for the given error class.
        """
        if errorClass is None:
            return None

        error_classes = errorClass.split(".")
        try:
            if len(error_classes) == 1:
                return self.error_info_map[errorClass]["sqlState"]
            else:
                return self.error_info_map[error_classes[0]]["sub_class"][error_classes[1]][
                    "sqlState"
                ]
        except KeyError:
            return None

    def get_error_message(self, errorClass: str, messageParameters: Dict[str, str]) -> str:
        """
        Returns the completed error message by applying message parameters to the message template.
        """
        message_template = self.get_message_template(errorClass)
        # Verify message parameters.
        message_parameters_from_template = re.findall("<([a-zA-Z0-9_-]+)>", message_template)
        assert set(message_parameters_from_template) == set(messageParameters), (
            f"Undefined error message parameter for error class: {errorClass}. "
            f"Parameters: {messageParameters}"
        )

        def replace_match(match: Match[str]) -> str:
            return match.group().translate(str.maketrans("<>", "{}"))

        # Convert <> to {} only when paired.
        message_template = re.sub(r"<([^<>]*)>", replace_match, message_template)

        return message_template.format(**messageParameters)

    def get_message_template(self, errorClass: str) -> str:
        """
        Returns the message template for corresponding error class from error-conditions.json.

        For example,
        when given `errorClass` is "EXAMPLE_ERROR_CLASS",
        and corresponding error class in error-conditions.json looks like the below:

        .. code-block:: python

            "EXAMPLE_ERROR_CLASS" : {
              "message" : [
                "Problem <A> because of <B>."
              ]
            }

        In this case, this function returns:
        "Problem <A> because of <B>."

        For sub error class, when given `errorClass` is "EXAMPLE_ERROR_CLASS.SUB_ERROR_CLASS",
        and corresponding error class in error-conditions.json looks like the below:

        .. code-block:: python

            "EXAMPLE_ERROR_CLASS" : {
              "message" : [
                "Problem <A> because of <B>."
              ],
              "sub_class" : {
                "SUB_ERROR_CLASS" : {
                  "message" : [
                    "Do <C> to fix the problem."
                  ]
                }
              }
            }

        In this case, this function returns:
        "Problem <A> because <B>. Do <C> to fix the problem."
        """
        error_classes = errorClass.split(".")
        len_error_classes = len(error_classes)
        assert len_error_classes in (1, 2)

        # Generate message template for main error class.
        main_error_class = error_classes[0]
        if main_error_class in self.error_info_map:
            main_error_class_info_map = self.error_info_map[main_error_class]
        else:
            raise ValueError(f"Cannot find main error class '{main_error_class}'")

        main_message_template = "\n".join(main_error_class_info_map["message"])
        if "breaking_change_info" in main_error_class_info_map:
            main_message_template += " " + "\n".join(
                main_error_class_info_map["breaking_change_info"]["migration_message"]
            )

        has_sub_class = len_error_classes == 2

        if not has_sub_class:
            message_template = main_message_template
        else:
            # Generate message template for sub error class if exists.
            sub_error_class = error_classes[1]
            main_error_class_subclass_info_map = main_error_class_info_map["sub_class"]
            if sub_error_class in main_error_class_subclass_info_map:
                sub_error_class_info_map = main_error_class_subclass_info_map[sub_error_class]
            else:
                raise ValueError(f"Cannot find sub error class '{sub_error_class}'")

            sub_message_template = "\n".join(sub_error_class_info_map["message"])
            if "breaking_change_info" in sub_error_class_info_map:
                sub_message_template += " " + "\n".join(
                    sub_error_class_info_map["breaking_change_info"]["migration_message"]
                )
            message_template = main_message_template + " " + sub_message_template

        return message_template

    def get_breaking_change_info(self, errorClass: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Returns the breaking change info for an error if it is present.
        """
        if errorClass is None:
            return None
        error_classes = errorClass.split(".")
        len_error_classes = len(error_classes)
        assert len_error_classes in (1, 2)

        main_error_class = error_classes[0]
        if main_error_class in self.error_info_map:
            main_error_class_info_map = self.error_info_map[main_error_class]
        else:
            raise ValueError(f"Cannot find main error class '{main_error_class}'")

        if len_error_classes == 2:
            sub_error_class = error_classes[1]
            main_error_class_subclass_info_map = main_error_class_info_map["sub_class"]
            if sub_error_class in main_error_class_subclass_info_map:
                sub_error_class_info_map = main_error_class_subclass_info_map[sub_error_class]
            else:
                raise ValueError(f"Cannot find sub error class '{sub_error_class}'")
            if "breaking_change_info" in sub_error_class_info_map:
                return sub_error_class_info_map["breaking_change_info"]
        if "breaking_change_info" in main_error_class_info_map:
            return main_error_class_info_map["breaking_change_info"]
        return None


@functools.lru_cache(maxsize=1)
def _get_ipython_roots() -> Optional[List[str]]:
    """
    Return the package roots to filter out when IPython is available, or None otherwise.

    Python does not cache failed imports, so probing IPython on every call site capture
    repeats the whole module search. The probe is cached here instead.
    """
    try:
        import IPython

        # ipykernel is required for IPython
        import ipykernel
    except ImportError:
        return None
    return [os.path.dirname(IPython.__file__), os.path.dirname(ipykernel.__file__)]


def _capture_call_site(depth: int) -> str:
    """
    Capture the call site information including file name, line number, and function name.
    This function updates the thread-local storage from JVM side (PySparkCurrentOrigin)
    with the current call site information when a PySpark API function is called.

    Notes
    -----
    The call site information is used to enhance error messages with the exact location
    in the user code that led to the error.
    """
    # Filtering out PySpark code and keeping user code only
    pyspark_root = os.path.dirname(pyspark.__file__)

    def inspect_stack() -> Iterator[FrameType]:
        frame = inspect.currentframe()
        while frame:
            yield frame
            frame = frame.f_back

    stack = (f for f in inspect_stack() if pyspark_root not in f.f_code.co_filename)

    selected_frames: Iterator[FrameType] = itertools.islice(stack, depth)

    # IPython is not a required dependency, so it is probed lazily. A failed import is not
    # cached by Python, so the probe is done once here and the result reused; otherwise every
    # call repeats the full module search.
    ipython_roots = _get_ipython_roots()
    if ipython_roots is not None:
        import IPython

        ipython = IPython.get_ipython()
        # Filtering out IPython related frames
        selected_frames = (
            frame
            for frame in selected_frames
            if all(root not in frame.f_code.co_filename for root in ipython_roots)
        )
    else:
        ipython = None

    # Identifying the cell is useful when the error is generated from IPython Notebook
    if ipython:
        call_sites = [
            f"line {frame.f_lineno} in cell [{ipython.execution_count}]"
            for frame in selected_frames
        ]
    else:
        call_sites = [f"{frame.f_code.co_filename}:{frame.f_lineno}" for frame in selected_frames]
    call_sites_str = "\n".join(call_sites)

    return call_sites_str


def _with_origin(func: FuncT) -> FuncT:
    """
    A decorator to capture and provide the call site information to the server side
    when PySpark API functions are invoked.

    Notes
    -----
    A PySpark API can invoke other decorated APIs internally, for example
    :meth:`Column.__and__` calls :func:`lit`. Only the outermost call is captured because
    that is the one closest to the user code, mirroring the JVM side `withOrigin`. Nested
    calls are skipped entirely, so they neither overwrite nor clear the captured origin.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        from pyspark.sql import SparkSession
        from pyspark.sql.utils import is_remote

        if hasattr(func, "__name__") and is_debugging_enabled():
            if current_origin().fragment is not None:
                # Already within a decorated API; keep the outermost origin.
                return func(*args, **kwargs)

            if is_remote():
                # Getting the configuration requires RPC call. Uses the default value for now.
                depth = 1
                set_current_origin(func.__name__, _capture_call_site(depth))

                try:
                    return func(*args, **kwargs)
                finally:
                    set_current_origin(None, None)
            else:
                spark = SparkSession.getActiveSession()
                if spark is None:
                    return func(*args, **kwargs)
                assert spark._jvm is not None
                jvm_pyspark_origin = getattr(
                    spark._jvm, "org.apache.spark.sql.catalyst.trees.PySparkCurrentOrigin"
                )
                call_site = _capture_call_site(_get_stack_traces_in_context(spark))
                # Update call site when the function is called. The call site is also kept
                # on the Python side so that nested calls can detect the outermost one.
                set_current_origin(func.__name__, call_site)
                jvm_pyspark_origin.set(func.__name__, call_site)

                try:
                    return func(*args, **kwargs)
                finally:
                    jvm_pyspark_origin.clear()
                    set_current_origin(None, None)
        else:
            return func(*args, **kwargs)

    # Marks the wrapper so that the decoration can be asserted on, since `functools.wraps`
    # makes it otherwise indistinguishable from the function it wraps.
    wrapper.__origin_wrapped__ = True  # type: ignore[attr-defined]
    return cast(FuncT, wrapper)


def _is_origin_wrapped(func: Any) -> bool:
    """
    Whether `func` is decorated with `_with_origin`, looking through other decorators.
    """
    while func is not None:
        if getattr(func, "__origin_wrapped__", False):
            return True
        func = getattr(func, "__wrapped__", None)
    return False


@overload
def with_origin_to_class(
    cls_or_ignores: Type[T], ignores: Optional[List[str]] = None
) -> Type[T]: ...


@overload
def with_origin_to_class(
    cls_or_ignores: Optional[List[str]] = None,
) -> Callable[[Type[T]], Type[T]]: ...


def with_origin_to_class(
    cls_or_ignores: Optional[Union[Type[T], List[str]]] = None, ignores: Optional[List[str]] = None
) -> Union[Type[T], Callable[[Type[T]], Type[T]]]:
    """
    Decorate all methods of a class with `_with_origin` to capture call site information.
    """
    if cls_or_ignores is None or isinstance(cls_or_ignores, list):
        ignores = cls_or_ignores or []
        return lambda cls: with_origin_to_class(cls, ignores)
    else:
        cls = cls_or_ignores
        if os.environ.get("PYSPARK_PIN_THREAD", "true").lower() == "true":
            skipping = set(
                ["__init__", "__new__", "__iter__", "__nonzero__", "__repr__", "__bool__"]
                + (ignores or [])
            )
            for name, method in cls.__dict__.items():
                # Excluding Python magic methods that do not utilize JVM functions.
                if callable(method) and name not in skipping:
                    setattr(cls, name, _with_origin(method))
        return cls


# Functions that take a user defined callable or an arbitrary expression string. The
# interesting call site for those is inside the user's own function, or the expression
# itself, so decorating them only adds overhead.
_ORIGIN_IGNORED_FUNCTIONS = frozenset(
    [
        "udf",
        "udtf",
        "arrow_udf",
        "arrow_udtf",
        "pandas_udf",
        "expr",
        "broadcast",
        "call_udf",
        "call_function",
    ]
)


def with_origin_to_functions(module_name: str) -> None:
    """
    Decorate the public functions of a ``pyspark.sql.functions`` module with `_with_origin`
    to capture call site information.

    Expressions are commonly built from these functions rather than from
    :class:`Column` methods, so without this a DataFrame query context cannot point at the
    user code that built a failing expression.
    """
    if os.environ.get("PYSPARK_PIN_THREAD", "true").lower() != "true":
        return

    import sys

    module = sys.modules[module_name]
    for name, func in list(vars(module).items()):
        if name.startswith("_") or name in _ORIGIN_IGNORED_FUNCTIONS:
            continue
        # Only functions defined in this module; leave imported helpers and classes alone.
        if not isinstance(func, types.FunctionType) or func.__module__ != module_name:
            continue
        setattr(module, name, _with_origin(func))
