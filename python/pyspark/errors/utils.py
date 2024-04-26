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
import os
from typing import Any, Callable, Dict, Match, TypeVar, Type, TYPE_CHECKING
from pyspark.errors.error_classes import ERROR_CLASSES_MAP


if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from py4j.java_gateway import JavaClass

T = TypeVar("T")


class ErrorClassesReader:
    """
    A reader to load error information from error_classes.py.
    """

    def __init__(self) -> None:
        self.error_info_map = ERROR_CLASSES_MAP

    def get_error_message(self, error_class: str, message_parameters: Dict[str, str]) -> str:
        """
        Returns the completed error message by applying message parameters to the message template.
        """
        message_template = self.get_message_template(error_class)
        # Verify message parameters.
        message_parameters_from_template = re.findall("<([a-zA-Z0-9_-]+)>", message_template)
        assert set(message_parameters_from_template) == set(message_parameters), (
            f"Undefined error message parameter for error class: {error_class}. "
            f"Parameters: {message_parameters}"
        )

        def replace_match(match: Match[str]) -> str:
            return match.group().translate(str.maketrans("<>", "{}"))

        # Convert <> to {} only when paired.
        message_template = re.sub(r"<([^<>]*)>", replace_match, message_template)

        return message_template.format(**message_parameters)

    def get_message_template(self, error_class: str) -> str:
        """
        Returns the message template for corresponding error class from error_classes.py.

        For example,
        when given `error_class` is "EXAMPLE_ERROR_CLASS",
        and corresponding error class in error_classes.py looks like the below:

        .. code-block:: python

            "EXAMPLE_ERROR_CLASS" : {
              "message" : [
                "Problem <A> because of <B>."
              ]
            }

        In this case, this function returns:
        "Problem <A> because of <B>."

        For sub error class, when given `error_class` is "EXAMPLE_ERROR_CLASS.SUB_ERROR_CLASS",
        and corresponding error class in error_classes.py looks like the below:

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
        error_classes = error_class.split(".")
        len_error_classes = len(error_classes)
        assert len_error_classes in (1, 2)

        # Generate message template for main error class.
        main_error_class = error_classes[0]
        if main_error_class in self.error_info_map:
            main_error_class_info_map = self.error_info_map[main_error_class]
        else:
            raise ValueError(f"Cannot find main error class '{main_error_class}'")

        main_message_template = "\n".join(main_error_class_info_map["message"])

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
            message_template = main_message_template + " " + sub_message_template

        return message_template


def _capture_call_site(
    spark_session: "SparkSession", pyspark_origin: "JavaClass", fragment: str
) -> None:
    """
    Capture the call site information including file name, line number, and function name.
    This function updates the thread-local storage from JVM side (PySparkCurrentOrigin)
    with the current call site information when a PySpark API function is called.

    Parameters
    ----------
    spark_session : SparkSession
        Current active Spark session.
    pyspark_origin : py4j.JavaClass
        PySparkCurrentOrigin from current active Spark session.
    fragment : str
        The name of the PySpark API function being captured.

    Notes
    -----
    The call site information is used to enhance error messages with the exact location
    in the user code that led to the error.
    """
    stack = list(reversed(inspect.stack()))
    depth = int(
        spark_session.conf.get("spark.sql.stackTracesInDataFrameContext")  # type: ignore[arg-type]
    )
    selected_frames = stack[:depth]
    call_sites = [f"{frame.filename}:{frame.lineno}" for frame in selected_frames]
    call_sites_str = "\n".join(call_sites)

    pyspark_origin.set(fragment, call_sites_str)


def _with_origin(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator to capture and provide the call site information to the server side
    when PySpark API functions are invoked.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None and hasattr(func, "__name__"):
            assert spark._jvm is not None
            pyspark_origin = spark._jvm.org.apache.spark.sql.catalyst.trees.PySparkCurrentOrigin

            # Update call site when the function is called
            _capture_call_site(spark, pyspark_origin, func.__name__)

            try:
                return func(*args, **kwargs)
            finally:
                pyspark_origin.clear()
        else:
            return func(*args, **kwargs)

    return wrapper


def with_origin_to_class(cls: Type[T]) -> Type[T]:
    """
    Decorate all methods of a class with `_with_origin` to capture call site information.
    """
    if os.environ.get("PYSPARK_PIN_THREAD", "true").lower() == "true":
        for name, method in cls.__dict__.items():
            # Excluding Python magic methods that do not utilize JVM functions.
            if callable(method) and name not in (
                "__init__",
                "__new__",
                "__iter__",
                "__nonzero__",
                "__repr__",
                "__bool__",
            ):
                setattr(cls, name, _with_origin(method))
    return cls
