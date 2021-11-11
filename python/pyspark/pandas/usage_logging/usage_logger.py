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

"""
The reference implementation of usage logger using the Python standard logging library.
"""

from inspect import Signature
import logging
from typing import Any, Optional


def get_logger() -> Any:
    """An entry point of the plug-in and return the usage logger."""
    return PandasOnSparkUsageLogger()


def _format_signature(signature):
    return (
        "({})".format(", ".join([p.name for p in signature.parameters.values()]))
        if signature is not None
        else ""
    )


class PandasOnSparkUsageLogger(object):
    """
    The reference implementation of usage logger.

    The usage logger needs to provide the following methods:

        - log_success(self, class_name, name, duration, signature=None)
        - log_failure(self, class_name, name, ex, duration, signature=None)
        - log_missing(self, class_name, name, is_deprecated=False, signature=None)
    """

    def __init__(self):
        self.logger = logging.getLogger("pyspark.pandas.usage_logger")

    def log_success(
        self, class_name: str, name: str, duration: float, signature: Optional[Signature] = None
    ) -> None:
        """
        Log the function or property call is successfully finished.

        :param class_name: the target class name
        :param name: the target function or property name
        :param duration: the duration to finish the function or property call
        :param signature: the signature if the target is a function, else None
        """
        if self.logger.isEnabledFor(logging.INFO):
            msg = (
                "A {function} `{class_name}.{name}{signature}` was successfully finished "
                "after {duration:.3f} ms."
            ).format(
                class_name=class_name,
                name=name,
                signature=_format_signature(signature),
                duration=duration * 1000,
                function="function" if signature is not None else "property",
            )
            self.logger.info(msg)

    def log_failure(
        self,
        class_name: str,
        name: str,
        ex: Exception,
        duration: float,
        signature: Optional[Signature] = None,
    ) -> None:
        """
        Log the function or property call failed.

        :param class_name: the target class name
        :param name: the target function or property name
        :param ex: the exception causing the failure
        :param duration: the duration until the function or property call fails
        :param signature: the signature if the target is a function, else None
        """
        if self.logger.isEnabledFor(logging.WARNING):
            msg = (
                "A {function} `{class_name}.{name}{signature}` was failed "
                "after {duration:.3f} ms: {msg}"
            ).format(
                class_name=class_name,
                name=name,
                signature=_format_signature(signature),
                msg=str(ex),
                duration=duration * 1000,
                function="function" if signature is not None else "property",
            )
            self.logger.warning(msg)

    def log_missing(
        self,
        class_name: str,
        name: str,
        is_deprecated: bool = False,
        signature: Optional[Signature] = None,
    ) -> None:
        """
        Log the missing or deprecated function or property is called.

        :param class_name: the target class name
        :param name: the target function or property name
        :param is_deprecated: True if the function or property is marked as deprecated
        :param signature: the original function signature if the target is a function, else None
        """
        if self.logger.isEnabledFor(logging.INFO):
            msg = "A {deprecated} {function} `{class_name}.{name}{signature}` was called.".format(
                class_name=class_name,
                name=name,
                signature=_format_signature(signature),
                function="function" if signature is not None else "property",
                deprecated="deprecated" if is_deprecated else "missing",
            )
            self.logger.info(msg)
