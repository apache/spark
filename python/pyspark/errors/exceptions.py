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

from typing import Dict, Optional, cast

from pyspark.errors.utils import ErrorClassesReader


class PySparkException(Exception):
    """
    Base Exception for handling errors generated from PySpark.
    """

    def __init__(
        self,
        message: Optional[str] = None,
        error_class: Optional[str] = None,
        message_parameters: Optional[Dict[str, str]] = None,
    ):
        # `message` vs `error_class` & `message_parameters` are mutually exclusive.
        assert (message is not None and (error_class is None and message_parameters is None)) or (
            message is None and (error_class is not None and message_parameters is not None)
        )

        self.error_reader = ErrorClassesReader()

        if message is None:
            self.message = self.error_reader.get_error_message(
                cast(str, error_class), cast(Dict[str, str], message_parameters)
            )
        else:
            self.message = message

        self.error_class = error_class
        self.message_parameters = message_parameters

    def getErrorClass(self) -> Optional[str]:
        """
        Returns an error class as a string.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getMessageParameters`
        """
        return self.error_class

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        """
        Returns a message parameters as a dictionary.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        """
        return self.message_parameters

    def __str__(self) -> str:
        return f"[{self.getErrorClass()}] {self.message}"
