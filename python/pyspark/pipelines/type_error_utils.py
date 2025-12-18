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
from typing import List, Optional
from pyspark.errors import PySparkTypeError


def validate_optional_list_of_str_arg(arg_name: str, arg_value: Optional[List[str]]) -> None:
    """
    Validates types for Optional[list] parameters.

    Validation succeeds if both:
    - The value provided to the parameter is either a list or None.
    - All elements inside the list are instances of the expected element type.

    Otherwise, raises an error.
    """
    if arg_value is None:
        return

    if not isinstance(arg_value, list):
        raise PySparkTypeError(
            errorClass="NOT_LIST_OF_STR",
            messageParameters={
                "arg_name": arg_name,
                "arg_type": type(arg_value).__name__,
            },
        )

    for el in arg_value:
        if not isinstance(el, str):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OF_STR",
                messageParameters={
                    "arg_name": arg_name,
                    "arg_type": type(arg_value).__name__,
                },
            )
