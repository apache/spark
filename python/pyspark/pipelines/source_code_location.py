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
import inspect
import traceback
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SourceCodeLocation:
    filename: str
    line_number: Optional[int]  # 1-indexed


def get_caller_source_code_location(stacklevel: int) -> SourceCodeLocation:
    """
    Returns a SourceCodeLocation object representing the location code that invokes this function.

    :param stacklevel: The number of stack frames to go up. 0 means the direct caller of this
        function, 1 means the caller of the caller, and so on.
    """
    # Stack is ordered from the caller first to the callee last.
    stack = traceback.extract_stack(inspect.currentframe())

    frame = stack[-(stacklevel + 2)]

    return SourceCodeLocation(
        filename=frame.filename,
        line_number=frame.lineno,
    )
