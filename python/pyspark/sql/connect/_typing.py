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

import sys

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

from typing import Union, Optional
import datetime
import decimal

from pyspark.sql.connect.column import Column


ColumnOrName = Union[Column, str]

PrimitiveType = Union[bool, float, int, str]

OptionalPrimitiveType = Optional[PrimitiveType]

LiteralType = PrimitiveType

DecimalLiteral = decimal.Decimal

DateTimeLiteral = Union[datetime.datetime, datetime.date]


class UserDefinedFunctionCallable(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...
