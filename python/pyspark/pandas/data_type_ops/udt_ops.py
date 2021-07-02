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

from typing import Any

from pyspark.pandas._typing import IndexOpsLike, SeriesOrIndex
from pyspark.pandas.data_type_ops.base import DataTypeOps


class UDTOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with Spark type:
    UserDefinedType or its subclasses.
    """

    @property
    def pretty_name(self) -> str:
        return "user defined types"

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary - can not be applied to %s." % self.pretty_name)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary ~ can not be applied to %s." % self.pretty_name)

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("abs() can not be applied to %s." % self.pretty_name)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("< can not be applied to %s." % self.pretty_name)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("<= can not be applied to %s." % self.pretty_name)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("> can not be applied to %s." % self.pretty_name)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError(">= can not be applied to %s." % self.pretty_name)
