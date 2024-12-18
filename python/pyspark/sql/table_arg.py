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

from typing import TYPE_CHECKING

from pyspark.sql.classic.column import _to_java_column, _to_seq
from pyspark.sql.tvf_argument import TableValuedFunctionArgument
from pyspark.sql.utils import get_active_spark_context


if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import ColumnOrName


class TableArg(TableValuedFunctionArgument):
    def __init__(self, j_table_arg: "JavaObject"):
        self._j_table_arg = j_table_arg

    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg":
        sc = get_active_spark_context()
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        j_cols = _to_seq(sc, cols, _to_java_column)
        new_j_table_arg = self._j_table_arg.partitionBy(j_cols)
        return TableArg(new_j_table_arg)

    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        sc = get_active_spark_context()
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        j_cols = _to_seq(sc, cols, _to_java_column)
        new_j_table_arg = self._j_table_arg.orderBy(j_cols)
        return TableArg(new_j_table_arg)

    def withSinglePartition(self) -> "TableArg":
        new_j_table_arg = self._j_table_arg.withSinglePartition()
        return TableArg(new_j_table_arg)
