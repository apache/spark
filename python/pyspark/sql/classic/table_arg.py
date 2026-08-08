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

from typing import cast, Iterable, overload, Sequence, TYPE_CHECKING, Union

from pyspark.sql.classic.column import _to_java_column, _to_seq
from pyspark.sql.table_arg import TableArg as ParentTableArg
from pyspark.sql.utils import get_active_spark_context

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import ColumnOrName


class TableArg(ParentTableArg):
    def __init__(self, j_table_arg: "JavaObject"):
        self._j_table_arg = j_table_arg

    @overload
    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg": ...

    @overload
    def partitionBy(self, __cols: Sequence["ColumnOrName"]) -> "TableArg": ...

    def partitionBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "TableArg":
        sc = get_active_spark_context()
        if len(cols) == 1 and not isinstance(cols[0], str) and isinstance(cols[0], Sequence):
            cols = tuple(cols[0])
        j_cols = _to_seq(sc, cast(Iterable["ColumnOrName"], cols), _to_java_column)
        new_j_table_arg = self._j_table_arg.partitionBy(j_cols)
        return TableArg(new_j_table_arg)

    @overload
    def orderBy(self, *cols: "ColumnOrName") -> "TableArg": ...

    @overload
    def orderBy(self, __cols: Sequence["ColumnOrName"]) -> "TableArg": ...

    def orderBy(self, *cols: Union["ColumnOrName", Sequence["ColumnOrName"]]) -> "TableArg":
        sc = get_active_spark_context()
        if len(cols) == 1 and not isinstance(cols[0], str) and isinstance(cols[0], Sequence):
            cols = tuple(cols[0])
        j_cols = _to_seq(sc, cast(Iterable["ColumnOrName"], cols), _to_java_column)
        new_j_table_arg = self._j_table_arg.orderBy(j_cols)
        return TableArg(new_j_table_arg)

    def withSinglePartition(self) -> "TableArg":
        new_j_table_arg = self._j_table_arg.withSinglePartition()
        return TableArg(new_j_table_arg)
