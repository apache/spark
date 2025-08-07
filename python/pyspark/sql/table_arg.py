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

# mypy: disable-error-code="empty-body"

from typing import TYPE_CHECKING

from pyspark.sql.tvf_argument import TableValuedFunctionArgument
from pyspark.sql.utils import dispatch_table_arg_method


if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


class TableArg(TableValuedFunctionArgument):
    """
    Represents a table argument in PySpark.

    This class provides methods to specify partitioning, ordering, and
    single-partition constraints when passing a DataFrame as a table argument
    to TVF(Table-Valued Function)s including UDTF(User-Defined Table Function)s.
    """

    @dispatch_table_arg_method
    def partitionBy(self, *cols: "ColumnOrName") -> "TableArg":
        """
        Partitions the data based on the specified columns.
        """
        ...

    @dispatch_table_arg_method
    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        """
        Orders the data within each partition by the specified columns.
        """
        ...

    @dispatch_table_arg_method
    def withSinglePartition(self) -> "TableArg":
        """
        Forces the data to be processed in a single partition.
        """
        ...
