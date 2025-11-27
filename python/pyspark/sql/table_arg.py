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

        This method partitions the table argument data by the specified columns.
        It must be called before `orderBy()` and cannot be called after
        `withSinglePartition()` has been called.

        Parameters
        ----------
        cols : str, :class:`Column`, or list
            Column names or :class:`Column` objects to partition by.

        Returns
        -------
        :class:`TableArg`
            A new `TableArg` instance with partitioning applied.

        Examples
        --------
        >>> from pyspark.sql.functions import udtf
        >>>
        >>> @udtf(returnType="key: int, value: string")
        ... class ProcessUDTF:
        ...     def eval(self, row):
        ...         yield row["key"], row["value"]
        ...
        >>> df = spark.createDataFrame(
        ...     [(1, "a"), (1, "b"), (2, "c"), (2, "d")], ["key", "value"]
        ... )
        >>>
        >>> # Partition by a single column
        >>> result = ProcessUDTF(df.asTable().partitionBy("key"))
        >>> result.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    a|
        |  1|    b|
        |  2|    c|
        |  2|    d|
        +---+-----+
        >>>
        >>> # Partition by multiple columns
        >>> df2 = spark.createDataFrame(
        ...     [(1, "x", 10), (1, "x", 20), (2, "y", 30)], ["key", "category", "value"]
        ... )
        >>> result2 = ProcessUDTF(df2.asTable().partitionBy("key", "category"))
        >>> result2.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    x|
        |  1|    x|
        |  2|    y|
        +---+-----+
        """
        ...

    @dispatch_table_arg_method
    def orderBy(self, *cols: "ColumnOrName") -> "TableArg":
        """
        Orders the data within each partition by the specified columns.

        This method orders the data within partitions. It must be called after
        `partitionBy()` or `withSinglePartition()` has been called.

        Parameters
        ----------
        cols : str, :class:`Column`, or list
            Column names or :class:`Column` objects to order by. Columns can be
            ordered in ascending or descending order using :meth:`Column.asc` or
            :meth:`Column.desc`.

        Returns
        -------
        :class:`TableArg`
            A new `TableArg` instance with ordering applied.

        Examples
        --------
        >>> from pyspark.sql.functions import udtf
        >>>
        >>> @udtf(returnType="key: int, value: string")
        ... class ProcessUDTF:
        ...     def eval(self, row):
        ...         yield row["key"], row["value"]
        ...
        >>> df = spark.createDataFrame(
        ...     [(1, "b"), (1, "a"), (2, "d"), (2, "c")], ["key", "value"]
        ... )
        >>>
        >>> # Order by a single column within partitions
        >>> result = ProcessUDTF(df.asTable().partitionBy("key").orderBy("value"))
        >>> result.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    a|
        |  1|    b|
        |  2|    c|
        |  2|    d|
        +---+-----+
        >>>
        >>> # Order by multiple columns
        >>> df2 = spark.createDataFrame(
        ...     [(1, "a", 2), (1, "a", 1), (1, "b", 3)], ["key", "value", "num"]
        ... )
        >>> result2 = ProcessUDTF(df2.asTable().partitionBy("key").orderBy("value", "num"))
        >>> result2.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    a|
        |  1|    a|
        |  1|    b|
        +---+-----+
        >>>
        >>> # Order by descending order
        >>> result3 = ProcessUDTF(df.asTable().partitionBy("key").orderBy(df.value.desc()))
        >>> result3.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    b|
        |  1|    a|
        |  2|    d|
        |  2|    c|
        +---+-----+
        """
        ...

    @dispatch_table_arg_method
    def withSinglePartition(self) -> "TableArg":
        """
        Forces the data to be processed in a single partition.

        This method indicates that all data should be treated as a single partition.
        It cannot be called after `partitionBy()` has been called. `orderBy()` can
        be called after this method to order the data within the single partition.

        Returns
        -------
        :class:`TableArg`
            A new `TableArg` instance with single partition constraint applied.

        Examples
        --------
        >>> from pyspark.sql.functions import udtf
        >>>
        >>> @udtf(returnType="key: int, value: string")
        ... class ProcessUDTF:
        ...     def eval(self, row):
        ...         yield row["key"], row["value"]
        ...
        >>> df = spark.createDataFrame(
        ...     [(1, "a"), (2, "b"), (3, "c")], ["key", "value"]
        ... )
        >>>
        >>> # Process all data in a single partition
        >>> result = ProcessUDTF(df.asTable().withSinglePartition())
        >>> result.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    a|
        |  2|    b|
        |  3|    c|
        +---+-----+
        >>>
        >>> # Use withSinglePartition and orderBy together
        >>> df2 = spark.createDataFrame(
        ...     [(3, "c"), (1, "a"), (2, "b")], ["key", "value"]
        ... )
        >>> result2 = ProcessUDTF(df2.asTable().withSinglePartition().orderBy("key"))
        >>> result2.show()
        +---+-----+
        |key|value|
        +---+-----+
        |  1|    a|
        |  2|    b|
        |  3|    c|
        +---+-----+
        """
        ...
