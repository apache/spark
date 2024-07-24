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
A collections of partitioning functions
"""

import sys
from typing import (
    TYPE_CHECKING,
    Union,
)

from pyspark.errors import PySparkTypeError
from pyspark.sql.column import Column
from pyspark.sql.functions.builtin import _invoke_function_over_columns, _invoke_function
from pyspark.sql.utils import (
    try_partitioning_remote_functions as _try_partitioning_remote_functions,
    get_active_spark_context as _get_active_spark_context,
)

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


@_try_partitioning_remote_functions
def years(col: "ColumnOrName") -> Column:
    """
    Partition transform function: A transform for timestamps and dates
    to partition data into years.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by years.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(  # doctest: +SKIP
    ...     partitioning.years("ts")
    ... ).createOrReplace()

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return _invoke_function_over_columns("years", col)


@_try_partitioning_remote_functions
def months(col: "ColumnOrName") -> Column:
    """
    Partition transform function: A transform for timestamps and dates
    to partition data into months.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by months.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(
    ...     partitioning.months("ts")
    ... ).createOrReplace()  # doctest: +SKIP

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return _invoke_function_over_columns("months", col)


@_try_partitioning_remote_functions
def days(col: "ColumnOrName") -> Column:
    """
    Partition transform function: A transform for timestamps and dates
    to partition data into days.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by days.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(  # doctest: +SKIP
    ...     partitioning.days("ts")
    ... ).createOrReplace()

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return _invoke_function_over_columns("days", col)


@_try_partitioning_remote_functions
def hours(col: "ColumnOrName") -> Column:
    """
    Partition transform function: A transform for timestamps
    to partition data into hours.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by hours.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(   # doctest: +SKIP
    ...     partitioning.hours("ts")
    ... ).createOrReplace()

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return _invoke_function_over_columns("hours", col)


@_try_partitioning_remote_functions
def bucket(numBuckets: Union[Column, int], col: "ColumnOrName") -> Column:
    """
    Partition transform function: A transform for any type that partitions
    by a hash of the input column.

    .. versionadded:: 4.0.0

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(  # doctest: +SKIP
    ...     partitioning.bucket(42, "ts")
    ... ).createOrReplace()

    Parameters
    ----------
    numBuckets : :class:`~pyspark.sql.Column` or int
        the number of buckets
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by given columns.

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    from pyspark.sql.classic.column import _to_java_column, _create_column_from_literal

    if not isinstance(numBuckets, (int, Column)):
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT",
            message_parameters={
                "arg_name": "numBuckets",
                "arg_type": type(numBuckets).__name__,
            },
        )

    _get_active_spark_context()
    numBuckets = (
        _create_column_from_literal(numBuckets)
        if isinstance(numBuckets, int)
        else _to_java_column(numBuckets)
    )
    return _invoke_function("bucket", numBuckets, _to_java_column(col))


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.functions.partitioning

    globs = pyspark.sql.functions.partitioning.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("sql.functions.partitioning tests")
        .getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions.partitioning,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
