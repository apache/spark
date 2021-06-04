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
An internal immutable DataFrame with some metadata to manage indexes.
"""
import re
from typing import Dict, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING, cast
from itertools import accumulate
import py4j

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype, is_datetime64_dtype, is_datetime64tz_dtype
from pyspark import sql as spark
from pyspark._globals import _NoValue, _NoValueType
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import BooleanType, DataType, StructField, StructType, LongType

# For running doctests and reference resolution in PyCharm.
from pyspark import pandas as ps  # noqa: F401

if TYPE_CHECKING:
    # This is required in old Python 3.5 to prevent circular reference.
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)
from pyspark.pandas.config import get_option
from pyspark.pandas.typedef import (
    Dtype,
    as_spark_type,
    extension_dtypes,
    infer_pd_series_spark_type,
    spark_type_to_pandas_dtype,
)
from pyspark.pandas.utils import (
    column_labels_level,
    default_session,
    is_name_like_tuple,
    is_testing,
    lazy_property,
    name_like_string,
    scol_for,
    spark_column_equals,
    verify_temp_column_name,
)


# A function to turn given numbers to Spark columns that represent pandas-on-Spark index.
SPARK_INDEX_NAME_FORMAT = "__index_level_{}__".format
SPARK_DEFAULT_INDEX_NAME = SPARK_INDEX_NAME_FORMAT(0)
# A pattern to check if the name of a Spark column is a pandas-on-Spark index name or not.
SPARK_INDEX_NAME_PATTERN = re.compile(r"__index_level_[0-9]+__")

NATURAL_ORDER_COLUMN_NAME = "__natural_order__"

HIDDEN_COLUMNS = {NATURAL_ORDER_COLUMN_NAME}

DEFAULT_SERIES_NAME = 0
SPARK_DEFAULT_SERIES_NAME = str(DEFAULT_SERIES_NAME)


class InternalFrame(object):
    """
    The internal immutable DataFrame which manages Spark DataFrame and column names and index
    information.

    .. note:: this is an internal class. It is not supposed to be exposed to users and users
        should not directly access to it.

    The internal immutable DataFrame represents the index information for a DataFrame it belongs to.
    For instance, if we have a pandas-on-Spark DataFrame as below, pandas DataFrame does not
    store the index as columns.

    >>> psdf = ps.DataFrame({
    ...     'A': [1, 2, 3, 4],
    ...     'B': [5, 6, 7, 8],
    ...     'C': [9, 10, 11, 12],
    ...     'D': [13, 14, 15, 16],
    ...     'E': [17, 18, 19, 20]}, columns = ['A', 'B', 'C', 'D', 'E'])
    >>> psdf  # doctest: +NORMALIZE_WHITESPACE
       A  B   C   D   E
    0  1  5   9  13  17
    1  2  6  10  14  18
    2  3  7  11  15  19
    3  4  8  12  16  20

    However, all columns including index column are also stored in Spark DataFrame internally
    as below.

    >>> psdf._internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +-----------------+---+---+---+---+---+
    |__index_level_0__|  A|  B|  C|  D|  E|
    +-----------------+---+---+---+---+---+
    |                0|  1|  5|  9| 13| 17|
    |                1|  2|  6| 10| 14| 18|
    |                2|  3|  7| 11| 15| 19|
    |                3|  4|  8| 12| 16| 20|
    +-----------------+---+---+---+---+---+

    In order to fill this gap, the current metadata is used by mapping Spark's internal column
    to pandas-on-Spark's index. See the method below:

    * `spark_frame` represents the internal Spark DataFrame

    * `data_spark_column_names` represents non-indexing Spark column names

    * `data_spark_columns` represents non-indexing Spark columns

    * `data_dtypes` represents external non-indexing dtypes

    * `index_spark_column_names` represents internal index Spark column names

    * `index_spark_columns` represents internal index Spark columns

    * `index_dtypes` represents external index dtypes

    * `spark_column_names` represents all columns

    * `index_names` represents the external index name as a label

    * `to_internal_spark_frame` represents Spark DataFrame derived by the metadata. Includes index.

    * `to_pandas_frame` represents pandas DataFrame derived by the metadata

    >>> internal = psdf._internal
    >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    +-----------------+---+---+---+---+---+-----------------+
    |__index_level_0__|  A|  B|  C|  D|  E|__natural_order__|
    +-----------------+---+---+---+---+---+-----------------+
    |                0|  1|  5|  9| 13| 17|              ...|
    |                1|  2|  6| 10| 14| 18|              ...|
    |                2|  3|  7| 11| 15| 19|              ...|
    |                3|  4|  8| 12| 16| 20|              ...|
    +-----------------+---+---+---+---+---+-----------------+
    >>> internal.data_spark_column_names
    ['A', 'B', 'C', 'D', 'E']
    >>> internal.index_spark_column_names
    ['__index_level_0__']
    >>> internal.spark_column_names
    ['__index_level_0__', 'A', 'B', 'C', 'D', 'E']
    >>> internal.index_names
    [None]
    >>> internal.data_dtypes
    [dtype('int64'), dtype('int64'), dtype('int64'), dtype('int64'), dtype('int64')]
    >>> internal.index_dtypes
    [dtype('int64')]
    >>> internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +-----------------+---+---+---+---+---+
    |__index_level_0__|  A|  B|  C|  D|  E|
    +-----------------+---+---+---+---+---+
    |                0|  1|  5|  9| 13| 17|
    |                1|  2|  6| 10| 14| 18|
    |                2|  3|  7| 11| 15| 19|
    |                3|  4|  8| 12| 16| 20|
    +-----------------+---+---+---+---+---+
    >>> internal.to_pandas_frame
       A  B   C   D   E
    0  1  5   9  13  17
    1  2  6  10  14  18
    2  3  7  11  15  19
    3  4  8  12  16  20

    In case that index is set to one of the existing column as below:

    >>> psdf1 = psdf.set_index("A")
    >>> psdf1  # doctest: +NORMALIZE_WHITESPACE
       B   C   D   E
    A
    1  5   9  13  17
    2  6  10  14  18
    3  7  11  15  19
    4  8  12  16  20

    >>> psdf1._internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+---+---+
    |  A|  B|  C|  D|  E|
    +---+---+---+---+---+
    |  1|  5|  9| 13| 17|
    |  2|  6| 10| 14| 18|
    |  3|  7| 11| 15| 19|
    |  4|  8| 12| 16| 20|
    +---+---+---+---+---+

    >>> internal = psdf1._internal
    >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    +-----------------+---+---+---+---+---+-----------------+
    |__index_level_0__|  A|  B|  C|  D|  E|__natural_order__|
    +-----------------+---+---+---+---+---+-----------------+
    |                0|  1|  5|  9| 13| 17|              ...|
    |                1|  2|  6| 10| 14| 18|              ...|
    |                2|  3|  7| 11| 15| 19|              ...|
    |                3|  4|  8| 12| 16| 20|              ...|
    +-----------------+---+---+---+---+---+-----------------+
    >>> internal.data_spark_column_names
    ['B', 'C', 'D', 'E']
    >>> internal.index_spark_column_names
    ['A']
    >>> internal.spark_column_names
    ['A', 'B', 'C', 'D', 'E']
    >>> internal.index_names
    [('A',)]
    >>> internal.data_dtypes
    [dtype('int64'), dtype('int64'), dtype('int64'), dtype('int64')]
    >>> internal.index_dtypes
    [dtype('int64')]
    >>> internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+---+---+---+
    |  A|  B|  C|  D|  E|
    +---+---+---+---+---+
    |  1|  5|  9| 13| 17|
    |  2|  6| 10| 14| 18|
    |  3|  7| 11| 15| 19|
    |  4|  8| 12| 16| 20|
    +---+---+---+---+---+
    >>> internal.to_pandas_frame  # doctest: +NORMALIZE_WHITESPACE
       B   C   D   E
    A
    1  5   9  13  17
    2  6  10  14  18
    3  7  11  15  19
    4  8  12  16  20

    In case that index becomes a multi index as below:

    >>> psdf2 = psdf.set_index("A", append=True)
    >>> psdf2  # doctest: +NORMALIZE_WHITESPACE
         B   C   D   E
      A
    0 1  5   9  13  17
    1 2  6  10  14  18
    2 3  7  11  15  19
    3 4  8  12  16  20

    >>> psdf2._internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +-----------------+---+---+---+---+---+
    |__index_level_0__|  A|  B|  C|  D|  E|
    +-----------------+---+---+---+---+---+
    |                0|  1|  5|  9| 13| 17|
    |                1|  2|  6| 10| 14| 18|
    |                2|  3|  7| 11| 15| 19|
    |                3|  4|  8| 12| 16| 20|
    +-----------------+---+---+---+---+---+

    >>> internal = psdf2._internal
    >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    +-----------------+---+---+---+---+---+-----------------+
    |__index_level_0__|  A|  B|  C|  D|  E|__natural_order__|
    +-----------------+---+---+---+---+---+-----------------+
    |                0|  1|  5|  9| 13| 17|              ...|
    |                1|  2|  6| 10| 14| 18|              ...|
    |                2|  3|  7| 11| 15| 19|              ...|
    |                3|  4|  8| 12| 16| 20|              ...|
    +-----------------+---+---+---+---+---+-----------------+
    >>> internal.data_spark_column_names
    ['B', 'C', 'D', 'E']
    >>> internal.index_spark_column_names
    ['__index_level_0__', 'A']
    >>> internal.spark_column_names
    ['__index_level_0__', 'A', 'B', 'C', 'D', 'E']
    >>> internal.index_names
    [None, ('A',)]
    >>> internal.data_dtypes
    [dtype('int64'), dtype('int64'), dtype('int64'), dtype('int64')]
    >>> internal.index_dtypes
    [dtype('int64'), dtype('int64')]
    >>> internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +-----------------+---+---+---+---+---+
    |__index_level_0__|  A|  B|  C|  D|  E|
    +-----------------+---+---+---+---+---+
    |                0|  1|  5|  9| 13| 17|
    |                1|  2|  6| 10| 14| 18|
    |                2|  3|  7| 11| 15| 19|
    |                3|  4|  8| 12| 16| 20|
    +-----------------+---+---+---+---+---+
    >>> internal.to_pandas_frame  # doctest: +NORMALIZE_WHITESPACE
         B   C   D   E
      A
    0 1  5   9  13  17
    1 2  6  10  14  18
    2 3  7  11  15  19
    3 4  8  12  16  20

    For multi-level columns, it also holds column_labels

    >>> columns = pd.MultiIndex.from_tuples([('X', 'A'), ('X', 'B'),
    ...                                      ('Y', 'C'), ('Y', 'D')])
    >>> psdf3 = ps.DataFrame([
    ...     [1, 2, 3, 4],
    ...     [5, 6, 7, 8],
    ...     [9, 10, 11, 12],
    ...     [13, 14, 15, 16],
    ...     [17, 18, 19, 20]], columns = columns)
    >>> psdf3  # doctest: +NORMALIZE_WHITESPACE
        X       Y
        A   B   C   D
    0   1   2   3   4
    1   5   6   7   8
    2   9  10  11  12
    3  13  14  15  16
    4  17  18  19  20

    >>> internal = psdf3._internal
    >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    +-----------------+------+------+------+------+-----------------+
    |__index_level_0__|(X, A)|(X, B)|(Y, C)|(Y, D)|__natural_order__|
    +-----------------+------+------+------+------+-----------------+
    |                0|     1|     2|     3|     4|              ...|
    |                1|     5|     6|     7|     8|              ...|
    |                2|     9|    10|    11|    12|              ...|
    |                3|    13|    14|    15|    16|              ...|
    |                4|    17|    18|    19|    20|              ...|
    +-----------------+------+------+------+------+-----------------+
    >>> internal.data_spark_column_names
    ['(X, A)', '(X, B)', '(Y, C)', '(Y, D)']
    >>> internal.column_labels
    [('X', 'A'), ('X', 'B'), ('Y', 'C'), ('Y', 'D')]

    For Series, it also holds scol to represent the column.

    >>> psseries = psdf1.B
    >>> psseries
    A
    1    5
    2    6
    3    7
    4    8
    Name: B, dtype: int64

    >>> internal = psseries._internal
    >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    +-----------------+---+---+---+---+---+-----------------+
    |__index_level_0__|  A|  B|  C|  D|  E|__natural_order__|
    +-----------------+---+---+---+---+---+-----------------+
    |                0|  1|  5|  9| 13| 17|              ...|
    |                1|  2|  6| 10| 14| 18|              ...|
    |                2|  3|  7| 11| 15| 19|              ...|
    |                3|  4|  8| 12| 16| 20|              ...|
    +-----------------+---+---+---+---+---+-----------------+
    >>> internal.data_spark_column_names
    ['B']
    >>> internal.index_spark_column_names
    ['A']
    >>> internal.spark_column_names
    ['A', 'B']
    >>> internal.index_names
    [('A',)]
    >>> internal.data_dtypes
    [dtype('int64')]
    >>> internal.index_dtypes
    [dtype('int64')]
    >>> internal.to_internal_spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE
    +---+---+
    |  A|  B|
    +---+---+
    |  1|  5|
    |  2|  6|
    |  3|  7|
    |  4|  8|
    +---+---+
    >>> internal.to_pandas_frame  # doctest: +NORMALIZE_WHITESPACE
       B
    A
    1  5
    2  6
    3  7
    4  8
    """

    def __init__(
        self,
        spark_frame: spark.DataFrame,
        index_spark_columns: Optional[List[spark.Column]],
        index_names: Optional[List[Optional[Tuple]]] = None,
        index_dtypes: Optional[List[Dtype]] = None,
        column_labels: Optional[List[Tuple]] = None,
        data_spark_columns: Optional[List[spark.Column]] = None,
        data_dtypes: Optional[List[Dtype]] = None,
        column_label_names: Optional[List[Optional[Tuple]]] = None,
    ):
        """
        Create a new internal immutable DataFrame to manage Spark DataFrame, column fields and
        index fields and names.

        :param spark_frame: Spark DataFrame to be managed.
        :param index_spark_columns: list of Spark Column
                                    Spark Columns for the index.
        :param index_names: list of tuples
                            the index names.
        :param index_dtypes: list of dtypes
                             the index dtypes.
        :param column_labels: list of tuples with the same length
                              The multi-level values in the tuples.
        :param data_spark_columns: list of Spark Column
                                   Spark Columns to appear as columns. If this is None, calculated
                                   from spark_frame.
        :param data_dtypes: list of dtypes.
                            the data dtypes.
        :param column_label_names: Names for each of the column index levels.

        See the examples below to refer what each parameter means.

        >>> column_labels = pd.MultiIndex.from_tuples(
        ...     [('a', 'x'), ('a', 'y'), ('b', 'z')], names=["column_labels_a", "column_labels_b"])
        >>> row_index = pd.MultiIndex.from_tuples(
        ...     [('foo', 'bar'), ('foo', 'bar'), ('zoo', 'bar')],
        ...     names=["row_index_a", "row_index_b"])
        >>> psdf = ps.DataFrame(
        ...     [[1, 2, 3], [4, 5, 6], [7, 8, 9]], index=row_index, columns=column_labels)
        >>> psdf.set_index(('a', 'x'), append=True, inplace=True)
        >>> psdf  # doctest: +NORMALIZE_WHITESPACE
        column_labels_a                  a  b
        column_labels_b                  y  z
        row_index_a row_index_b (a, x)
        foo         bar         1       2  3
                                4       5  6
        zoo         bar         7       8  9

        >>> internal = psdf._internal

        >>> internal.spark_frame.show()  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        +-----------------+-----------------+------+------+------+...
        |__index_level_0__|__index_level_1__|(a, x)|(a, y)|(b, z)|...
        +-----------------+-----------------+------+------+------+...
        |              foo|              bar|     1|     2|     3|...
        |              foo|              bar|     4|     5|     6|...
        |              zoo|              bar|     7|     8|     9|...
        +-----------------+-----------------+------+------+------+...

        >>> internal.index_spark_columns  # doctest: +SKIP
        [Column<'__index_level_0__'>, Column<'__index_level_1__'>, Column<'(a, x)'>]

        >>> internal.index_names
        [('row_index_a',), ('row_index_b',), ('a', 'x')]

        >>> internal.index_dtypes
        [dtype('O'), dtype('O'), dtype('int64')]

        >>> internal.column_labels
        [('a', 'y'), ('b', 'z')]

        >>> internal.data_spark_columns  # doctest: +SKIP
        [Column<'(a, y)'>, Column<'(b, z)'>]

        >>> internal.data_dtypes
        [dtype('int64'), dtype('int64')]

        >>> internal.column_label_names
        [('column_labels_a',), ('column_labels_b',)]
        """

        assert isinstance(spark_frame, spark.DataFrame)
        assert not spark_frame.isStreaming, "pandas-on-Spark does not support Structured Streaming."

        if not index_spark_columns:
            if data_spark_columns is not None:
                if column_labels is not None:
                    data_spark_columns = [
                        scol.alias(name_like_string(label))
                        for scol, label in zip(data_spark_columns, column_labels)
                    ]
                spark_frame = spark_frame.select(data_spark_columns)

            assert not any(SPARK_INDEX_NAME_PATTERN.match(name) for name in spark_frame.columns), (
                "Index columns should not appear in columns of the Spark DataFrame. Avoid "
                "index column names [%s]." % SPARK_INDEX_NAME_PATTERN
            )

            # Create default index.
            spark_frame = InternalFrame.attach_default_index(spark_frame)
            index_spark_columns = [scol_for(spark_frame, SPARK_DEFAULT_INDEX_NAME)]

            if data_spark_columns is not None:
                data_spark_columns = [
                    scol_for(spark_frame, col)
                    for col in spark_frame.columns
                    if col != SPARK_DEFAULT_INDEX_NAME
                ]

        if NATURAL_ORDER_COLUMN_NAME not in spark_frame.columns:
            spark_frame = spark_frame.withColumn(
                NATURAL_ORDER_COLUMN_NAME, F.monotonically_increasing_id()
            )

        self._sdf = spark_frame  # type: spark.DataFrame

        # index_spark_columns
        assert all(
            isinstance(index_scol, spark.Column) for index_scol in index_spark_columns
        ), index_spark_columns

        self._index_spark_columns = index_spark_columns  # type: List[spark.Column]

        # index_names
        if not index_names:
            index_names = [None] * len(index_spark_columns)

        assert len(index_spark_columns) == len(index_names), (
            len(index_spark_columns),
            len(index_names),
        )
        assert all(
            is_name_like_tuple(index_name, check_type=True) for index_name in index_names
        ), index_names

        self._index_names = index_names  # type: List[Optional[Tuple]]

        # index_dtypes
        if not index_dtypes:
            index_dtypes = [None] * len(index_spark_columns)

        assert len(index_spark_columns) == len(index_dtypes), (
            len(index_spark_columns),
            len(index_dtypes),
        )

        index_dtypes = [
            spark_type_to_pandas_dtype(spark_frame.select(scol).schema[0].dataType)
            if dtype is None or dtype == np.dtype("object")
            else dtype
            for dtype, scol in zip(index_dtypes, index_spark_columns)
        ]

        assert all(
            isinstance(dtype, Dtype.__args__)  # type: ignore
            and (dtype == np.dtype("object") or as_spark_type(dtype, raise_error=False) is not None)
            for dtype in index_dtypes
        ), index_dtypes

        self._index_dtypes = index_dtypes  # type: List[Dtype]

        # data_spark-columns
        if data_spark_columns is None:
            data_spark_columns = [
                scol_for(spark_frame, col)
                for col in spark_frame.columns
                if all(
                    not spark_column_equals(scol_for(spark_frame, col), index_scol)
                    for index_scol in index_spark_columns
                )
                and col not in HIDDEN_COLUMNS
            ]
            self._data_spark_columns = data_spark_columns  # type: List[spark.Column]
        else:
            assert all(isinstance(scol, spark.Column) for scol in data_spark_columns)
            self._data_spark_columns = data_spark_columns

        # column_labels
        if column_labels is None:
            self._column_labels = [
                (col,) for col in spark_frame.select(self._data_spark_columns).columns
            ]  # type: List[Tuple]
        else:
            assert len(column_labels) == len(self._data_spark_columns), (
                len(column_labels),
                len(self._data_spark_columns),
            )
            if len(column_labels) == 1:
                column_label = column_labels[0]
                assert is_name_like_tuple(column_label, check_type=True), column_label
            else:
                assert all(
                    is_name_like_tuple(column_label, check_type=True)
                    for column_label in column_labels
                ), column_labels
                assert len(set(len(label) for label in column_labels)) <= 1, column_labels
            self._column_labels = column_labels

        # data_dtypes
        if not data_dtypes:
            data_dtypes = [None] * len(data_spark_columns)

        assert len(data_spark_columns) == len(data_dtypes), (
            len(data_spark_columns),
            len(data_dtypes),
        )

        data_dtypes = [
            spark_type_to_pandas_dtype(spark_frame.select(scol).schema[0].dataType)
            if dtype is None or dtype == np.dtype("object")
            else dtype
            for dtype, scol in zip(data_dtypes, data_spark_columns)
        ]

        assert all(
            isinstance(dtype, Dtype.__args__)  # type: ignore
            and (dtype == np.dtype("object") or as_spark_type(dtype, raise_error=False) is not None)
            for dtype in data_dtypes
        ), data_dtypes

        self._data_dtypes = data_dtypes  # type: List[Dtype]

        # column_label_names
        if column_label_names is None:
            self._column_label_names = [None] * column_labels_level(
                self._column_labels
            )  # type: List[Optional[Tuple]]
        else:
            if len(self._column_labels) > 0:
                assert len(column_label_names) == column_labels_level(self._column_labels), (
                    len(column_label_names),
                    column_labels_level(self._column_labels),
                )
            else:
                assert len(column_label_names) > 0, len(column_label_names)
            assert all(
                is_name_like_tuple(column_label_name, check_type=True)
                for column_label_name in column_label_names
            ), column_label_names
            self._column_label_names = column_label_names

    @staticmethod
    def attach_default_index(
        sdf: spark.DataFrame, default_index_type: Optional[str] = None
    ) -> spark.DataFrame:
        """
        This method attaches a default index to Spark DataFrame. Spark does not have the index
        notion so corresponding column should be generated.
        There are several types of default index can be configured by `compute.default_index_type`.

        >>> spark_frame = ps.range(10).to_spark()
        >>> spark_frame
        DataFrame[id: bigint]

        It adds the default index column '__index_level_0__'.

        >>> spark_frame = InternalFrame.attach_default_index(spark_frame)
        >>> spark_frame
        DataFrame[__index_level_0__: bigint, id: bigint]

        It throws an exception if the given column name already exists.

        >>> InternalFrame.attach_default_index(spark_frame)
        ... # doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        AssertionError: '__index_level_0__' already exists...
        """
        index_column = SPARK_DEFAULT_INDEX_NAME
        assert (
            index_column not in sdf.columns
        ), "'%s' already exists in the Spark column names '%s'" % (index_column, sdf.columns)

        if default_index_type is None:
            default_index_type = get_option("compute.default_index_type")

        if default_index_type == "sequence":
            return InternalFrame.attach_sequence_column(sdf, column_name=index_column)
        elif default_index_type == "distributed-sequence":
            return InternalFrame.attach_distributed_sequence_column(sdf, column_name=index_column)
        elif default_index_type == "distributed":
            return InternalFrame.attach_distributed_column(sdf, column_name=index_column)
        else:
            raise ValueError(
                "'compute.default_index_type' should be one of 'sequence',"
                " 'distributed-sequence' and 'distributed'"
            )

    @staticmethod
    def attach_sequence_column(sdf: spark.DataFrame, column_name: str) -> spark.DataFrame:
        scols = [scol_for(sdf, column) for column in sdf.columns]
        sequential_index = (
            F.row_number().over(Window.orderBy(F.monotonically_increasing_id())).cast("long") - 1
        )
        return sdf.select(sequential_index.alias(column_name), *scols)

    @staticmethod
    def attach_distributed_column(sdf: spark.DataFrame, column_name: str) -> spark.DataFrame:
        scols = [scol_for(sdf, column) for column in sdf.columns]
        return sdf.select(F.monotonically_increasing_id().alias(column_name), *scols)

    @staticmethod
    def attach_distributed_sequence_column(
        sdf: spark.DataFrame, column_name: str
    ) -> spark.DataFrame:
        """
        This method attaches a Spark column that has a sequence in a distributed manner.
        This is equivalent to the column assigned when default index type 'distributed-sequence'.

        >>> sdf = ps.DataFrame(['a', 'b', 'c']).to_spark()
        >>> sdf = InternalFrame.attach_distributed_sequence_column(sdf, column_name="sequence")
        >>> sdf.show()  # doctest: +NORMALIZE_WHITESPACE
        +--------+---+
        |sequence|  0|
        +--------+---+
        |       0|  a|
        |       1|  b|
        |       2|  c|
        +--------+---+
        """
        if len(sdf.columns) > 0:
            try:
                jdf = sdf._jdf.toDF()  # type: ignore

                sql_ctx = sdf.sql_ctx
                encoders = sql_ctx._jvm.org.apache.spark.sql.Encoders  # type: ignore
                encoder = encoders.tuple(jdf.exprEnc(), encoders.scalaLong())

                jrdd = jdf.localCheckpoint(False).rdd().zipWithIndex()

                df = spark.DataFrame(
                    sql_ctx.sparkSession._jsparkSession.createDataset(  # type: ignore
                        jrdd, encoder
                    ).toDF(),
                    sql_ctx,
                )
                columns = df.columns
                return df.selectExpr(
                    "`{}` as `{}`".format(columns[1], column_name), "`{}`.*".format(columns[0])
                )
            except py4j.protocol.Py4JError:
                if is_testing():
                    raise
                return InternalFrame._attach_distributed_sequence_column(sdf, column_name)
        else:
            cnt = sdf.count()
            if cnt > 0:
                return default_session().range(cnt).toDF(column_name)
            else:
                return default_session().createDataFrame(
                    [], schema=StructType().add(column_name, data_type=LongType(), nullable=False)
                )

    @staticmethod
    def _attach_distributed_sequence_column(
        sdf: spark.DataFrame, column_name: str
    ) -> spark.DataFrame:
        """
        >>> sdf = ps.DataFrame(['a', 'b', 'c']).to_spark()
        >>> sdf = InternalFrame._attach_distributed_sequence_column(sdf, column_name="sequence")
        >>> sdf.sort("sequence").show()  # doctest: +NORMALIZE_WHITESPACE
        +--------+---+
        |sequence|  0|
        +--------+---+
        |       0|  a|
        |       1|  b|
        |       2|  c|
        +--------+---+
        """
        scols = [scol_for(sdf, column) for column in sdf.columns]

        spark_partition_column = verify_temp_column_name(sdf, "__spark_partition_id__")
        offset_column = verify_temp_column_name(sdf, "__offset__")
        row_number_column = verify_temp_column_name(sdf, "__row_number__")

        # 1. Calculates counts per each partition ID. `counts` here is, for instance,
        #     {
        #         1: 83,
        #         6: 83,
        #         3: 83,
        #         ...
        #     }
        sdf = sdf.withColumn(spark_partition_column, F.spark_partition_id())

        # Checkpoint the DataFrame to fix the partition ID.
        sdf = sdf.localCheckpoint(eager=False)

        counts = map(
            lambda x: (x["key"], x["count"]),
            sdf.groupby(sdf[spark_partition_column].alias("key")).count().collect(),
        )

        # 2. Calculates cumulative sum in an order of partition id.
        #     Note that it does not matter if partition id guarantees its order or not.
        #     We just need a one-by-one sequential id.

        # sort by partition key.
        sorted_counts = sorted(counts, key=lambda x: x[0])
        # get cumulative sum in an order of partition key.
        cumulative_counts = [0] + list(accumulate(map(lambda count: count[1], sorted_counts)))
        # zip it with partition key.
        sums = dict(zip(map(lambda count: count[0], sorted_counts), cumulative_counts))

        # 3. Attach offset for each partition.
        @pandas_udf(LongType(), PandasUDFType.SCALAR)
        def offset(id: pd.Series) -> pd.Series:
            current_partition_offset = sums[id.iloc[0]]
            return pd.Series(current_partition_offset).repeat(len(id))

        sdf = sdf.withColumn(offset_column, offset(spark_partition_column))

        # 4. Calculate row_number in each partition.
        w = Window.partitionBy(spark_partition_column).orderBy(F.monotonically_increasing_id())
        row_number = F.row_number().over(w)
        sdf = sdf.withColumn(row_number_column, row_number)

        # 5. Calculate the index.
        return sdf.select(
            (sdf[offset_column] + sdf[row_number_column] - 1).alias(column_name), *scols
        )

    def spark_column_for(self, label: Tuple) -> spark.Column:
        """ Return Spark Column for the given column label. """
        column_labels_to_scol = dict(zip(self.column_labels, self.data_spark_columns))
        if label in column_labels_to_scol:
            return column_labels_to_scol[label]
        else:
            raise KeyError(name_like_string(label))

    def spark_column_name_for(self, label_or_scol: Union[Tuple, spark.Column]) -> str:
        """ Return the actual Spark column name for the given column label. """
        if isinstance(label_or_scol, spark.Column):
            scol = label_or_scol
        else:
            scol = self.spark_column_for(label_or_scol)
        return self.spark_frame.select(scol).columns[0]

    def spark_type_for(self, label_or_scol: Union[Tuple, spark.Column]) -> DataType:
        """ Return DataType for the given column label. """
        if isinstance(label_or_scol, spark.Column):
            scol = label_or_scol
        else:
            scol = self.spark_column_for(label_or_scol)
        return self.spark_frame.select(scol).schema[0].dataType

    def spark_column_nullable_for(self, label_or_scol: Union[Tuple, spark.Column]) -> bool:
        """ Return nullability for the given column label. """
        if isinstance(label_or_scol, spark.Column):
            scol = label_or_scol
        else:
            scol = self.spark_column_for(label_or_scol)
        return self.spark_frame.select(scol).schema[0].nullable

    def dtype_for(self, label: Tuple) -> Dtype:
        """ Return dtype for the given column label. """
        column_labels_to_dtype = dict(zip(self.column_labels, self.data_dtypes))
        if label in column_labels_to_dtype:
            return column_labels_to_dtype[label]
        else:
            raise KeyError(name_like_string(label))

    @property
    def spark_frame(self) -> spark.DataFrame:
        """ Return the managed Spark DataFrame. """
        return self._sdf

    @lazy_property
    def data_spark_column_names(self) -> List[str]:
        """ Return the managed column field names. """
        return self.spark_frame.select(self.data_spark_columns).columns

    @property
    def data_spark_columns(self) -> List[spark.Column]:
        """ Return Spark Columns for the managed data columns. """
        return self._data_spark_columns

    @property
    def index_spark_column_names(self) -> List[str]:
        """ Return the managed index field names. """
        return self.spark_frame.select(self.index_spark_columns).columns

    @property
    def index_spark_columns(self) -> List[spark.Column]:
        """ Return Spark Columns for the managed index columns. """
        return self._index_spark_columns

    @lazy_property
    def spark_column_names(self) -> List[str]:
        """ Return all the field names including index field names. """
        return self.spark_frame.select(self.spark_columns).columns

    @lazy_property
    def spark_columns(self) -> List[spark.Column]:
        """ Return Spark Columns for the managed columns including index columns. """
        index_spark_columns = self.index_spark_columns
        return index_spark_columns + [
            spark_column
            for spark_column in self.data_spark_columns
            if all(
                not spark_column_equals(spark_column, scol)
                for scol in index_spark_columns
            )
        ]

    @property
    def index_names(self) -> List[Optional[Tuple]]:
        """ Return the managed index names. """
        return self._index_names

    @lazy_property
    def index_level(self) -> int:
        """ Return the level of the index. """
        return len(self._index_names)

    @property
    def column_labels(self) -> List[Tuple]:
        """ Return the managed column index. """
        return self._column_labels

    @lazy_property
    def column_labels_level(self) -> int:
        """ Return the level of the column index. """
        return len(self._column_label_names)

    @property
    def column_label_names(self) -> List[Optional[Tuple]]:
        """ Return names of the index levels. """
        return self._column_label_names

    @property
    def index_dtypes(self) -> List[Dtype]:
        """ Return dtypes for the managed index columns. """
        return self._index_dtypes

    @property
    def data_dtypes(self) -> List[Dtype]:
        """ Return dtypes for the managed columns. """
        return self._data_dtypes

    @lazy_property
    def to_internal_spark_frame(self) -> spark.DataFrame:
        """
        Return as Spark DataFrame. This contains index columns as well
        and should be only used for internal purposes.
        """
        index_spark_columns = self.index_spark_columns
        data_columns = []
        for spark_column in self.data_spark_columns:
            if all(
                not spark_column_equals(spark_column, scol)
                for scol in index_spark_columns
            ):
                data_columns.append(spark_column)
        return self.spark_frame.select(index_spark_columns + data_columns)

    @lazy_property
    def to_pandas_frame(self) -> pd.DataFrame:
        """ Return as pandas DataFrame. """
        sdf = self.to_internal_spark_frame
        pdf = sdf.toPandas()
        if len(pdf) == 0 and len(sdf.schema) > 0:
            pdf = pdf.astype(
                {field.name: spark_type_to_pandas_dtype(field.dataType) for field in sdf.schema}
            )

        return InternalFrame.restore_index(pdf, **self.arguments_for_restore_index)

    @lazy_property
    def arguments_for_restore_index(self) -> Dict:
        """ Create arguments for `restore_index`. """
        column_names = []
        ext_dtypes = {
            col: dtype
            for col, dtype in zip(self.index_spark_column_names, self.index_dtypes)
            if isinstance(dtype, extension_dtypes)
        }
        categorical_dtypes = {
            col: dtype
            for col, dtype in zip(self.index_spark_column_names, self.index_dtypes)
            if isinstance(dtype, CategoricalDtype)
        }
        for spark_column, column_name, dtype in zip(
            self.data_spark_columns, self.data_spark_column_names, self.data_dtypes
        ):
            for index_spark_column_name, index_spark_column in zip(
                self.index_spark_column_names, self.index_spark_columns
            ):
                if spark_column_equals(spark_column, index_spark_column):
                    column_names.append(index_spark_column_name)
                    break
            else:
                column_names.append(column_name)
                if isinstance(dtype, extension_dtypes):
                    ext_dtypes[column_name] = dtype
                elif isinstance(dtype, CategoricalDtype):
                    categorical_dtypes[column_name] = dtype

        return dict(
            index_columns=self.index_spark_column_names,
            index_names=self.index_names,
            data_columns=column_names,
            column_labels=self.column_labels,
            column_label_names=self.column_label_names,
            ext_dtypes=ext_dtypes,
            categorical_dtypes=categorical_dtypes,
        )

    @staticmethod
    def restore_index(
        pdf: pd.DataFrame,
        *,
        index_columns: List[str],
        index_names: List[Tuple],
        data_columns: List[str],
        column_labels: List[Tuple],
        column_label_names: List[Tuple],
        ext_dtypes: Dict[str, Dtype] = None,
        categorical_dtypes: Dict[str, CategoricalDtype] = None
    ) -> pd.DataFrame:
        """
        Restore pandas DataFrame indices using the metadata.

        :param pdf: the pandas DataFrame to be processed.
        :param index_columns: the original column names for index columns.
        :param index_names: the index names after restored.
        :param data_columns: the original column names for data columns.
        :param column_labels: the column labels after restored.
        :param column_label_names: the column label names after restored.
        :param ext_dtypes: the map from the original column names to extension data types.
        :param categorical_dtypes: the map from the original column names to categorical types.
        :return: the restored pandas DataFrame

        >>> pdf = pd.DataFrame({"index": [10, 20, 30], "a": ['a', 'b', 'c'], "b": [0, 2, 1]})
        >>> InternalFrame.restore_index(
        ...     pdf,
        ...     index_columns=["index"],
        ...     index_names=[("idx",)],
        ...     data_columns=["a", "b", "index"],
        ...     column_labels=[("x",), ("y",), ("z",)],
        ...     column_label_names=[("lv1",)],
        ...     ext_dtypes=None,
        ...     categorical_dtypes={"b": CategoricalDtype(categories=["i", "j", "k"])}
        ... )  # doctest: +NORMALIZE_WHITESPACE
        lv1  x  y   z
        idx
        10   a  i  10
        20   b  k  20
        30   c  j  30
        """
        if ext_dtypes is not None and len(ext_dtypes) > 0:
            pdf = pdf.astype(ext_dtypes, copy=True)

        if categorical_dtypes is not None:
            for col, dtype in categorical_dtypes.items():
                pdf[col] = pd.Categorical.from_codes(
                    pdf[col], categories=dtype.categories, ordered=dtype.ordered
                )

        append = False
        for index_field in index_columns:
            drop = index_field not in data_columns
            pdf = pdf.set_index(index_field, drop=drop, append=append)
            append = True
        pdf = pdf[data_columns]

        pdf.index.names = [
            name if name is None or len(name) > 1 else name[0] for name in index_names
        ]

        names = [name if name is None or len(name) > 1 else name[0] for name in column_label_names]
        if len(column_label_names) > 1:
            pdf.columns = pd.MultiIndex.from_tuples(column_labels, names=names)
        else:
            pdf.columns = pd.Index(
                [None if label is None else label[0] for label in column_labels], name=names[0],
            )

        return pdf

    @lazy_property
    def resolved_copy(self) -> "InternalFrame":
        """ Copy the immutable InternalFrame with the updates resolved. """
        sdf = self.spark_frame.select(self.spark_columns + list(HIDDEN_COLUMNS))
        return self.copy(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in self.index_spark_column_names],
            data_spark_columns=[scol_for(sdf, col) for col in self.data_spark_column_names],
        )

    def with_new_sdf(
        self,
        spark_frame: spark.DataFrame,
        *,
        index_dtypes: Optional[List[Dtype]] = None,
        data_columns: Optional[List[str]] = None,
        data_dtypes: Optional[List[Dtype]] = None
    ) -> "InternalFrame":
        """ Copy the immutable InternalFrame with the updates by the specified Spark DataFrame.

        :param spark_frame: the new Spark DataFrame
        :param index_dtypes: the index dtypes. If None, the original dtyeps are used.
        :param data_columns: the new column names. If None, the original one is used.
        :param data_dtypes: the data dtypes. If None, the original dtyeps are used.
        :return: the copied InternalFrame.
        """
        if index_dtypes is None:
            index_dtypes = self.index_dtypes
        else:
            assert len(index_dtypes) == len(self.index_dtypes), (
                len(index_dtypes),
                len(self.index_dtypes),
            )

        if data_columns is None:
            data_columns = self.data_spark_column_names
        else:
            assert len(data_columns) == len(self.column_labels), (
                len(data_columns),
                len(self.column_labels),
            )

        if data_dtypes is None:
            data_dtypes = self.data_dtypes
        else:
            assert len(data_dtypes) == len(self.column_labels), (
                len(data_dtypes),
                len(self.column_labels),
            )

        sdf = spark_frame.drop(NATURAL_ORDER_COLUMN_NAME)
        return self.copy(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in self.index_spark_column_names],
            index_dtypes=index_dtypes,
            data_spark_columns=[scol_for(sdf, col) for col in data_columns],
            data_dtypes=data_dtypes,
        )

    def with_new_columns(
        self,
        scols_or_pssers: Sequence[Union[spark.Column, "Series"]],
        *,
        column_labels: Optional[List[Tuple]] = None,
        data_dtypes: Optional[List[Dtype]] = None,
        column_label_names: Union[Optional[List[Optional[Tuple]]], _NoValueType] = _NoValue,
        keep_order: bool = True
    ) -> "InternalFrame":
        """
        Copy the immutable InternalFrame with the updates by the specified Spark Columns or Series.

        :param scols_or_pssers: the new Spark Columns or Series.
        :param column_labels: the new column index.
            If None, the column_labels of the corresponding `scols_or_pssers` is used if it is
            Series; otherwise the original one is used.
        :param data_dtypes: the new dtypes.
            If None, the dtypes of the corresponding `scols_or_pssers` is used if it is Series;
            otherwise the dtypes will be inferred from the corresponding `scols_or_pssers`.
        :param column_label_names: the new names of the column index levels.
        :return: the copied InternalFrame.
        """
        from pyspark.pandas.series import Series

        if column_labels is None:
            if all(isinstance(scol_or_psser, Series) for scol_or_psser in scols_or_pssers):
                column_labels = [cast(Series, psser)._column_label for psser in scols_or_pssers]
            else:
                assert len(scols_or_pssers) == len(self.column_labels), (
                    len(scols_or_pssers),
                    len(self.column_labels),
                )
                column_labels = []
                for scol_or_psser, label in zip(scols_or_pssers, self.column_labels):
                    if isinstance(scol_or_psser, Series):
                        column_labels.append(scol_or_psser._column_label)
                    else:
                        column_labels.append(label)
        else:
            assert len(scols_or_pssers) == len(column_labels), (
                len(scols_or_pssers),
                len(column_labels),
            )

        data_spark_columns = []
        for scol_or_psser in scols_or_pssers:
            if isinstance(scol_or_psser, Series):
                scol = scol_or_psser.spark.column
            else:
                scol = scol_or_psser
            data_spark_columns.append(scol)

        if data_dtypes is None:
            data_dtypes = []
            for scol_or_psser in scols_or_pssers:
                if isinstance(scol_or_psser, Series):
                    data_dtypes.append(scol_or_psser.dtype)
                else:
                    data_dtypes.append(None)
        else:
            assert len(scols_or_pssers) == len(data_dtypes), (
                len(scols_or_pssers),
                len(data_dtypes),
            )

        sdf = self.spark_frame
        if not keep_order:
            sdf = self.spark_frame.select(self.index_spark_columns + data_spark_columns)
            index_spark_columns = [scol_for(sdf, col) for col in self.index_spark_column_names]
            data_spark_columns = [
                scol_for(sdf, col) for col in self.spark_frame.select(data_spark_columns).columns
            ]
        else:
            index_spark_columns = self.index_spark_columns

        if column_label_names is _NoValue:
            column_label_names = self._column_label_names

        return self.copy(
            spark_frame=sdf,
            index_spark_columns=index_spark_columns,
            column_labels=column_labels,
            data_spark_columns=data_spark_columns,
            data_dtypes=data_dtypes,
            column_label_names=column_label_names,
        )

    def with_filter(self, pred: Union[spark.Column, "Series"]) -> "InternalFrame":
        """ Copy the immutable InternalFrame with the updates by the predicate.

        :param pred: the predicate to filter.
        :return: the copied InternalFrame.
        """
        from pyspark.pandas.series import Series

        if isinstance(pred, Series):
            assert isinstance(pred.spark.data_type, BooleanType), pred.spark.data_type
            condition = pred.spark.column
        else:
            spark_type = self.spark_frame.select(pred).schema[0].dataType
            assert isinstance(spark_type, BooleanType), spark_type
            condition = pred

        return self.with_new_sdf(self.spark_frame.filter(condition).select(self.spark_columns))

    def with_new_spark_column(
        self,
        column_label: Tuple,
        scol: spark.Column,
        *,
        dtype: Optional[Dtype] = None,
        keep_order: bool = True
    ) -> "InternalFrame":
        """
        Copy the immutable InternalFrame with the updates by the specified Spark Column.

        :param column_label: the column label to be updated.
        :param scol: the new Spark Column
        :param dtype: the new dtype.
            If not specified, the dtypes will be inferred from the spark Column.
        :return: the copied InternalFrame.
        """
        assert column_label in self.column_labels, column_label

        idx = self.column_labels.index(column_label)
        data_spark_columns = self.data_spark_columns.copy()
        data_spark_columns[idx] = scol
        data_dtypes = self.data_dtypes.copy()
        data_dtypes[idx] = dtype
        return self.with_new_columns(
            data_spark_columns, data_dtypes=data_dtypes, keep_order=keep_order
        )

    def select_column(self, column_label: Tuple) -> "InternalFrame":
        """
        Copy the immutable InternalFrame with the specified column.

        :param column_label: the column label to use.
        :return: the copied InternalFrame.
        """
        assert column_label in self.column_labels, column_label

        return self.copy(
            column_labels=[column_label],
            data_spark_columns=[self.spark_column_for(column_label)],
            data_dtypes=[self.dtype_for(column_label)],
            column_label_names=None,
        )

    def copy(
        self,
        *,
        spark_frame: Union[spark.DataFrame, _NoValueType] = _NoValue,
        index_spark_columns: Union[List[spark.Column], _NoValueType] = _NoValue,
        index_names: Union[Optional[List[Optional[Tuple]]], _NoValueType] = _NoValue,
        index_dtypes: Union[Optional[List[Dtype]], _NoValueType] = _NoValue,
        column_labels: Union[Optional[List[Tuple]], _NoValueType] = _NoValue,
        data_spark_columns: Union[Optional[List[spark.Column]], _NoValueType] = _NoValue,
        data_dtypes: Union[Optional[List[Dtype]], _NoValueType] = _NoValue,
        column_label_names: Union[Optional[List[Optional[Tuple]]], _NoValueType] = _NoValue
    ) -> "InternalFrame":
        """ Copy the immutable InternalFrame.

        :param spark_frame: the new Spark DataFrame. If not specified, the original one is used.
        :param index_spark_columns: the list of Spark Column.
                                    If not specified, the original ones are used.
        :param index_names: the index names. If not specified, the original ones are used.
        :param index_dtypes: the index dtypes. If not specified, the original dtyeps are used.
        :param column_labels: the new column labels. If not specified, the original ones are used.
        :param data_spark_columns: the new Spark Columns.
                                   If not specified, the original ones are used.
        :param data_dtypes: the data dtypes. If not specified, the original dtyeps are used.
        :param column_label_names: the new names of the column index levels.
                                   If not specified, the original ones are used.
        :return: the copied immutable InternalFrame.
        """
        if spark_frame is _NoValue:
            spark_frame = self.spark_frame
        if index_spark_columns is _NoValue:
            index_spark_columns = self.index_spark_columns
        if index_names is _NoValue:
            index_names = self.index_names
        if index_dtypes is _NoValue:
            index_dtypes = self.index_dtypes
        if column_labels is _NoValue:
            column_labels = self.column_labels
        if data_spark_columns is _NoValue:
            data_spark_columns = self.data_spark_columns
        if data_dtypes is _NoValue:
            data_dtypes = self.data_dtypes
        if column_label_names is _NoValue:
            column_label_names = self.column_label_names
        return InternalFrame(
            spark_frame=cast(spark.DataFrame, spark_frame),
            index_spark_columns=cast(List[spark.Column], index_spark_columns),
            index_names=cast(Optional[List[Optional[Tuple]]], index_names),
            index_dtypes=cast(Optional[List[Dtype]], index_dtypes),
            column_labels=cast(Optional[List[Tuple]], column_labels),
            data_spark_columns=cast(Optional[List[spark.Column]], data_spark_columns),
            data_dtypes=cast(Optional[List[Dtype]], data_dtypes),
            column_label_names=cast(Optional[List[Optional[Tuple]]], column_label_names),
        )

    @staticmethod
    def from_pandas(pdf: pd.DataFrame) -> "InternalFrame":
        """ Create an immutable DataFrame from pandas DataFrame.

        :param pdf: :class:`pd.DataFrame`
        :return: the created immutable DataFrame
        """

        index_names = [
            name if name is None or isinstance(name, tuple) else (name,) for name in pdf.index.names
        ]

        columns = pdf.columns
        if isinstance(columns, pd.MultiIndex):
            column_labels = columns.tolist()
        else:
            column_labels = [(col,) for col in columns]
        column_label_names = [
            name if name is None or isinstance(name, tuple) else (name,) for name in columns.names
        ]

        (
            pdf,
            index_columns,
            index_dtypes,
            data_columns,
            data_dtypes,
        ) = InternalFrame.prepare_pandas_frame(pdf)

        schema = StructType(
            [
                StructField(
                    name, infer_pd_series_spark_type(col, dtype), nullable=bool(col.isnull().any()),
                )
                for (name, col), dtype in zip(pdf.iteritems(), index_dtypes + data_dtypes)
            ]
        )

        sdf = default_session().createDataFrame(pdf, schema=schema)
        return InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in index_columns],
            index_names=index_names,
            index_dtypes=index_dtypes,
            column_labels=column_labels,
            data_spark_columns=[scol_for(sdf, col) for col in data_columns],
            data_dtypes=data_dtypes,
            column_label_names=column_label_names,
        )

    @staticmethod
    def prepare_pandas_frame(
        pdf: pd.DataFrame, *, retain_index: bool = True
    ) -> Tuple[pd.DataFrame, List[str], List[Dtype], List[str], List[Dtype]]:
        """
        Prepare pandas DataFrame for creating Spark DataFrame.

        :param pdf: the pandas DataFrame to be prepared.
        :param retain_index: whether the indices should be retained.
        :return: the tuple of
            - the prepared pandas dataFrame
            - index column names for Spark DataFrame
            - index dtypes of the given pandas DataFrame
            - data column names for Spark DataFrame
            - data dtypes of the given pandas DataFrame

        >>> pdf = pd.DataFrame(
        ...    {("x", "a"): ['a', 'b', 'c'],
        ...     ("y", "b"): pd.Categorical(["i", "k", "j"], categories=["i", "j", "k"])},
        ...    index=[10, 20, 30])
        >>> prepared, index_columns, index_dtypes, data_columns, data_dtypes = (
        ...     InternalFrame.prepare_pandas_frame(pdf))
        >>> prepared
           __index_level_0__ (x, a)  (y, b)
        0                 10      a       0
        1                 20      b       2
        2                 30      c       1
        >>> index_columns
        ['__index_level_0__']
        >>> index_dtypes
        [dtype('int64')]
        >>> data_columns
        ['(x, a)', '(y, b)']
        >>> data_dtypes
        [dtype('O'), CategoricalDtype(categories=['i', 'j', 'k'], ordered=False)]
        """
        pdf = pdf.copy()

        data_columns = [name_like_string(col) for col in pdf.columns]
        pdf.columns = data_columns

        if retain_index:
            index_nlevels = pdf.index.nlevels
            index_columns = [SPARK_INDEX_NAME_FORMAT(i) for i in range(index_nlevels)]
            pdf.index.names = index_columns
            reset_index = pdf.reset_index()
        else:
            index_nlevels = 0
            index_columns = []
            reset_index = pdf

        index_dtypes = list(reset_index.dtypes)[:index_nlevels]
        data_dtypes = list(reset_index.dtypes)[index_nlevels:]

        for name, col in reset_index.iteritems():
            dt = col.dtype
            if is_datetime64_dtype(dt) or is_datetime64tz_dtype(dt):
                continue
            elif isinstance(dt, CategoricalDtype):
                col = col.cat.codes
            reset_index[name] = col.replace({np.nan: None})

        return reset_index, index_columns, index_dtypes, data_columns, data_dtypes


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.internal

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.internal.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.internal tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.internal,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
