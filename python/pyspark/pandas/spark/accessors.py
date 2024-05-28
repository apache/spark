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
Spark related features. Usually, the features here are missing in pandas
but Spark has it.
"""
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Callable, Generic, List, Optional, Union

from pyspark import StorageLevel
from pyspark.sql import Column as PySparkColumn, DataFrame as PySparkDataFrame
from pyspark.sql.types import DataType, StructType
from pyspark.pandas._typing import IndexOpsLike
from pyspark.pandas.internal import InternalField
from pyspark.sql.utils import get_column_class, get_dataframe_class

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType
    from pyspark._typing import PrimitiveType

    import pyspark.pandas as ps
    from pyspark.pandas.frame import CachedDataFrame


class SparkIndexOpsMethods(Generic[IndexOpsLike], metaclass=ABCMeta):
    """Spark related features. Usually, the features here are missing in pandas
    but Spark has it."""

    def __init__(self, data: IndexOpsLike):
        self._data = data

    @property
    def data_type(self) -> DataType:
        """Returns the data type as defined by Spark, as a Spark DataType object."""
        return self._data._internal.spark_type_for(self._data._column_label)

    @property
    def nullable(self) -> bool:
        """Returns the nullability as defined by Spark."""
        return self._data._internal.spark_column_nullable_for(self._data._column_label)

    @property
    def column(self) -> PySparkColumn:
        """
        Spark Column object representing the Series/Index.

        .. note:: This Spark Column object is strictly stick to its base DataFrame the Series/Index
            was derived from.
        """
        return self._data._internal.spark_column_for(self._data._column_label)

    def transform(self, func: Callable[[PySparkColumn], PySparkColumn]) -> IndexOpsLike:
        """
        Applies a function that takes and returns a Spark column. It allows natively
        applying a Spark function and column APIs with the Spark column internally used
        in Series or Index. The output length of the Spark column should be the same as input's.

        .. note:: It requires to have the same input and output length; therefore,
            the aggregate Spark functions such as count does not work.

        Parameters
        ----------
        func : function
            Function to use for transforming the data by using Spark columns.

        Returns
        -------
        Series or Index

        Raises
        ------
        ValueError : If the output from the function is not a Spark column.

        Examples
        --------
        >>> from pyspark.sql.functions import log
        >>> df = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"])
        >>> df
           a  b
        0  1  4
        1  2  5
        2  3  6

        >>> df.a.spark.transform(lambda c: log(c))
        0    0.000000
        1    0.693147
        2    1.098612
        Name: a, dtype: float64

        >>> df.index.spark.transform(lambda c: c + 10)
        Index([10, 11, 12], dtype='int64')

        >>> df.a.spark.transform(lambda c: c + df.b.spark.column)
        0    5
        1    7
        2    9
        Name: a, dtype: int64
        """
        from pyspark.pandas import MultiIndex

        if isinstance(self._data, MultiIndex):
            raise NotImplementedError("MultiIndex does not support spark.transform yet.")
        output = func(self._data.spark.column)
        Column = get_column_class()
        if not isinstance(output, Column):
            raise ValueError(
                "The output of the function [%s] should be of a "
                "pyspark.sql.Column; however, got [%s]." % (func, type(output))
            )
        # Trigger the resolution so it throws an exception if anything does wrong
        # within the function, for example,
        # `df1.a.spark.transform(lambda _: F.col("non-existent"))`.
        field = InternalField.from_struct_field(
            self._data._internal.spark_frame.select(output).schema.fields[0]
        )
        return self._data._with_new_scol(scol=output, field=field)

    @property
    @abstractmethod
    def analyzed(self) -> IndexOpsLike:
        pass


class SparkSeriesMethods(SparkIndexOpsMethods["ps.Series"]):
    def apply(self, func: Callable[[PySparkColumn], PySparkColumn]) -> "ps.Series":
        """
        Applies a function that takes and returns a Spark column. It allows to natively
        apply a Spark function and column APIs with the Spark column internally used
        in Series or Index.

        .. note:: It forces to lose the index and end up using the default index. It is
            preferred to use :meth:`Series.spark.transform` or `:meth:`DataFrame.spark.apply`
            with specifying the `index_col`.

        .. note:: It does not require to have the same length of the input and output.
            However, it requires to create a new DataFrame internally which will require
            to set `compute.ops_on_diff_frames` to compute even with the same origin
            DataFrame is expensive, whereas :meth:`Series.spark.transform` does not
            require it.

        Parameters
        ----------
        func : function
            Function to apply the function against the data by using Spark columns.

        Returns
        -------
        Series

        Raises
        ------
        ValueError : If the output from the function is not a Spark column.

        Examples
        --------
        >>> from pyspark import pandas as ps
        >>> from pyspark.sql.functions import count, lit
        >>> df = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"])
        >>> df
           a  b
        0  1  4
        1  2  5
        2  3  6

        >>> df.a.spark.apply(lambda c: count(c))
        0    3
        Name: a, dtype: int64

        >>> df.a.spark.apply(lambda c: c + df.b.spark.column)
        0    5
        1    7
        2    9
        Name: a, dtype: int64
        """
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series, first_series
        from pyspark.pandas.internal import HIDDEN_COLUMNS

        output = func(self._data.spark.column)
        Column = get_column_class()
        if not isinstance(output, Column):
            raise ValueError(
                "The output of the function [%s] should be of a "
                "pyspark.sql.Column; however, got [%s]." % (func, type(output))
            )
        assert isinstance(self._data, Series)

        sdf = self._data._internal.spark_frame.drop(*HIDDEN_COLUMNS).select(output)
        # Lose index.
        return first_series(DataFrame(sdf)).rename(self._data.name)

    @property
    def analyzed(self) -> "ps.Series":
        """
        Returns a new Series with the analyzed Spark DataFrame.

        After multiple operations, the underlying Spark plan could grow huge
        and make the Spark planner take a long time to finish the planning.

        This function is for the workaround to avoid it.

        .. note:: After analyzing, operations between the analyzed Series and the original one
            will **NOT** work without setting a config `compute.ops_on_diff_frames` to `True`.

        Returns
        -------
        Series

        Examples
        --------
        >>> ser = ps.Series([1, 2, 3])
        >>> ser
        0    1
        1    2
        2    3
        dtype: int64

        The analyzed one should return the same value.

        >>> ser.spark.analyzed
        0    1
        1    2
        2    3
        dtype: int64

        However, it won't work with the same anchor Series.

        >>> with ps.option_context('compute.ops_on_diff_frames', False):
        ...     ser + ser.spark.analyzed
        Traceback (most recent call last):
        ...
        ValueError: ... enable 'compute.ops_on_diff_frames' option.

        >>> with ps.option_context('compute.ops_on_diff_frames', True):
        ...     (ser + ser.spark.analyzed).sort_index()
        0    2
        1    4
        2    6
        dtype: int64
        """
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import first_series

        return first_series(DataFrame(self._data._internal.resolved_copy))


class SparkIndexMethods(SparkIndexOpsMethods["ps.Index"]):
    @property
    def analyzed(self) -> "ps.Index":
        """
        Returns a new Index with the analyzed Spark DataFrame.

        After multiple operations, the underlying Spark plan could grow huge
        and make the Spark planner take a long time to finish the planning.

        This function is for the workaround to avoid it.

        .. note:: After analyzing, operations between the analyzed Series and the original one
            will **NOT** work without setting a config `compute.ops_on_diff_frames` to `True`.

        Returns
        -------
        Index

        Examples
        --------
        >>> import pyspark.pandas as ps
        >>> idx = ps.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')

        The analyzed one should return the same value.

        >>> idx.spark.analyzed
        Index([1, 2, 3], dtype='int64')

        However, it won't work with the same anchor Index.

        >>> with ps.option_context('compute.ops_on_diff_frames', False):
        ...     idx + idx.spark.analyzed
        Traceback (most recent call last):
        ...
        ValueError: ... enable 'compute.ops_on_diff_frames' option.

        >>> with ps.option_context('compute.ops_on_diff_frames', True):
        ...     (idx + idx.spark.analyzed).sort_values()
        Index([2, 4, 6], dtype='int64')
        """
        from pyspark.pandas.frame import DataFrame

        return DataFrame(self._data._internal.resolved_copy).index


class SparkFrameMethods:
    """Spark related features. Usually, the features here are missing in pandas
    but Spark has it."""

    def __init__(self, frame: "ps.DataFrame"):
        self._psdf = frame

    def schema(self, index_col: Optional[Union[str, List[str]]] = None) -> StructType:
        """
        Returns the underlying Spark schema.

        Returns
        -------
        pyspark.sql.types.StructType
            The underlying Spark schema.

        Parameters
        ----------
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default, the index is always lost.

        Examples
        --------
        >>> df = ps.DataFrame({'a': list('abc'),
        ...                    'b': list(range(1, 4)),
        ...                    'c': np.arange(3, 6).astype('i1'),
        ...                    'd': np.arange(4.0, 7.0, dtype='float64'),
        ...                    'e': [True, False, True],
        ...                    'f': pd.date_range('20130101', periods=3)},
        ...                   columns=['a', 'b', 'c', 'd', 'e', 'f'])
        >>> df.spark.schema().simpleString()
        'struct<a:string,b:bigint,c:tinyint,d:double,e:boolean,f:timestamp>'
        >>> df.spark.schema(index_col='index').simpleString()
        'struct<index:bigint,a:string,b:bigint,c:tinyint,d:double,e:boolean,f:timestamp>'
        """
        return self.frame(index_col).schema

    def print_schema(self, index_col: Optional[Union[str, List[str]]] = None) -> None:
        """
        Prints out the underlying Spark schema in the tree format.

        Parameters
        ----------
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default, the index is always lost.

        Returns
        -------
        None

        Examples
        --------
        >>> df = ps.DataFrame({'a': list('abc'),
        ...                    'b': list(range(1, 4)),
        ...                    'c': np.arange(3, 6).astype('i1'),
        ...                    'd': np.arange(4.0, 7.0, dtype='float64'),
        ...                    'e': [True, False, True],
        ...                    'f': pd.date_range('20130101', periods=3)},
        ...                   columns=['a', 'b', 'c', 'd', 'e', 'f'])
        >>> df.spark.print_schema()  # doctest: +NORMALIZE_WHITESPACE
        root
         |-- a: string (nullable = false)
         |-- b: long (nullable = false)
         |-- c: byte (nullable = false)
         |-- d: double (nullable = false)
         |-- e: boolean (nullable = false)
         |-- f: timestamp (nullable = false)
        >>> df.spark.print_schema(index_col='index')  # doctest: +NORMALIZE_WHITESPACE
        root
         |-- index: long (nullable = false)
         |-- a: string (nullable = false)
         |-- b: long (nullable = false)
         |-- c: byte (nullable = false)
         |-- d: double (nullable = false)
         |-- e: boolean (nullable = false)
         |-- f: timestamp (nullable = false)
        """
        self.frame(index_col).printSchema()

    def frame(self, index_col: Optional[Union[str, List[str]]] = None) -> PySparkDataFrame:
        """
        Return the current DataFrame as a Spark DataFrame.  :meth:`DataFrame.spark.frame` is an
        alias of  :meth:`DataFrame.to_spark`.

        Parameters
        ----------
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default, the index is always lost.

        See Also
        --------
        DataFrame.to_spark
        DataFrame.pandas_api
        DataFrame.spark.frame

        Examples
        --------
        By default, this method loses the index as below.

        >>> df = ps.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
        >>> df.to_spark().show()  # doctest: +NORMALIZE_WHITESPACE
        +---+---+---+
        |  a|  b|  c|
        +---+---+---+
        |  1|  4|  7|
        |  2|  5|  8|
        |  3|  6|  9|
        +---+---+---+

        >>> df = ps.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
        >>> df.spark.frame().show()  # doctest: +NORMALIZE_WHITESPACE
        +---+---+---+
        |  a|  b|  c|
        +---+---+---+
        |  1|  4|  7|
        |  2|  5|  8|
        |  3|  6|  9|
        +---+---+---+

        If `index_col` is set, it keeps the index column as specified.

        >>> df.to_spark(index_col="index").show()  # doctest: +NORMALIZE_WHITESPACE
        +-----+---+---+---+
        |index|  a|  b|  c|
        +-----+---+---+---+
        |    0|  1|  4|  7|
        |    1|  2|  5|  8|
        |    2|  3|  6|  9|
        +-----+---+---+---+

        Keeping an index column is useful when you want to call some Spark APIs and
        convert it back to pandas-on-Spark DataFrame without creating a default index, which
        can affect performance.

        >>> spark_df = df.to_spark(index_col="index")
        >>> spark_df = spark_df.filter("a == 2")
        >>> spark_df.pandas_api(index_col="index")  # doctest: +NORMALIZE_WHITESPACE
               a  b  c
        index
        1      2  5  8

        In case of multi-index, specify a list to `index_col`.

        >>> new_df = df.set_index("a", append=True)
        >>> new_spark_df = new_df.to_spark(index_col=["index_1", "index_2"])
        >>> new_spark_df.show()  # doctest: +NORMALIZE_WHITESPACE
        +-------+-------+---+---+
        |index_1|index_2|  b|  c|
        +-------+-------+---+---+
        |      0|      1|  4|  7|
        |      1|      2|  5|  8|
        |      2|      3|  6|  9|
        +-------+-------+---+---+

        Can be converted back to pandas-on-Spark DataFrame.

        >>> new_spark_df.pandas_api(
        ...     index_col=["index_1", "index_2"])  # doctest: +NORMALIZE_WHITESPACE
                         b  c
        index_1 index_2
        0       1        4  7
        1       2        5  8
        2       3        6  9
        """
        from pyspark.pandas.utils import name_like_string

        psdf = self._psdf

        data_column_names = []
        data_columns = []
        for i, (label, spark_column, column_name) in enumerate(
            zip(
                psdf._internal.column_labels,
                psdf._internal.data_spark_columns,
                psdf._internal.data_spark_column_names,
            )
        ):
            name = str(i) if label is None else name_like_string(label)
            data_column_names.append(name)
            if column_name != name:
                spark_column = spark_column.alias(name)
            data_columns.append(spark_column)

        if index_col is None:
            return psdf._internal.spark_frame.select(data_columns)
        else:
            if isinstance(index_col, str):
                index_col = [index_col]

            old_index_scols = psdf._internal.index_spark_columns

            if len(index_col) != len(old_index_scols):
                raise ValueError(
                    "length of index columns is %s; however, the length of the given "
                    "'index_col' is %s." % (len(old_index_scols), len(index_col))
                )

            if any(col in data_column_names for col in index_col):
                raise ValueError("'index_col' cannot be overlapped with other columns.")

            new_index_scols = [
                index_scol.alias(col) for index_scol, col in zip(old_index_scols, index_col)
            ]
            return psdf._internal.spark_frame.select(new_index_scols + data_columns)

    def cache(self) -> "CachedDataFrame":
        """
        Yields and caches the current DataFrame.

        The pandas-on-Spark DataFrame is yielded as a protected resource and its corresponding
        data is cached which gets uncached after execution goes off the context.

        If you want to specify the StorageLevel manually, use :meth:`DataFrame.spark.persist`

        See Also
        --------
        DataFrame.spark.persist

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df
           dogs  cats
        0   0.2   0.3
        1   0.0   0.6
        2   0.6   0.0
        3   0.2   0.1

        >>> with df.spark.cache() as cached_df:
        ...     print(cached_df.count())
        ...
        dogs    4
        cats    4
        dtype: int64

        >>> df = df.spark.cache()
        >>> df.to_pandas().mean(axis=1)
        0    0.25
        1    0.30
        2    0.30
        3    0.15
        dtype: float64

        To uncache the dataframe, use `unpersist` function

        >>> df.spark.unpersist()
        """
        from pyspark.pandas.frame import CachedDataFrame

        self._psdf._update_internal_frame(
            self._psdf._internal.resolved_copy, check_same_anchor=False
        )
        return CachedDataFrame(self._psdf._internal)

    def persist(
        self, storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ) -> "CachedDataFrame":
        """
        Yields and caches the current DataFrame with a specific StorageLevel.
        If a StorageLevel is not given, the `MEMORY_AND_DISK` level is used by default like PySpark.

        The pandas-on-Spark DataFrame is yielded as a protected resource and its corresponding
        data is cached which gets uncached after execution goes off the context.

        See Also
        --------
        DataFrame.spark.cache

        Examples
        --------
        >>> import pyspark
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df
           dogs  cats
        0   0.2   0.3
        1   0.0   0.6
        2   0.6   0.0
        3   0.2   0.1

        Set the StorageLevel to `MEMORY_ONLY`.

        >>> with df.spark.persist(pyspark.StorageLevel.MEMORY_ONLY) as cached_df:
        ...     print(cached_df.spark.storage_level)
        ...     print(cached_df.count())
        ...
        Memory Serialized 1x Replicated
        dogs    4
        cats    4
        dtype: int64

        Set the StorageLevel to `DISK_ONLY`.

        >>> with df.spark.persist(pyspark.StorageLevel.DISK_ONLY) as cached_df:
        ...     print(cached_df.spark.storage_level)
        ...     print(cached_df.count())
        ...
        Disk Serialized 1x Replicated
        dogs    4
        cats    4
        dtype: int64

        If a StorageLevel is not given, it uses `MEMORY_AND_DISK` by default.

        >>> with df.spark.persist() as cached_df:
        ...     print(cached_df.spark.storage_level)
        ...     print(cached_df.count())
        ...
        Disk Memory Serialized 1x Replicated
        dogs    4
        cats    4
        dtype: int64

        >>> df = df.spark.persist()
        >>> df.to_pandas().mean(axis=1)
        0    0.25
        1    0.30
        2    0.30
        3    0.15
        dtype: float64

        To uncache the dataframe, use `unpersist` function

        >>> df.spark.unpersist()
        """
        from pyspark.pandas.frame import CachedDataFrame

        self._psdf._update_internal_frame(
            self._psdf._internal.resolved_copy, check_same_anchor=False
        )
        return CachedDataFrame(self._psdf._internal, storage_level=storage_level)

    def hint(self, name: str, *parameters: "PrimitiveType") -> "ps.DataFrame":
        """
        Specifies some hint on the current DataFrame.

        Parameters
        ----------
        name : A name of the hint.
        parameters : Optional parameters.

        Returns
        -------
        ret : DataFrame with the hint.

        See Also
        --------
        broadcast : Marks a DataFrame as small enough for use in broadcast joins.

        Examples
        --------
        >>> df1 = ps.DataFrame({'lkey': ['foo', 'bar', 'baz', 'foo'],
        ...                     'value': [1, 2, 3, 5]},
        ...                    columns=['lkey', 'value']).set_index('lkey')
        >>> df2 = ps.DataFrame({'rkey': ['foo', 'bar', 'baz', 'foo'],
        ...                     'value': [5, 6, 7, 8]},
        ...                    columns=['rkey', 'value']).set_index('rkey')
        >>> merged = df1.merge(df2.spark.hint("broadcast"), left_index=True, right_index=True)
        >>> merged.spark.explain()  # doctest: +ELLIPSIS
        == Physical Plan ==
        ...
        ...BroadcastHashJoin...
        ...
        """
        from pyspark.pandas.frame import DataFrame

        internal = self._psdf._internal.resolved_copy
        return DataFrame(internal.with_new_sdf(internal.spark_frame.hint(name, *parameters)))

    def to_table(
        self,
        name: str,
        format: Optional[str] = None,
        mode: str = "overwrite",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Write the DataFrame into a Spark table. :meth:`DataFrame.spark.to_table`
        is an alias of :meth:`DataFrame.to_table`.

        Parameters
        ----------
        name : str, required
            Table name in Spark.
        format : string, optional
            Specifies the output data source format. Some common ones are:

            - 'delta'
            - 'parquet'
            - 'orc'
            - 'json'
            - 'csv'

        mode : str {'append', 'overwrite', 'ignore', 'error', 'errorifexists'}, default
            'overwrite'. Specifies the behavior of the save operation when the table exists
            already.

            - 'append': Append the new data to existing data.
            - 'overwrite': Overwrite existing data.
            - 'ignore': Silently ignore this operation if data already exists.
            - 'error' or 'errorifexists': Throw an exception if data already exists.

        partition_cols : str or list of str, optional, default None
            Names of partitioning columns
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default, the index is always lost.
        options
            Additional options passed directly to Spark.

        Returns
        -------
        None

        See Also
        --------
        read_table
        DataFrame.spark.to_spark_io
        DataFrame.to_parquet

        Examples
        --------
        >>> df = ps.DataFrame(dict(
        ...    date=list(pd.date_range('2012-1-1 12:00:00', periods=3, freq='M')),
        ...    country=['KR', 'US', 'JP'],
        ...    code=[1, 2 ,3]), columns=['date', 'country', 'code'])
        >>> df
                         date country  code
        0 2012-01-31 12:00:00      KR     1
        1 2012-02-29 12:00:00      US     2
        2 2012-03-31 12:00:00      JP     3

        >>> df.to_table('%s.my_table' % db, partition_cols='date')
        """
        if "options" in options and isinstance(options.get("options"), dict) and len(options) == 1:
            options = options.get("options")  # type: ignore[assignment]

        self._psdf.spark.frame(index_col=index_col).write.saveAsTable(
            name=name, format=format, mode=mode, partitionBy=partition_cols, **options
        )

    def to_spark_io(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        mode: str = "overwrite",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """Write the DataFrame out to a Spark data source.

        Parameters
        ----------
        path : string, optional
            Path to the data source.
        format : string, optional
            Specifies the output data source format. Some common ones are:

            - 'delta'
            - 'parquet'
            - 'orc'
            - 'json'
            - 'csv'
        mode : str {'append', 'overwrite', 'ignore', 'error', 'errorifexists'}, default
            'overwrite'. Specifies the behavior of the save operation when data already exists.

            - 'append': Append the new data to existing data.
            - 'overwrite': Overwrite existing data.
            - 'ignore': Silently ignore this operation if data already exists.
            - 'error' or 'errorifexists': Throw an exception if data already exists.
        partition_cols : str or list of str, optional
            Names of partitioning columns
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default, the index is always lost.
        options : dict
            All other options passed directly into Spark's data source.

        Returns
        -------
        None

        See Also
        --------
        read_spark_io
        DataFrame.to_delta
        DataFrame.to_parquet
        DataFrame.to_table
        DataFrame.spark.to_spark_io

        Examples
        --------
        >>> df = ps.DataFrame(dict(
        ...    date=list(pd.date_range('2012-1-1 12:00:00', periods=3, freq='M')),
        ...    country=['KR', 'US', 'JP'],
        ...    code=[1, 2 ,3]), columns=['date', 'country', 'code'])
        >>> df
                         date country  code
        0 2012-01-31 12:00:00      KR     1
        1 2012-02-29 12:00:00      US     2
        2 2012-03-31 12:00:00      JP     3

        >>> df.spark.to_spark_io(path='%s/to_spark_io/foo.json' % path, format='json')
        """
        if "options" in options and isinstance(options.get("options"), dict) and len(options) == 1:
            options = options.get("options")  # type: ignore[assignment]

        self._psdf.spark.frame(index_col=index_col).write.save(
            path=path, format=format, mode=mode, partitionBy=partition_cols, **options
        )

    def explain(self, extended: Optional[bool] = None, mode: Optional[str] = None) -> None:
        """
        Prints the underlying (logical and physical) Spark plans to the console for debugging
        purpose.

        Parameters
        ----------
        extended : boolean, default ``False``.
            If ``False``, prints only the physical plan.
        mode : string, default ``None``.
            The expected output format of plans.

        Returns
        -------
        None

        Examples
        --------
        >>> df = ps.DataFrame({'id': range(10)})
        >>> df.spark.explain()  # doctest: +ELLIPSIS
        == Physical Plan ==
        ...

        >>> df.spark.explain(True)  # doctest: +ELLIPSIS
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...

        >>> df.spark.explain("extended")  # doctest: +ELLIPSIS
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...

        >>> df.spark.explain(mode="extended")  # doctest: +ELLIPSIS
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        """
        self._psdf._internal.to_internal_spark_frame.explain(extended, mode)

    def apply(
        self,
        func: Callable[[PySparkDataFrame], PySparkDataFrame],
        index_col: Optional[Union[str, List[str]]] = None,
    ) -> "ps.DataFrame":
        """
        Applies a function that takes and returns a Spark DataFrame. It allows natively
        apply a Spark function and column APIs with the Spark column internally used
        in Series or Index.

        .. note:: set `index_col` and keep the column named as so in the output Spark
            DataFrame to avoid using the default index to prevent performance penalty.
            If you omit `index_col`, it will use default index which is potentially
            expensive in general.

        .. note:: it will lose column labels. This is a synonym of
            ``func(psdf.to_spark(index_col)).pandas_api(index_col)``.

        Parameters
        ----------
        func : function
            Function to apply the function against the data by using Spark DataFrame.

        Returns
        -------
        DataFrame

        Raises
        ------
        ValueError : If the output from the function is not a Spark DataFrame.

        Examples
        --------
        >>> psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"])
        >>> psdf
           a  b
        0  1  4
        1  2  5
        2  3  6

        >>> psdf.spark.apply(
        ...     lambda sdf: sdf.selectExpr("a + b as c", "index"), index_col="index")
        ... # doctest: +NORMALIZE_WHITESPACE
               c
        index
        0      5
        1      7
        2      9

        The case below ends up with using the default index, which should be avoided
        if possible.

        >>> psdf.spark.apply(lambda sdf: sdf.groupby("a").count().sort("a"))
           a  count
        0  1      1
        1  2      1
        2  3      1
        """
        output = func(self.frame(index_col))
        SparkDataFrame = get_dataframe_class()
        if not isinstance(output, SparkDataFrame):
            raise ValueError(
                "The output of the function [%s] should be of a "
                "pyspark.sql.DataFrame; however, got [%s]." % (func, type(output))
            )
        return output.pandas_api(index_col)

    def repartition(self, num_partitions: int) -> "ps.DataFrame":
        """
        Returns a new DataFrame partitioned by the given partitioning expressions. The
        resulting DataFrame is hash partitioned.

        Parameters
        ----------
        num_partitions : int
            The target number of partitions.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> psdf = ps.DataFrame({"age": [5, 5, 2, 2],
        ...         "name": ["Bob", "Bob", "Alice", "Alice"]}).set_index("age")
        >>> psdf.sort_index()  # doctest: +NORMALIZE_WHITESPACE
              name
        age
        2    Alice
        2    Alice
        5      Bob
        5      Bob
        >>> new_psdf = psdf.spark.repartition(7)
        >>> new_psdf.to_spark().rdd.getNumPartitions()
        7
        >>> new_psdf.sort_index()   # doctest: +NORMALIZE_WHITESPACE
              name
        age
        2    Alice
        2    Alice
        5      Bob
        5      Bob
        """
        from pyspark.pandas.frame import DataFrame

        internal = self._psdf._internal.resolved_copy
        repartitioned_sdf = internal.spark_frame.repartition(num_partitions)
        return DataFrame(internal.with_new_sdf(repartitioned_sdf))

    def coalesce(self, num_partitions: int) -> "ps.DataFrame":
        """
        Returns a new DataFrame that has exactly `num_partitions` partitions.

        .. note:: This operation results in a narrow dependency, e.g. if you go from 1000
            partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new
            partitions will claim 10 of the current partitions. If a larger number of partitions is
            requested, it will stay at the current number of partitions. However, if you're doing a
            drastic coalesce, e.g. to num_partitions = 1, this may result in your computation taking
            place on fewer nodes than you like (e.g. one node in the case of num_partitions = 1). To
            avoid this, you can call repartition(). This will add a shuffle step, but means the
            current upstream partitions will be executed in parallel (per whatever the current
            partitioning is).

        Parameters
        ----------
        num_partitions : int
            The target number of partitions.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> psdf = ps.DataFrame({"age": [5, 5, 2, 2],
        ...         "name": ["Bob", "Bob", "Alice", "Alice"]}).set_index("age")
        >>> psdf.sort_index()  # doctest: +NORMALIZE_WHITESPACE
              name
        age
        2    Alice
        2    Alice
        5      Bob
        5      Bob
        >>> new_psdf = psdf.spark.coalesce(1)
        >>> new_psdf.to_spark().rdd.getNumPartitions()
        1
        >>> new_psdf.sort_index()   # doctest: +NORMALIZE_WHITESPACE
              name
        age
        2    Alice
        2    Alice
        5      Bob
        5      Bob
        """
        from pyspark.pandas.frame import DataFrame

        internal = self._psdf._internal.resolved_copy
        coalesced_sdf = internal.spark_frame.coalesce(num_partitions)
        return DataFrame(internal.with_new_sdf(coalesced_sdf))

    def checkpoint(self, eager: bool = True) -> "ps.DataFrame":
        """Returns a checkpointed version of this DataFrame.

        Checkpointing can be used to truncate the logical plan of this DataFrame, which is
        especially useful in iterative algorithms where the plan may grow exponentially. It will be
        saved to files inside the checkpoint directory set with `SparkContext.setCheckpointDir`.

        Parameters
        ----------
        eager : bool
            Whether to checkpoint this DataFrame immediately

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> psdf = ps.DataFrame({"a": ["a", "b", "c"]})
        >>> psdf
           a
        0  a
        1  b
        2  c
        >>> new_psdf = psdf.spark.checkpoint()  # doctest: +SKIP
        >>> new_psdf  # doctest: +SKIP
           a
        0  a
        1  b
        2  c
        """
        from pyspark.pandas.frame import DataFrame

        internal = self._psdf._internal.resolved_copy
        checkpointed_sdf = internal.spark_frame.checkpoint(eager)
        return DataFrame(internal.with_new_sdf(checkpointed_sdf))

    def local_checkpoint(self, eager: bool = True) -> "ps.DataFrame":
        """Returns a locally checkpointed version of this DataFrame.

        Checkpointing can be used to truncate the logical plan of this DataFrame, which is
        especially useful in iterative algorithms where the plan may grow exponentially. Local
        checkpoints are stored in the executors using the caching subsystem and therefore they are
        not reliable.

        Parameters
        ----------
        eager : bool
            Whether to locally checkpoint this DataFrame immediately

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> psdf = ps.DataFrame({"a": ["a", "b", "c"]})
        >>> psdf
           a
        0  a
        1  b
        2  c
        >>> new_psdf = psdf.spark.local_checkpoint()
        >>> new_psdf
           a
        0  a
        1  b
        2  c
        """
        from pyspark.pandas.frame import DataFrame

        internal = self._psdf._internal.resolved_copy
        checkpointed_sdf = internal.spark_frame.localCheckpoint(eager)
        return DataFrame(internal.with_new_sdf(checkpointed_sdf))

    @property
    def analyzed(self) -> "ps.DataFrame":
        """
        Returns a new DataFrame with the analyzed Spark DataFrame.

        After multiple operations, the underlying Spark plan could grow huge
        and make the Spark planner take a long time to finish the planning.

        This function is for the workaround to avoid it.

        .. note:: After analysis, operations between the analyzed DataFrame and the original one
            will **NOT** work without setting a config `compute.ops_on_diff_frames` to `True`.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"])
        >>> df
           a  b
        0  1  4
        1  2  5
        2  3  6

        The analyzed one should return the same value.

        >>> df.spark.analyzed
           a  b
        0  1  4
        1  2  5
        2  3  6

        However, it won't work with the same anchor Series.

        >>> with ps.option_context('compute.ops_on_diff_frames', False):
        ...     df + df.spark.analyzed
        Traceback (most recent call last):
        ...
        ValueError: ... enable 'compute.ops_on_diff_frames' option.

        >>> with ps.option_context('compute.ops_on_diff_frames', True):
        ...     (df + df.spark.analyzed).sort_index()
           a   b
        0  2   8
        1  4  10
        2  6  12
        """
        from pyspark.pandas.frame import DataFrame

        return DataFrame(self._psdf._internal.resolved_copy)


class CachedSparkFrameMethods(SparkFrameMethods):
    """Spark related features for cached DataFrame. This is usually created via
    `df.spark.cache()`."""

    def __init__(self, frame: "CachedDataFrame"):
        super().__init__(frame)

    @property
    def storage_level(self) -> StorageLevel:
        """
        Return the storage level of this cache.

        Examples
        --------
        >>> import pyspark.pandas as ps
        >>> import pyspark
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df
           dogs  cats
        0   0.2   0.3
        1   0.0   0.6
        2   0.6   0.0
        3   0.2   0.1

        >>> with df.spark.cache() as cached_df:
        ...     print(cached_df.spark.storage_level)
        ...
        Disk Memory Deserialized 1x Replicated

        Set the StorageLevel to `MEMORY_ONLY`.

        >>> with df.spark.persist(pyspark.StorageLevel.MEMORY_ONLY) as cached_df:
        ...     print(cached_df.spark.storage_level)
        ...
        Memory Serialized 1x Replicated
        """
        return self._psdf._cached.storageLevel

    def unpersist(self) -> None:
        """
        The `unpersist` function is used to uncache the pandas-on-Spark DataFrame when it
        is not used with the `with` statement.

        Returns
        -------
        None

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df = df.spark.cache()

        To uncache the dataframe, use `unpersist` function

        >>> df.spark.unpersist()
        """
        if self._psdf._cached.is_cached:
            self._psdf._cached.unpersist()


def _test() -> None:
    import os
    import doctest
    import shutil
    import sys
    import tempfile
    import uuid
    import numpy
    import pandas
    from pyspark.sql import SparkSession
    import pyspark.pandas.spark.accessors

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.spark.accessors.__dict__.copy()
    globs["np"] = numpy
    globs["pd"] = pandas
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.spark.accessors tests")
        .getOrCreate()
    )

    db_name = "db%s" % str(uuid.uuid4()).replace("-", "")
    spark.sql("CREATE DATABASE %s" % db_name)
    globs["db"] = db_name

    path = tempfile.mkdtemp()
    globs["path"] = path

    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.spark.accessors,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )

    shutil.rmtree(path, ignore_errors=True)
    spark.sql("DROP DATABASE IF EXISTS %s CASCADE" % db_name)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
