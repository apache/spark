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
pandas-on-Spark specific features.
"""
import inspect
from typing import Any, Callable, Optional, Tuple, Union, TYPE_CHECKING, cast, List
from types import FunctionType

import numpy as np  # noqa: F401
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DataType, LongType, StructField, StructType
from pyspark.pandas._typing import DataFrameOrSeries, Name
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    SPARK_INDEX_NAME_FORMAT,
    SPARK_DEFAULT_SERIES_NAME,
    SPARK_INDEX_NAME_PATTERN,
)
from pyspark.pandas.typedef import infer_return_type, DataFrameType, ScalarType, SeriesType
from pyspark.pandas.utils import (
    is_name_like_value,
    is_name_like_tuple,
    name_like_string,
    scol_for,
    verify_temp_column_name,
    log_advice,
)

if TYPE_CHECKING:
    from pyspark.pandas.frame import DataFrame
    from pyspark.pandas.series import Series
    from pyspark.sql._typing import UserDefinedFunctionLike


class PandasOnSparkFrameMethods:
    """pandas-on-Spark specific features for DataFrame."""

    def __init__(self, frame: "DataFrame"):
        self._psdf = frame

    def attach_id_column(self, id_type: str, column: Name) -> "DataFrame":
        """
        Attach a column to be used as an identifier of rows similar to the default index.

        See also `Default Index type
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/options.html#default-index-type>`_.

        Parameters
        ----------
        id_type : string
            The id type.

            - 'sequence' : a sequence that increases one by one.

              .. note:: this uses Spark's Window without specifying partition specification.
                  This leads to moving all data into a single partition in a single machine and
                  could cause serious performance degradation.
                  Avoid this method with very large datasets.

            - 'distributed-sequence' : a sequence that increases one by one,
              by group-by and group-map approach in a distributed manner.
            - 'distributed' : a monotonically increasing sequence simply by using PySpark’s
              monotonically_increasing_id function in a fully distributed manner.

        column : string or tuple of string
            The column name.

        Returns
        -------
        DataFrame
            The DataFrame attached the column.

        Examples
        --------
        >>> df = ps.DataFrame({"x": ['a', 'b', 'c']})
        >>> df.pandas_on_spark.attach_id_column(id_type="sequence", column="id")
           x  id
        0  a   0
        1  b   1
        2  c   2

        >>> df.pandas_on_spark.attach_id_column(id_type="distributed-sequence", column=0)
           x  0
        0  a  0
        1  b  1
        2  c  2

        >>> df.pandas_on_spark.attach_id_column(id_type="distributed", column=0.0)
        ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
           x  0.0
        0  a  ...
        1  b  ...
        2  c  ...

        For multi-index columns:

        >>> df = ps.DataFrame({("x", "y"): ['a', 'b', 'c']})
        >>> df.pandas_on_spark.attach_id_column(id_type="sequence", column=("id-x", "id-y"))
           x id-x
           y id-y
        0  a    0
        1  b    1
        2  c    2

        >>> df.pandas_on_spark.attach_id_column(id_type="distributed-sequence", column=(0, 1.0))
           x   0
           y 1.0
        0  a   0
        1  b   1
        2  c   2
        """
        from pyspark.pandas.frame import DataFrame

        if id_type == "sequence":
            attach_func = InternalFrame.attach_sequence_column
        elif id_type == "distributed-sequence":
            attach_func = InternalFrame.attach_distributed_sequence_column
        elif id_type == "distributed":
            attach_func = InternalFrame.attach_distributed_column
        else:
            raise ValueError(
                "id_type should be one of 'sequence', 'distributed-sequence' and 'distributed'"
            )

        assert is_name_like_value(column, allow_none=False), column
        if not is_name_like_tuple(column):
            column = (column,)

        internal = self._psdf._internal

        if len(column) != internal.column_labels_level:
            raise ValueError(
                "The given column `{}` must be the same length as the existing columns.".format(
                    column
                )
            )
        elif column in internal.column_labels:
            raise ValueError(
                "The given column `{}` already exists.".format(name_like_string(column))
            )

        # Make sure the underlying Spark column names are the form of
        # `name_like_string(column_label)`.
        sdf = internal.spark_frame.select(
            [
                scol.alias(SPARK_INDEX_NAME_FORMAT(i))
                for i, scol in enumerate(internal.index_spark_columns)
            ]
            + [
                scol.alias(name_like_string(label))
                for scol, label in zip(internal.data_spark_columns, internal.column_labels)
            ]
        )
        sdf = attach_func(sdf, name_like_string(column))

        return DataFrame(
            InternalFrame(
                spark_frame=sdf,
                index_spark_columns=[
                    scol_for(sdf, SPARK_INDEX_NAME_FORMAT(i)) for i in range(internal.index_level)
                ],
                index_names=internal.index_names,
                index_fields=internal.index_fields,
                column_labels=internal.column_labels + [column],
                data_spark_columns=(
                    [scol_for(sdf, name_like_string(label)) for label in internal.column_labels]
                    + [scol_for(sdf, name_like_string(column))]
                ),
                data_fields=internal.data_fields
                + [
                    InternalField.from_struct_field(
                        StructField(name_like_string(column), LongType(), nullable=False)
                    )
                ],
                column_label_names=internal.column_label_names,
            ).resolved_copy
        )

    def apply_batch(
        self, func: Callable[..., pd.DataFrame], args: Tuple = (), **kwds: Any
    ) -> "DataFrame":
        """
        Apply a function that takes pandas DataFrame and outputs pandas DataFrame. The pandas
        DataFrame given to the function is of a batch used internally.

        See also `Transform and apply a function
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/transform_apply.html>`_.

        .. note:: the `func` is unable to access the whole input frame. pandas-on-Spark
            internally splits the input series into multiple batches and calls `func` with each
            batch multiple times. Therefore, operations such as global aggregations are impossible.
            See the example below.

            >>> # This case does not return the length of whole frame but of the batch internally
            ... # used.
            ... def length(pdf) -> ps.DataFrame[int, [int]]:
            ...     return pd.DataFrame([len(pdf)])
            ...
            >>> df = ps.DataFrame({'A': range(1000)})
            >>> df.pandas_on_spark.apply_batch(length)  # doctest: +SKIP
                c0
            0   83
            1   83
            2   83
            ...
            10  83
            11  83

        .. note:: this API executes the function once to infer the type which is
            potentially expensive, for instance, when the dataset is created after
            aggregations or sorting.

            To avoid this, specify return type in ``func``, for instance, as below:

            >>> def plus_one(x) -> ps.DataFrame[int, [float, float]]:
            ...     return x + 1

            If the return type is specified, the output column names become
            `c0, c1, c2 ... cn`. These names are positionally mapped to the returned
            DataFrame in ``func``.

            To specify the column names, you can assign them in a NumPy compound type style
            as below:

            >>> def plus_one(x) -> ps.DataFrame[("index", int), [("a", float), ("b", float)]]:
            ...     return x + 1

            >>> pdf = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5]})
            >>> def plus_one(x) -> ps.DataFrame[
            ...         (pdf.index.name, pdf.index.dtype), zip(pdf.dtypes, pdf.columns)]:
            ...     return x + 1

        Parameters
        ----------
        func : function
            Function to apply to each pandas frame.
        args : tuple
            Positional arguments to pass to `func` in addition to the
            array/series.
        **kwds
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        DataFrame

        See Also
        --------
        DataFrame.apply: For row/columnwise operations.
        DataFrame.applymap: For elementwise operations.
        DataFrame.aggregate: Only perform aggregating type operations.
        DataFrame.transform: Only perform transforming type operations.
        Series.pandas_on_spark.transform_batch: transform the search as each pandas chunks.

        Examples
        --------
        >>> df = ps.DataFrame([(1, 2), (3, 4), (5, 6)], columns=['A', 'B'])
        >>> df
           A  B
        0  1  2
        1  3  4
        2  5  6

        >>> def query_func(pdf) -> ps.DataFrame[int, [int, int]]:
        ...     return pdf.query('A == 1')
        >>> df.pandas_on_spark.apply_batch(query_func)
           c0  c1
        0   1   2

        >>> def query_func(pdf) -> ps.DataFrame[("idx", int), [("A", int), ("B", int)]]:
        ...     return pdf.query('A == 1')
        >>> df.pandas_on_spark.apply_batch(query_func)  # doctest: +NORMALIZE_WHITESPACE
             A  B
        idx
        0    1  2

        You can also omit the type hints so pandas-on-Spark infers the return schema as below:

        >>> df.pandas_on_spark.apply_batch(lambda pdf: pdf.query('A == 1'))
           A  B
        0  1  2

        You can also specify extra arguments.

        >>> def calculation(pdf, y, z) -> ps.DataFrame[int, [int, int]]:
        ...     return pdf ** y + z
        >>> df.pandas_on_spark.apply_batch(calculation, args=(10,), z=20)
                c0        c1
        0       21      1044
        1    59069   1048596
        2  9765645  60466196

        You can also use ``np.ufunc`` and built-in functions as input.

        >>> df.pandas_on_spark.apply_batch(np.add, args=(10,))
            A   B
        0  11  12
        1  13  14
        2  15  16

        >>> (df * -1).pandas_on_spark.apply_batch(abs)
           A  B
        0  1  2
        1  3  4
        2  5  6

        """
        # TODO: codes here partially duplicate `DataFrame.apply`. Can we deduplicate?

        from pyspark.pandas.groupby import GroupBy
        from pyspark.pandas.frame import DataFrame
        from pyspark import pandas as ps

        if not isinstance(func, FunctionType):
            assert callable(func), "the first argument should be a callable function."
            f = func
            # Note that the return type hint specified here affects actual return
            # type in Spark (e.g., infer_return_type). And, MyPy does not allow
            # redefinition of a function.
            func = lambda *args, **kwargs: f(*args, **kwargs)  # noqa: E731

        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)
        should_infer_schema = return_sig is None

        original_func = func

        def new_func(o: Any) -> pd.DataFrame:
            return original_func(o, *args, **kwds)

        self_applied: DataFrame = DataFrame(self._psdf._internal.resolved_copy)

        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            log_advice(
                "If the type hints is not specified for `apply_batch`, "
                "it is expensive to infer the data type internally."
            )
            limit = ps.get_option("compute.shortcut_limit")
            pdf = self_applied.head(limit + 1)._to_internal_pandas()
            applied = new_func(pdf)
            if not isinstance(applied, pd.DataFrame):
                raise ValueError(
                    "The given function should return a frame; however, "
                    "the return type was %s." % type(applied)
                )
            psdf: DataFrame = DataFrame(applied)
            if len(pdf) <= limit:
                return psdf

            index_fields = [field.normalize_spark_type() for field in psdf._internal.index_fields]
            data_fields = [field.normalize_spark_type() for field in psdf._internal.data_fields]

            return_schema = StructType([field.struct_field for field in index_fields + data_fields])

            output_func = GroupBy._make_pandas_df_builder_func(
                self_applied, new_func, return_schema, retain_index=True
            )
            sdf = self_applied._internal.spark_frame.mapInPandas(
                lambda iterator: map(output_func, iterator), schema=return_schema
            )

            # If schema is inferred, we can restore indexes too.
            internal = psdf._internal.with_new_sdf(
                spark_frame=sdf, index_fields=index_fields, data_fields=data_fields
            )
        else:
            return_type = infer_return_type(original_func)
            is_return_dataframe = isinstance(return_type, DataFrameType)
            if not is_return_dataframe:
                raise TypeError(
                    "The given function should specify a frame as its type "
                    "hints; however, the return type was %s." % return_sig
                )
            index_fields = cast(DataFrameType, return_type).index_fields
            should_retain_index = len(index_fields) > 0
            return_schema = cast(DataFrameType, return_type).spark_type

            output_func = GroupBy._make_pandas_df_builder_func(
                self_applied, new_func, return_schema, retain_index=should_retain_index
            )
            sdf = self_applied._internal.to_internal_spark_frame.mapInPandas(
                lambda iterator: map(output_func, iterator), schema=return_schema
            )

            index_spark_columns = None
            index_names: Optional[List[Optional[Tuple[Any, ...]]]] = None

            if should_retain_index:
                index_spark_columns = [
                    scol_for(sdf, index_field.struct_field.name) for index_field in index_fields
                ]

                if not any(
                    [
                        SPARK_INDEX_NAME_PATTERN.match(index_field.struct_field.name)
                        for index_field in index_fields
                    ]
                ):
                    index_names = [(index_field.struct_field.name,) for index_field in index_fields]
            internal = InternalFrame(
                spark_frame=sdf,
                index_names=index_names,
                index_spark_columns=index_spark_columns,
                index_fields=index_fields,
                data_fields=cast(DataFrameType, return_type).data_fields,
            )
        return DataFrame(internal)

    def transform_batch(
        self, func: Callable[..., Union[pd.DataFrame, pd.Series]], *args: Any, **kwargs: Any
    ) -> DataFrameOrSeries:
        """
        Transform chunks with a function that takes pandas DataFrame and outputs pandas DataFrame.
        The pandas DataFrame given to the function is of a batch used internally. The length of
        each input and output should be the same.

        See also `Transform and apply a function
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/transform_apply.html>`_.

        .. note:: the `func` is unable to access the whole input frame. pandas-on-Spark
            internally splits the input series into multiple batches and calls `func` with each
            batch multiple times. Therefore, operations such as global aggregations are impossible.
            See the example below.

            >>> # This case does not return the length of whole frame but of the batch internally
            ... # used.
            ... def length(pdf) -> ps.DataFrame[int]:
            ...     return pd.DataFrame([len(pdf)] * len(pdf))
            ...
            >>> df = ps.DataFrame({'A': range(1000)})
            >>> df.pandas_on_spark.transform_batch(length)  # doctest: +SKIP
                c0
            0   83
            1   83
            2   83
            ...

        .. note:: this API executes the function once to infer the type which is
            potentially expensive, for instance, when the dataset is created after
            aggregations or sorting.

            To avoid this, specify return type in ``func``, for instance, as below:

            >>> def plus_one(x) -> ps.DataFrame[int, [float, float]]:
            ...     return x + 1

            If the return type is specified, the output column names become
            `c0, c1, c2 ... cn`. These names are positionally mapped to the returned
            DataFrame in ``func``.

            To specify the column names, you can assign them in a NumPy compound type style
            as below:

            >>> def plus_one(x) -> ps.DataFrame[("index", int), [("a", float), ("b", float)]]:
            ...     return x + 1

            >>> pdf = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5]})
            >>> def plus_one(x) -> ps.DataFrame[
            ...         (pdf.index.name, pdf.index.dtype), zip(pdf.dtypes, pdf.columns)]:
            ...     return x + 1

        Parameters
        ----------
        func : function
            Function to transform each pandas frame.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        DataFrame or Series

        See Also
        --------
        DataFrame.pandas_on_spark.apply_batch: For row/columnwise operations.
        Series.pandas_on_spark.transform_batch: transform the search as each pandas chunks.

        Examples
        --------
        >>> df = ps.DataFrame([(1, 2), (3, 4), (5, 6)], columns=['A', 'B'])
        >>> df
           A  B
        0  1  2
        1  3  4
        2  5  6

        >>> def plus_one_func(pdf) -> ps.DataFrame[int, [int, int]]:
        ...     return pdf + 1
        >>> df.pandas_on_spark.transform_batch(plus_one_func)
           c0  c1
        0   2   3
        1   4   5
        2   6   7

        >>> def plus_one_func(pdf) -> ps.DataFrame[("index", int), [('A', int), ('B', int)]]:
        ...     return pdf + 1
        >>> df.pandas_on_spark.transform_batch(plus_one_func)  # doctest: +NORMALIZE_WHITESPACE
               A  B
        index
        0      2  3
        1      4  5
        2      6  7

        >>> def plus_one_func(pdf) -> ps.Series[int]:
        ...     return pdf.B + 1
        >>> df.pandas_on_spark.transform_batch(plus_one_func)
        0    3
        1    5
        2    7
        dtype: int64

        You can also omit the type hints so pandas-on-Spark infers the return schema as below:

        >>> df.pandas_on_spark.transform_batch(lambda pdf: pdf + 1)
           A  B
        0  2  3
        1  4  5
        2  6  7

        >>> (df * -1).pandas_on_spark.transform_batch(abs)
           A  B
        0  1  2
        1  3  4
        2  5  6

        Note that you should not transform the index. The index information will not change.

        >>> df.pandas_on_spark.transform_batch(lambda pdf: pdf.B + 1)
        0    3
        1    5
        2    7
        Name: B, dtype: int64

        You can also specify extra arguments as below.

        >>> df.pandas_on_spark.transform_batch(lambda pdf, a, b, c: pdf.B + a + b + c, 1, 2, c=3)
        0     8
        1    10
        2    12
        Name: B, dtype: int64
        """
        from pyspark.pandas.groupby import GroupBy
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import first_series
        from pyspark import pandas as ps

        assert callable(func), "the first argument should be a callable function."
        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)
        should_infer_schema = return_sig is None
        should_retain_index = should_infer_schema
        original_func = func

        def new_func(o: Any) -> Union[pd.DataFrame, pd.Series]:
            return original_func(o, *args, **kwargs)

        def apply_func(pdf: pd.DataFrame) -> pd.DataFrame:
            return new_func(pdf).to_frame()

        def pandas_series_func(
            f: Callable[[pd.DataFrame], pd.DataFrame], return_type: DataType
        ) -> "UserDefinedFunctionLike":
            ff = f

            @pandas_udf(returnType=return_type)  # type: ignore[call-overload]
            def udf(pdf: pd.DataFrame) -> pd.Series:
                return first_series(ff(pdf))

            return udf

        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            log_advice(
                "If the type hints is not specified for `transform_batch`, "
                "it is expensive to infer the data type internally."
            )
            limit = ps.get_option("compute.shortcut_limit")
            pdf = self._psdf.head(limit + 1)._to_internal_pandas()
            transformed = new_func(pdf)
            if not isinstance(transformed, (pd.DataFrame, pd.Series)):
                raise ValueError(
                    "The given function should return a frame; however, "
                    "the return type was %s." % type(transformed)
                )
            if len(transformed) != len(pdf):
                raise ValueError("transform_batch cannot produce aggregated results")
            psdf_or_psser = ps.from_pandas(transformed)

            if isinstance(psdf_or_psser, ps.Series):
                psser = psdf_or_psser

                field = psser._internal.data_fields[0].normalize_spark_type()

                return_schema = StructType([field.struct_field])
                output_func = GroupBy._make_pandas_df_builder_func(
                    self._psdf, apply_func, return_schema, retain_index=False
                )

                pudf = pandas_series_func(output_func, return_type=field.spark_type)
                columns = self._psdf._internal.spark_columns
                # TODO: Index will be lost in this case.
                internal = self._psdf._internal.copy(
                    column_labels=psser._internal.column_labels,
                    data_spark_columns=[pudf(F.struct(*columns)).alias(field.name)],
                    data_fields=[field],
                    column_label_names=psser._internal.column_label_names,
                )
                return first_series(DataFrame(internal))
            else:
                psdf = cast(DataFrame, psdf_or_psser)
                if len(pdf) <= limit:
                    # only do the short cut when it returns a frame to avoid
                    # operations on different dataframes in case of series.
                    return psdf

                index_fields = [
                    field.normalize_spark_type() for field in psdf._internal.index_fields
                ]
                data_fields = [field.normalize_spark_type() for field in psdf._internal.data_fields]

                return_schema = StructType(
                    [field.struct_field for field in index_fields + data_fields]
                )

                self_applied: DataFrame = DataFrame(self._psdf._internal.resolved_copy)

                output_func = GroupBy._make_pandas_df_builder_func(
                    self_applied,
                    new_func,  # type: ignore[arg-type]
                    return_schema,
                    retain_index=True,
                )
                columns = self_applied._internal.spark_columns

                pudf = pandas_udf(  # type: ignore[call-overload]
                    output_func, returnType=return_schema
                )
                temp_struct_column = verify_temp_column_name(
                    self_applied._internal.spark_frame, "__temp_struct__"
                )
                applied = pudf(F.struct(*columns)).alias(temp_struct_column)
                sdf = self_applied._internal.spark_frame.select(applied)
                sdf = sdf.selectExpr("%s.*" % temp_struct_column)

                return DataFrame(
                    psdf._internal.with_new_sdf(
                        spark_frame=sdf, index_fields=index_fields, data_fields=data_fields
                    )
                )
        else:
            return_type = infer_return_type(original_func)
            is_return_series = isinstance(return_type, SeriesType)
            is_return_dataframe = isinstance(return_type, DataFrameType)
            if not is_return_dataframe and not is_return_series:
                raise TypeError(
                    "The given function should specify a frame or series as its type "
                    "hints; however, the return type was %s." % return_sig
                )
            if is_return_series:
                field = InternalField(
                    dtype=cast(SeriesType, return_type).dtype,
                    struct_field=StructField(
                        name=SPARK_DEFAULT_SERIES_NAME,
                        dataType=cast(SeriesType, return_type).spark_type,
                    ),
                ).normalize_spark_type()

                return_schema = StructType([field.struct_field])
                output_func = GroupBy._make_pandas_df_builder_func(
                    self._psdf, apply_func, return_schema, retain_index=False
                )

                pudf = pandas_series_func(output_func, return_type=field.spark_type)
                columns = self._psdf._internal.spark_columns
                internal = self._psdf._internal.copy(
                    column_labels=[None],
                    data_spark_columns=[pudf(F.struct(*columns)).alias(field.name)],
                    data_fields=[field],
                    column_label_names=None,
                )
                return first_series(DataFrame(internal))
            else:
                index_fields = cast(DataFrameType, return_type).index_fields
                index_fields = [index_field.normalize_spark_type() for index_field in index_fields]
                data_fields = [
                    field.normalize_spark_type()
                    for field in cast(DataFrameType, return_type).data_fields
                ]
                normalized_fields = index_fields + data_fields
                return_schema = StructType([field.struct_field for field in normalized_fields])
                should_retain_index = len(index_fields) > 0

                self_applied = DataFrame(self._psdf._internal.resolved_copy)

                output_func = GroupBy._make_pandas_df_builder_func(
                    self_applied,
                    new_func,  # type: ignore[arg-type]
                    return_schema,
                    retain_index=should_retain_index,
                )
                columns = self_applied._internal.spark_columns

                pudf = pandas_udf(  # type: ignore[call-overload]
                    output_func, returnType=return_schema
                )
                temp_struct_column = verify_temp_column_name(
                    self_applied._internal.spark_frame, "__temp_struct__"
                )
                applied = pudf(F.struct(*columns)).alias(temp_struct_column)
                sdf = self_applied._internal.spark_frame.select(applied)
                sdf = sdf.selectExpr("%s.*" % temp_struct_column)

                index_spark_columns = None
                index_names: Optional[List[Optional[Tuple[Any, ...]]]] = None

                if should_retain_index:
                    index_spark_columns = [
                        scol_for(sdf, index_field.struct_field.name) for index_field in index_fields
                    ]

                    if not any(
                        [
                            SPARK_INDEX_NAME_PATTERN.match(index_field.struct_field.name)
                            for index_field in index_fields
                        ]
                    ):
                        index_names = [
                            (index_field.struct_field.name,) for index_field in index_fields
                        ]
                internal = InternalFrame(
                    spark_frame=sdf,
                    index_names=index_names,
                    index_spark_columns=index_spark_columns,
                    index_fields=index_fields,
                    data_fields=data_fields,
                )
                return DataFrame(internal)


class PandasOnSparkSeriesMethods:
    """pandas-on-Spark specific features for Series."""

    def __init__(self, series: "Series"):
        self._psser = series

    def transform_batch(
        self, func: Callable[..., pd.Series], *args: Any, **kwargs: Any
    ) -> "Series":
        """
        Transform the data with the function that takes pandas Series and outputs pandas Series.
        The pandas Series given to the function is of a batch used internally.

        See also `Transform and apply a function
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/transform_apply.html>`_.

        .. note:: the `func` is unable to access the whole input series. pandas-on-Spark
            internally splits the input series into multiple batches and calls `func` with each
            batch multiple times. Therefore, operations such as global aggregations are impossible.
            See the example below.

            >>> # This case does not return the length of whole frame but of the batch internally
            ... # used.
            ... def length(pser) -> ps.Series[int]:
            ...     return pd.Series([len(pser)] * len(pser))
            ...
            >>> df = ps.DataFrame({'A': range(1000)})
            >>> df.A.pandas_on_spark.transform_batch(length)  # doctest: +SKIP
                c0
            0   83
            1   83
            2   83
            ...

        .. note:: this API executes the function once to infer the type which is
            potentially expensive, for instance, when the dataset is created after
            aggregations or sorting.

            To avoid this, specify return type in ``func``, for instance, as below:

            >>> def plus_one(x) -> ps.Series[int]:
            ...     return x + 1

        Parameters
        ----------
        func : function
            Function to apply to each pandas frame.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        DataFrame

        See Also
        --------
        DataFrame.pandas_on_spark.apply_batch : Similar but it takes pandas DataFrame as its
            internal batch.

        Examples
        --------
        >>> df = ps.DataFrame([(1, 2), (3, 4), (5, 6)], columns=['A', 'B'])
        >>> df
           A  B
        0  1  2
        1  3  4
        2  5  6

        >>> def plus_one_func(pser) -> ps.Series[np.int64]:
        ...     return pser + 1
        >>> df.A.pandas_on_spark.transform_batch(plus_one_func)
        0    2
        1    4
        2    6
        Name: A, dtype: int64

        You can also omit the type hints so pandas-on-Spark infers the return schema as below:

        >>> df.A.pandas_on_spark.transform_batch(lambda pser: pser + 1)
        0    2
        1    4
        2    6
        Name: A, dtype: int64

        You can also specify extra arguments.

        >>> def plus_one_func(pser, a, b, c=3) -> ps.Series[np.int64]:
        ...     return pser + a + b + c
        >>> df.A.pandas_on_spark.transform_batch(plus_one_func, 1, b=2)
        0     7
        1     9
        2    11
        Name: A, dtype: int64

        You can also use ``np.ufunc`` and built-in functions as input.

        >>> df.A.pandas_on_spark.transform_batch(np.add, 10)
        0    11
        1    13
        2    15
        Name: A, dtype: int64

        >>> (df * -1).A.pandas_on_spark.transform_batch(abs)
        0    1
        1    3
        2    5
        Name: A, dtype: int64
        """
        assert callable(func), "the first argument should be a callable function."

        return_sig = None
        try:
            spec = inspect.getfullargspec(func)
            return_sig = spec.annotations.get("return", None)
        except TypeError:
            # Falls back to schema inference if it fails to get signature.
            pass

        return_type = None
        if return_sig is not None:
            # Extract the signature arguments from this function.
            sig_return = infer_return_type(func)
            if not isinstance(sig_return, SeriesType):
                raise ValueError(
                    "Expected the return type of this function to be of type column,"
                    " but found type {}".format(sig_return)
                )
            return_type = sig_return

        return self._transform_batch(lambda c: func(c, *args, **kwargs), return_type)

    def _transform_batch(
        self, func: Callable[..., pd.Series], return_type: Optional[Union[SeriesType, ScalarType]]
    ) -> "Series":
        from pyspark.pandas.groupby import GroupBy
        from pyspark.pandas.series import Series, first_series
        from pyspark import pandas as ps

        if not isinstance(func, FunctionType):
            f = func
            # Note that the return type hint specified here affects actual return
            # type in Spark (e.g., infer_return_type). And, MyPy does not allow
            # redefinition of a function.
            func = lambda *args, **kwargs: f(*args, **kwargs)  # noqa: E731

        if return_type is None:
            # TODO: In this case, it avoids the shortcut for now (but only infers schema)
            #  because it returns a series from a different DataFrame and it has a different
            #  anchor. We should fix this to allow the shortcut or only allow to infer
            #  schema.
            limit = ps.get_option("compute.shortcut_limit")
            pser = self._psser.head(limit + 1)._to_internal_pandas()
            transformed = pser.transform(func)
            psser: Series = Series(transformed)

            field = psser._internal.data_fields[0].normalize_spark_type()
        else:
            spark_return_type = return_type.spark_type
            dtype = return_type.dtype
            field = InternalField(
                dtype=dtype,
                struct_field=StructField(
                    name=self._psser._internal.data_spark_column_names[0],
                    dataType=spark_return_type,
                ),
            )

        psdf = self._psser.to_frame()
        columns = psdf._internal.spark_column_names

        def pandas_concat(*series: pd.Series) -> pd.DataFrame:
            # The input can only be a DataFrame for struct from Spark 3.0.
            # This works around making the input as a frame. See SPARK-27240
            pdf = pd.concat(series, axis=1)
            pdf.columns = columns
            return pdf

        def apply_func(pdf: pd.DataFrame) -> pd.DataFrame:
            return func(first_series(pdf)).to_frame()

        return_schema = StructType([StructField(SPARK_DEFAULT_SERIES_NAME, field.spark_type)])
        output_func = GroupBy._make_pandas_df_builder_func(
            psdf, apply_func, return_schema, retain_index=False
        )

        @pandas_udf(returnType=field.spark_type)  # type: ignore[call-overload]
        def pudf(*series: pd.Series) -> pd.Series:
            return first_series(output_func(pandas_concat(*series)))

        return self._psser._with_new_scol(
            scol=pudf(*psdf._internal.spark_columns).alias(field.name), field=field
        )


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.accessors

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.accessors.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.accessors tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.accessors,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
