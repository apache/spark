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

import functools
import sys
import warnings

from pyspark import since
from pyspark.rdd import PythonEvalType
from pyspark.sql.pandas.typehints import infer_eval_type
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.sql.types import DataType
from pyspark.sql.udf import _create_udf
from pyspark.util import _get_argspec


class PandasUDFType(object):
    """Pandas UDF Types. See :meth:`pyspark.sql.functions.pandas_udf`.
    """
    SCALAR = PythonEvalType.SQL_SCALAR_PANDAS_UDF

    SCALAR_ITER = PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF

    GROUPED_MAP = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF

    GROUPED_AGG = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF


@since(2.3)
def pandas_udf(f=None, returnType=None, functionType=None):
    """
    Creates a vectorized user defined function (UDF).

    :param f: user-defined function. A python function if used as a standalone function
    :param returnType: the return type of the user-defined function. The value can be either a
        :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
    :param functionType: an enum value in :class:`pyspark.sql.functions.PandasUDFType`.
                         Default: SCALAR.

    .. seealso:: :meth:`pyspark.sql.DataFrame.mapInPandas`
    .. seealso:: :meth:`pyspark.sql.GroupedData.applyInPandas`
    .. seealso:: :meth:`pyspark.sql.PandasCogroupedOps.applyInPandas`

    The function type of the UDF can be one of the following:

    1. SCALAR

       A scalar UDF defines a transformation: One or more `pandas.Series` -> A `pandas.Series`.
       The length of the returned `pandas.Series` must be of the same as the input `pandas.Series`.
       If the return type is :class:`StructType`, the returned value should be a `pandas.DataFrame`.

       :class:`MapType`, nested :class:`StructType` are currently not supported as output types.

       Scalar UDFs can be used with :meth:`pyspark.sql.DataFrame.withColumn` and
       :meth:`pyspark.sql.DataFrame.select`.

       >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
       >>> from pyspark.sql.types import IntegerType, StringType
       >>> slen = pandas_udf(lambda s: s.str.len(), IntegerType())  # doctest: +SKIP
       >>> @pandas_udf(StringType())  # doctest: +SKIP
       ... def to_upper(s):
       ...     return s.str.upper()
       ...
       >>> @pandas_udf("integer", PandasUDFType.SCALAR)  # doctest: +SKIP
       ... def add_one(x):
       ...     return x + 1
       ...
       >>> df = spark.createDataFrame([(1, "John Doe", 21)],
       ...                            ("id", "name", "age"))  # doctest: +SKIP
       >>> df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")) \\
       ...     .show()  # doctest: +SKIP
       +----------+--------------+------------+
       |slen(name)|to_upper(name)|add_one(age)|
       +----------+--------------+------------+
       |         8|      JOHN DOE|          22|
       +----------+--------------+------------+
       >>> @pandas_udf("first string, last string")  # doctest: +SKIP
       ... def split_expand(n):
       ...     return n.str.split(expand=True)
       >>> df.select(split_expand("name")).show()  # doctest: +SKIP
       +------------------+
       |split_expand(name)|
       +------------------+
       |       [John, Doe]|
       +------------------+

       .. note:: The length of `pandas.Series` within a scalar UDF is not that of the whole input
           column, but is the length of an internal batch used for each call to the function.
           Therefore, this can be used, for example, to ensure the length of each returned
           `pandas.Series`, and can not be used as the column length.

    2. SCALAR_ITER

       A scalar iterator UDF is semantically the same as the scalar Pandas UDF above except that the
       wrapped Python function takes an iterator of batches as input instead of a single batch and,
       instead of returning a single output batch, it yields output batches or explicitly returns an
       generator or an iterator of output batches.
       It is useful when the UDF execution requires initializing some state, e.g., loading a machine
       learning model file to apply inference to every input batch.

       .. note:: It is not guaranteed that one invocation of a scalar iterator UDF will process all
           batches from one partition, although it is currently implemented this way.
           Your code shall not rely on this behavior because it might change in the future for
           further optimization, e.g., one invocation processes multiple partitions.

       Scalar iterator UDFs are used with :meth:`pyspark.sql.DataFrame.withColumn` and
       :meth:`pyspark.sql.DataFrame.select`.

       >>> import pandas as pd  # doctest: +SKIP
       >>> from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType
       >>> pdf = pd.DataFrame([1, 2, 3], columns=["x"])  # doctest: +SKIP
       >>> df = spark.createDataFrame(pdf)  # doctest: +SKIP

       When the UDF is called with a single column that is not `StructType`, the input to the
       underlying function is an iterator of `pd.Series`.

       >>> @pandas_udf("long", PandasUDFType.SCALAR_ITER)  # doctest: +SKIP
       ... def plus_one(batch_iter):
       ...     for x in batch_iter:
       ...         yield x + 1
       ...
       >>> df.select(plus_one(col("x"))).show()  # doctest: +SKIP
       +-----------+
       |plus_one(x)|
       +-----------+
       |          2|
       |          3|
       |          4|
       +-----------+

       When the UDF is called with more than one columns, the input to the underlying function is an
       iterator of `pd.Series` tuple.

       >>> @pandas_udf("long", PandasUDFType.SCALAR_ITER)  # doctest: +SKIP
       ... def multiply_two_cols(batch_iter):
       ...     for a, b in batch_iter:
       ...         yield a * b
       ...
       >>> df.select(multiply_two_cols(col("x"), col("x"))).show()  # doctest: +SKIP
       +-----------------------+
       |multiply_two_cols(x, x)|
       +-----------------------+
       |                      1|
       |                      4|
       |                      9|
       +-----------------------+

       When the UDF is called with a single column that is `StructType`, the input to the underlying
       function is an iterator of `pd.DataFrame`.

       >>> @pandas_udf("long", PandasUDFType.SCALAR_ITER)  # doctest: +SKIP
       ... def multiply_two_nested_cols(pdf_iter):
       ...    for pdf in pdf_iter:
       ...        yield pdf["a"] * pdf["b"]
       ...
       >>> df.select(
       ...     multiply_two_nested_cols(
       ...         struct(col("x").alias("a"), col("x").alias("b"))
       ...     ).alias("y")
       ... ).show()  # doctest: +SKIP
       +---+
       |  y|
       +---+
       |  1|
       |  4|
       |  9|
       +---+

       In the UDF, you can initialize some states before processing batches, wrap your code with
       `try ... finally ...` or use context managers to ensure the release of resources at the end
       or in case of early termination.

       >>> y_bc = spark.sparkContext.broadcast(1)  # doctest: +SKIP
       >>> @pandas_udf("long", PandasUDFType.SCALAR_ITER)  # doctest: +SKIP
       ... def plus_y(batch_iter):
       ...     y = y_bc.value  # initialize some state
       ...     try:
       ...         for x in batch_iter:
       ...             yield x + y
       ...     finally:
       ...         pass  # release resources here, if any
       ...
       >>> df.select(plus_y(col("x"))).show()  # doctest: +SKIP
       +---------+
       |plus_y(x)|
       +---------+
       |        2|
       |        3|
       |        4|
       +---------+

    3. GROUPED_MAP

       A grouped map UDF defines transformation: A `pandas.DataFrame` -> A `pandas.DataFrame`
       The returnType should be a :class:`StructType` describing the schema of the returned
       `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
       the field names in the defined returnType schema if specified as strings, or match the
       field data types by position if not strings, e.g. integer indices.
       The length of the returned `pandas.DataFrame` can be arbitrary.

       Grouped map UDFs are used with :meth:`pyspark.sql.GroupedData.apply`.

       >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))  # doctest: +SKIP
       >>> @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       ... def normalize(pdf):
       ...     v = pdf.v
       ...     return pdf.assign(v=(v - v.mean()) / v.std())
       >>> df.groupby("id").apply(normalize).show()  # doctest: +SKIP
       +---+-------------------+
       | id|                  v|
       +---+-------------------+
       |  1|-0.7071067811865475|
       |  1| 0.7071067811865475|
       |  2|-0.8320502943378437|
       |  2|-0.2773500981126146|
       |  2| 1.1094003924504583|
       +---+-------------------+

       Alternatively, the user can define a function that takes two arguments.
       In this case, the grouping key(s) will be passed as the first argument and the data will
       be passed as the second argument. The grouping key(s) will be passed as a tuple of numpy
       data types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in
       as a `pandas.DataFrame` containing all columns from the original Spark DataFrame.
       This is useful when the user does not want to hardcode grouping key(s) in the function.

       >>> import pandas as pd  # doctest: +SKIP
       >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))  # doctest: +SKIP
       >>> @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       ... def mean_udf(key, pdf):
       ...     # key is a tuple of one numpy.int64, which is the value
       ...     # of 'id' for the current group
       ...     return pd.DataFrame([key + (pdf.v.mean(),)])
       >>> df.groupby('id').apply(mean_udf).show()  # doctest: +SKIP
       +---+---+
       | id|  v|
       +---+---+
       |  1|1.5|
       |  2|6.0|
       +---+---+
       >>> @pandas_udf(
       ...    "id long, `ceil(v / 2)` long, v double",
       ...    PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       >>> def sum_udf(key, pdf):
       ...     # key is a tuple of two numpy.int64s, which is the values
       ...     # of 'id' and 'ceil(df.v / 2)' for the current group
       ...     return pd.DataFrame([key + (pdf.v.sum(),)])
       >>> df.groupby(df.id, ceil(df.v / 2)).apply(sum_udf).show()  # doctest: +SKIP
       +---+-----------+----+
       | id|ceil(v / 2)|   v|
       +---+-----------+----+
       |  2|          5|10.0|
       |  1|          1| 3.0|
       |  2|          3| 5.0|
       |  2|          2| 3.0|
       +---+-----------+----+

       .. note:: If returning a new `pandas.DataFrame` constructed with a dictionary, it is
           recommended to explicitly index the columns by name to ensure the positions are correct,
           or alternatively use an `OrderedDict`.
           For example, `pd.DataFrame({'id': ids, 'a': data}, columns=['id', 'a'])` or
           `pd.DataFrame(OrderedDict([('id', ids), ('a', data)]))`.

       .. seealso:: :meth:`pyspark.sql.GroupedData.apply`

    4. GROUPED_AGG

       A grouped aggregate UDF defines a transformation: One or more `pandas.Series` -> A scalar
       The `returnType` should be a primitive data type, e.g., :class:`DoubleType`.
       The returned scalar can be either a python primitive type, e.g., `int` or `float`
       or a numpy data type, e.g., `numpy.int64` or `numpy.float64`.

       :class:`MapType` and :class:`StructType` are currently not supported as output types.

       Group aggregate UDFs are used with :meth:`pyspark.sql.GroupedData.agg` and
       :class:`pyspark.sql.Window`

       This example shows using grouped aggregated UDFs with groupby:

       >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))
       >>> @pandas_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
       ... def mean_udf(v):
       ...     return v.mean()
       >>> df.groupby("id").agg(mean_udf(df['v'])).show()  # doctest: +SKIP
       +---+-----------+
       | id|mean_udf(v)|
       +---+-----------+
       |  1|        1.5|
       |  2|        6.0|
       +---+-----------+

       This example shows using grouped aggregated UDFs as window functions.

       >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
       >>> from pyspark.sql import Window
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))
       >>> @pandas_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
       ... def mean_udf(v):
       ...     return v.mean()
       >>> w = (Window.partitionBy('id')
       ...            .orderBy('v')
       ...            .rowsBetween(-1, 0))
       >>> df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()  # doctest: +SKIP
       +---+----+------+
       | id|   v|mean_v|
       +---+----+------+
       |  1| 1.0|   1.0|
       |  1| 2.0|   1.5|
       |  2| 3.0|   3.0|
       |  2| 5.0|   4.0|
       |  2|10.0|   7.5|
       +---+----+------+

       .. note:: For performance reasons, the input series to window functions are not copied.
            Therefore, mutating the input series is not allowed and will cause incorrect results.
            For the same reason, users should also not rely on the index of the input series.

       .. seealso:: :meth:`pyspark.sql.GroupedData.agg` and :class:`pyspark.sql.Window`

    .. note:: The user-defined functions do not support conditional expressions or short circuiting
        in boolean expressions and it ends up with being executed all internally. If the functions
        can fail on special rows, the workaround is to incorporate the condition into the functions.

    .. note:: The user-defined functions do not take keyword arguments on the calling side.

    .. note:: The data type of returned `pandas.Series` from the user-defined functions should be
        matched with defined returnType (see :meth:`types.to_arrow_type` and
        :meth:`types.from_arrow_type`). When there is mismatch between them, Spark might do
        conversion on returned data. The conversion is not guaranteed to be correct and results
        should be checked for accuracy by users.
    """

    # The following table shows most of Pandas data and SQL type conversions in Pandas UDFs that
    # are not yet visible to the user. Some of behaviors are buggy and might be changed in the near
    # future. The table might have to be eventually documented externally.
    # Please see SPARK-28132's PR to see the codes in order to generate the table below.
    #
    # +-----------------------------+----------------------+------------------+------------------+------------------+--------------------+--------------------+------------------+------------------+------------------+------------------+--------------+--------------+--------------+-----------------------------------+-----------------------------------------------------+-----------------+--------------------+-----------------------------+--------------+-----------------+------------------+-----------+--------------------------------+  # noqa
    # |SQL Type \ Pandas Value(Type)|None(object(NoneType))|        True(bool)|           1(int8)|          1(int16)|            1(int32)|            1(int64)|          1(uint8)|         1(uint16)|         1(uint32)|         1(uint64)|  1.0(float16)|  1.0(float32)|  1.0(float64)|1970-01-01 00:00:00(datetime64[ns])|1970-01-01 00:00:00-05:00(datetime64[ns, US/Eastern])|a(object(string))|  1(object(Decimal))|[1 2 3](object(array[int32]))| 1.0(float128)|(1+0j)(complex64)|(1+0j)(complex128)|A(category)|1 days 00:00:00(timedelta64[ns])|  # noqa
    # +-----------------------------+----------------------+------------------+------------------+------------------+--------------------+--------------------+------------------+------------------+------------------+------------------+--------------+--------------+--------------+-----------------------------------+-----------------------------------------------------+-----------------+--------------------+-----------------------------+--------------+-----------------+------------------+-----------+--------------------------------+  # noqa
    # |                      boolean|                  None|              True|              True|              True|                True|                True|              True|              True|              True|              True|          True|          True|          True|                                  X|                                                    X|                X|                   X|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                      tinyint|                  None|                 1|                 1|                 1|                   1|                   1|                 1|                 1|                 1|                 1|             1|             1|             1|                                  X|                                                    X|                X|                   1|                            X|             X|                X|                 X|          0|                               X|  # noqa
    # |                     smallint|                  None|                 1|                 1|                 1|                   1|                   1|                 1|                 1|                 1|                 1|             1|             1|             1|                                  X|                                                    X|                X|                   1|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                          int|                  None|                 1|                 1|                 1|                   1|                   1|                 1|                 1|                 1|                 1|             1|             1|             1|                                  X|                                                    X|                X|                   1|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                       bigint|                  None|                 1|                 1|                 1|                   1|                   1|                 1|                 1|                 1|                 1|             1|             1|             1|                                  0|                                       18000000000000|                X|                   1|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                        float|                  None|               1.0|               1.0|               1.0|                 1.0|                 1.0|               1.0|               1.0|               1.0|               1.0|           1.0|           1.0|           1.0|                                  X|                                                    X|                X|                   X|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                       double|                  None|               1.0|               1.0|               1.0|                 1.0|                 1.0|               1.0|               1.0|               1.0|               1.0|           1.0|           1.0|           1.0|                                  X|                                                    X|                X|                   X|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                         date|                  None|                 X|                 X|                 X|datetime.date(197...|                   X|                 X|                 X|                 X|                 X|             X|             X|             X|               datetime.date(197...|                                 datetime.date(197...|                X|datetime.date(197...|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                    timestamp|                  None|                 X|                 X|                 X|                   X|datetime.datetime...|                 X|                 X|                 X|                 X|             X|             X|             X|               datetime.datetime...|                                 datetime.datetime...|                X|datetime.datetime...|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                       string|                  None|                ''|                ''|                ''|              '\x01'|              '\x01'|                ''|                ''|            '\x01'|            '\x01'|            ''|            ''|            ''|                                  X|                                                    X|              'a'|                   X|                            X|            ''|                X|                ''|          X|                               X|  # noqa
    # |                decimal(10,0)|                  None|                 X|                 X|                 X|                   X|                   X|                 X|                 X|                 X|                 X|             X|             X|             X|                                  X|                                                    X|                X|        Decimal('1')|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                   array<int>|                  None|                 X|                 X|                 X|                   X|                   X|                 X|                 X|                 X|                 X|             X|             X|             X|                                  X|                                                    X|                X|                   X|                    [1, 2, 3]|             X|                X|                 X|          X|                               X|  # noqa
    # |              map<string,int>|                     X|                 X|                 X|                 X|                   X|                   X|                 X|                 X|                 X|                 X|             X|             X|             X|                                  X|                                                    X|                X|                   X|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |               struct<_1:int>|                     X|                 X|                 X|                 X|                   X|                   X|                 X|                 X|                 X|                 X|             X|             X|             X|                                  X|                                                    X|                X|                   X|                            X|             X|                X|                 X|          X|                               X|  # noqa
    # |                       binary|                  None|bytearray(b'\x01')|bytearray(b'\x01')|bytearray(b'\x01')|  bytearray(b'\x01')|  bytearray(b'\x01')|bytearray(b'\x01')|bytearray(b'\x01')|bytearray(b'\x01')|bytearray(b'\x01')|bytearray(b'')|bytearray(b'')|bytearray(b'')|                     bytearray(b'')|                                       bytearray(b'')|  bytearray(b'a')|                   X|                            X|bytearray(b'')|   bytearray(b'')|    bytearray(b'')|          X|                  bytearray(b'')|  # noqa
    # +-----------------------------+----------------------+------------------+------------------+------------------+--------------------+--------------------+------------------+------------------+------------------+------------------+--------------+--------------+--------------+-----------------------------------+-----------------------------------------------------+-----------------+--------------------+-----------------------------+--------------+-----------------+------------------+-----------+--------------------------------+  # noqa
    #
    # Note: DDL formatted string is used for 'SQL Type' for simplicity. This string can be
    #       used in `returnType`.
    # Note: The values inside of the table are generated by `repr`.
    # Note: Python 3.7.3, Pandas 0.24.2 and PyArrow 0.13.0 are used.
    # Note: Timezone is KST.
    # Note: 'X' means it throws an exception during the conversion.
    require_minimum_pandas_version()
    require_minimum_pyarrow_version()

    # decorator @pandas_udf(returnType, functionType)
    is_decorator = f is None or isinstance(f, (str, DataType))

    if is_decorator:
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType

        if functionType is not None:
            # @pandas_udf(dataType, functionType=functionType)
            # @pandas_udf(returnType=dataType, functionType=functionType)
            eval_type = functionType
        elif returnType is not None and isinstance(returnType, int):
            # @pandas_udf(dataType, functionType)
            eval_type = returnType
        else:
            # @pandas_udf(dataType) or @pandas_udf(returnType=dataType)
            eval_type = None
    else:
        return_type = returnType

        if functionType is not None:
            eval_type = functionType
        else:
            eval_type = None

    if return_type is None:
        raise ValueError("Invalid return type: returnType can not be None")

    if eval_type not in [PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                         PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
                         PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                         PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
                         PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
                         PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
                         None]:  # None means it should infer the type from type hints.

        raise ValueError("Invalid function type: "
                         "functionType must be one the values from PandasUDFType")

    if is_decorator:
        return functools.partial(_create_pandas_udf, returnType=return_type, evalType=eval_type)
    else:
        return _create_pandas_udf(f=f, returnType=return_type, evalType=eval_type)


def _create_pandas_udf(f, returnType, evalType):
    argspec = _get_argspec(f)

    # pandas UDF by type hints.
    if sys.version_info >= (3, 6):
        from inspect import signature

        if evalType in [PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
                        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF]:
            warnings.warn(
                "In Python 3.6+ and Spark 3.0+, it is preferred to specify type hints for "
                "pandas UDF instead of specifying pandas UDF type which will be deprecated "
                "in the future releases. See SPARK-28264 for more details.", UserWarning)
        elif len(argspec.annotations) > 0:
            evalType = infer_eval_type(signature(f))
            assert evalType is not None

    if evalType is None:
        # Set default is scalar UDF.
        evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF

    if (evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF or
            evalType == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF) and \
            len(argspec.args) == 0 and \
            argspec.varargs is None:
        raise ValueError(
            "Invalid function: 0-arg pandas_udfs are not supported. "
            "Instead, create a 1-arg pandas_udf and ignore the arg in your function."
        )

    if evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF \
            and len(argspec.args) not in (1, 2):
        raise ValueError(
            "Invalid function: pandas_udf with function type GROUPED_MAP or "
            "the function in groupby.applyInPandas "
            "must take either one argument (data) or two arguments (key, data).")

    if evalType == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF \
            and len(argspec.args) not in (2, 3):
        raise ValueError(
            "Invalid function: the function in cogroup.applyInPandas "
            "must take either two arguments (left, right) "
            "or three arguments (key, left, right).")

    return _create_udf(f, returnType, evalType)


def _test():
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.functions
    globs = pyspark.sql.pandas.functions.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.pandas.functions tests")\
        .getOrCreate()
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.functions, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
