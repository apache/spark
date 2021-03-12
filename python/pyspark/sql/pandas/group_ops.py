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
import sys
import warnings

from pyspark import since
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame


class PandasGroupedOpsMixin(object):
    """
    Min-in for pandas grouped operations. Currently, only :class:`GroupedData`
    can use this class.
    """

    @since(2.3)
    def apply(self, udf):
        """
        It is an alias of :meth:`pyspark.sql.GroupedData.applyInPandas`; however, it takes a
        :meth:`pyspark.sql.functions.pandas_udf` whereas
        :meth:`pyspark.sql.GroupedData.applyInPandas` takes a Python native function.

        .. note:: It is preferred to use :meth:`pyspark.sql.GroupedData.applyInPandas` over this
            API. This API will be deprecated in the future releases.

        :param udf: a grouped map user-defined function returned by
            :func:`pyspark.sql.functions.pandas_udf`.

        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))
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

        .. seealso:: :meth:`pyspark.sql.functions.pandas_udf`

        """
        # Columns are special because hasattr always return True
        if isinstance(udf, Column) or not hasattr(udf, 'func') \
                or udf.evalType != PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
            raise ValueError("Invalid udf: the udf argument must be a pandas_udf of type "
                             "GROUPED_MAP.")

        warnings.warn(
            "It is preferred to use 'applyInPandas' over this "
            "API. This API will be deprecated in the future releases. See SPARK-28264 for "
            "more details.", UserWarning)

        return self.applyInPandas(udf.func, schema=udf.returnType)

    @since(3.0)
    def applyInPandas(self, func, schema):
        """
        Maps each group of the current :class:`DataFrame` using a pandas udf and returns the result
        as a `DataFrame`.

        The function should take a `pandas.DataFrame` and return another
        `pandas.DataFrame`. For each group, all columns are passed together as a `pandas.DataFrame`
        to the user-function and the returned `pandas.DataFrame` are combined as a
        :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pandas.DataFrame` can be arbitrary.

        :param func: a Python native function that takes a `pandas.DataFrame`, and outputs a
            `pandas.DataFrame`.
        :param schema: the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        >>> import pandas as pd  # doctest: +SKIP
        >>> from pyspark.sql.functions import pandas_udf, ceil
        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def normalize(pdf):
        ...     v = pdf.v
        ...     return pdf.assign(v=(v - v.mean()) / v.std())
        >>> df.groupby("id").applyInPandas(
        ...     normalize, schema="id long, v double").show()  # doctest: +SKIP
        +---+-------------------+
        | id|                  v|
        +---+-------------------+
        |  1|-0.7071067811865475|
        |  1| 0.7071067811865475|
        |  2|-0.8320502943378437|
        |  2|-0.2773500981126146|
        |  2| 1.1094003924504583|
        +---+-------------------+

        Alternatively, the user can pass a function that takes two arguments.
        In this case, the grouping key(s) will be passed as the first argument and the data will
        be passed as the second argument. The grouping key(s) will be passed as a tuple of numpy
        data types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in
        as a `pandas.DataFrame` containing all columns from the original Spark DataFrame.
        This is useful when the user does not want to hardcode grouping key(s) in the function.

        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def mean_func(key, pdf):
        ...     # key is a tuple of one numpy.int64, which is the value
        ...     # of 'id' for the current group
        ...     return pd.DataFrame([key + (pdf.v.mean(),)])
        >>> df.groupby('id').applyInPandas(
        ...     mean_func, schema="id long, v double").show()  # doctest: +SKIP
        +---+---+
        | id|  v|
        +---+---+
        |  1|1.5|
        |  2|6.0|
        +---+---+
        >>> def sum_func(key, pdf):
        ...     # key is a tuple of two numpy.int64s, which is the values
        ...     # of 'id' and 'ceil(df.v / 2)' for the current group
        ...     return pd.DataFrame([key + (pdf.v.sum(),)])
        >>> df.groupby(df.id, ceil(df.v / 2)).applyInPandas(
        ...     sum_func, schema="id long, `ceil(v / 2)` long, v double").show()  # doctest: +SKIP
        +---+-----------+----+
        | id|ceil(v / 2)|   v|
        +---+-----------+----+
        |  2|          5|10.0|
        |  1|          1| 3.0|
        |  2|          3| 5.0|
        |  2|          2| 3.0|
        +---+-----------+----+

        .. note:: This function requires a full shuffle. All the data of a group will be loaded
            into memory, so the user should be aware of the potential OOM risk if data is skewed
            and certain groups are too large to fit in memory.

        .. note:: If returning a new `pandas.DataFrame` constructed with a dictionary, it is
            recommended to explicitly index the columns by name to ensure the positions are correct,
            or alternatively use an `OrderedDict`.
            For example, `pd.DataFrame({'id': ids, 'a': data}, columns=['id', 'a'])` or
            `pd.DataFrame(OrderedDict([('id', ids), ('a', data)]))`.

        .. note:: Experimental

        .. seealso:: :meth:`pyspark.sql.functions.pandas_udf`
        """
        from pyspark.sql import GroupedData
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        assert isinstance(self, GroupedData)

        udf = pandas_udf(
            func, returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])
        jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)

    @since(3.0)
    def cogroup(self, other):
        """
        Cogroups this group with another group so that we can run cogrouped operations.

        See :class:`CoGroupedData` for the operations that can be run.
        """
        from pyspark.sql import GroupedData

        assert isinstance(self, GroupedData)

        return PandasCogroupedOps(self, other)


class PandasCogroupedOps(object):
    """
    A logical grouping of two :class:`GroupedData`,
    created by :func:`GroupedData.cogroup`.

    .. note:: Experimental

    .. versionadded:: 3.0
    """

    def __init__(self, gd1, gd2):
        self._gd1 = gd1
        self._gd2 = gd2
        self.sql_ctx = gd1.sql_ctx

    @since(3.0)
    def applyInPandas(self, func, schema):
        """
        Applies a function to each cogroup using pandas and returns the result
        as a `DataFrame`.

        The function should take two `pandas.DataFrame`\\s and return another
        `pandas.DataFrame`.  For each side of the cogroup, all columns are passed together as a
        `pandas.DataFrame` to the user-function and the returned `pandas.DataFrame` are combined as
        a :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pandas.DataFrame` can be arbitrary.

        :param func: a Python native function that takes two `pandas.DataFrame`\\s, and
            outputs a `pandas.DataFrame`, or that takes one tuple (grouping keys) and two
            pandas ``DataFrame``s, and outputs a pandas ``DataFrame``.
        :param schema: the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        >>> from pyspark.sql.functions import pandas_udf
        >>> df1 = spark.createDataFrame(
        ...     [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
        ...     ("time", "id", "v1"))
        >>> df2 = spark.createDataFrame(
        ...     [(20000101, 1, "x"), (20000101, 2, "y")],
        ...     ("time", "id", "v2"))
        >>> def asof_join(l, r):
        ...     return pd.merge_asof(l, r, on="time", by="id")
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        ...     asof_join, schema="time int, id int, v1 double, v2 string"
        ... ).show()  # doctest: +SKIP
        +--------+---+---+---+
        |    time| id| v1| v2|
        +--------+---+---+---+
        |20000101|  1|1.0|  x|
        |20000102|  1|3.0|  x|
        |20000101|  2|2.0|  y|
        |20000102|  2|4.0|  y|
        +--------+---+---+---+

        Alternatively, the user can define a function that takes three arguments.  In this case,
        the grouping key(s) will be passed as the first argument and the data will be passed as the
        second and third arguments.  The grouping key(s) will be passed as a tuple of numpy data
        types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in as two
        `pandas.DataFrame` containing all columns from the original Spark DataFrames.

        >>> def asof_join(k, l, r):
        ...     if k == (1,):
        ...         return pd.merge_asof(l, r, on="time", by="id")
        ...     else:
        ...         return pd.DataFrame(columns=['time', 'id', 'v1', 'v2'])
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        ...     asof_join, "time int, id int, v1 double, v2 string").show()  # doctest: +SKIP
        +--------+---+---+---+
        |    time| id| v1| v2|
        +--------+---+---+---+
        |20000101|  1|1.0|  x|
        |20000102|  1|3.0|  x|
        +--------+---+---+---+

        .. note:: This function requires a full shuffle. All the data of a cogroup will be loaded
            into memory, so the user should be aware of the potential OOM risk if data is skewed
            and certain groups are too large to fit in memory.

        .. note:: If returning a new `pandas.DataFrame` constructed with a dictionary, it is
            recommended to explicitly index the columns by name to ensure the positions are correct,
            or alternatively use an `OrderedDict`.
            For example, `pd.DataFrame({'id': ids, 'a': data}, columns=['id', 'a'])` or
            `pd.DataFrame(OrderedDict([('id', ids), ('a', data)]))`.

        .. note:: Experimental

        .. seealso:: :meth:`pyspark.sql.functions.pandas_udf`

        """
        from pyspark.sql.pandas.functions import pandas_udf

        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
        all_cols = self._extract_cols(self._gd1) + self._extract_cols(self._gd2)
        udf_column = udf(*all_cols)
        jdf = self._gd1._jgd.flatMapCoGroupsInPandas(self._gd2._jgd, udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)

    @staticmethod
    def _extract_cols(gd):
        df = gd._df
        return [df[col] for col in df.columns]


def _test():
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.group_ops
    globs = pyspark.sql.pandas.group_ops.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.pandas.group tests")\
        .getOrCreate()
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.group_ops, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
