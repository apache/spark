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

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix, PythonEvalType
from pyspark.sql.column import Column, _to_seq, _to_java_column, _create_column_from_literal
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.udf import UserDefinedFunction
from pyspark.sql.types import *

__all__ = ["GroupedData"]


def dfapi(f):
    def _api(self):
        name = f.__name__
        jdf = getattr(self._jgd, name)()
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


def df_varargs_api(f):
    def _api(self, *cols):
        name = f.__name__
        jdf = getattr(self._jgd, name)(_to_seq(self.sql_ctx._sc, cols))
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class GroupedData(object):
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.

    .. note:: Experimental

    .. versionadded:: 1.3
    """

    def __init__(self, jgd, df):
        self._jgd = jgd
        self._df = df
        self.sql_ctx = df.sql_ctx

    @ignore_unicode_prefix
    @since(1.3)
    def agg(self, *exprs):
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        :param exprs: a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        >>> gdf = df.groupBy(df.name)
        >>> sorted(gdf.agg({"*": "count"}).collect())
        [Row(name=u'Alice', count(1)=1), Row(name=u'Bob', count(1)=1)]

        >>> from pyspark.sql import functions as F
        >>> sorted(gdf.agg(F.min(df.age)).collect())
        [Row(name=u'Alice', min(age)=2), Row(name=u'Bob', min(age)=5)]
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            jdf = self._jgd.agg(exprs[0])
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            jdf = self._jgd.agg(exprs[0]._jc,
                                _to_seq(self.sql_ctx._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(jdf, self.sql_ctx)

    @dfapi
    @since(1.3)
    def count(self):
        """Counts the number of records for each group.

        >>> sorted(df.groupBy(df.age).count().collect())
        [Row(age=2, count=1), Row(age=5, count=1)]
        """

    @df_varargs_api
    @since(1.3)
    def mean(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().mean('age').collect()
        [Row(avg(age)=3.5)]
        >>> df3.groupBy().mean('age', 'height').collect()
        [Row(avg(age)=3.5, avg(height)=82.5)]
        """

    @df_varargs_api
    @since(1.3)
    def avg(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().avg('age').collect()
        [Row(avg(age)=3.5)]
        >>> df3.groupBy().avg('age', 'height').collect()
        [Row(avg(age)=3.5, avg(height)=82.5)]
        """

    @df_varargs_api
    @since(1.3)
    def max(self, *cols):
        """Computes the max value for each numeric columns for each group.

        >>> df.groupBy().max('age').collect()
        [Row(max(age)=5)]
        >>> df3.groupBy().max('age', 'height').collect()
        [Row(max(age)=5, max(height)=85)]
        """

    @df_varargs_api
    @since(1.3)
    def min(self, *cols):
        """Computes the min value for each numeric column for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().min('age').collect()
        [Row(min(age)=2)]
        >>> df3.groupBy().min('age', 'height').collect()
        [Row(min(age)=2, min(height)=80)]
        """

    @df_varargs_api
    @since(1.3)
    def sum(self, *cols):
        """Compute the sum for each numeric columns for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().sum('age').collect()
        [Row(sum(age)=7)]
        >>> df3.groupBy().sum('age', 'height').collect()
        [Row(sum(age)=7, sum(height)=165)]
        """

    @since(1.6)
    def pivot(self, pivot_col, values=None):
        """
        Pivots a column of the current :class:`DataFrame` and perform the specified aggregation.
        There are two versions of pivot function: one that requires the caller to specify the list
        of distinct values to pivot on, and one that does not. The latter is more concise but less
        efficient, because Spark needs to first compute the list of distinct values internally.

        :param pivot_col: Name of the column to pivot.
        :param values: List of values that will be translated to columns in the output DataFrame.

        # Compute the sum of earnings for each year by course with each course as a separate column

        >>> df4.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings").collect()
        [Row(year=2012, dotNET=15000, Java=20000), Row(year=2013, dotNET=48000, Java=30000)]

        # Or without specifying column values (less efficient)

        >>> df4.groupBy("year").pivot("course").sum("earnings").collect()
        [Row(year=2012, Java=20000, dotNET=15000), Row(year=2013, Java=30000, dotNET=48000)]
        """
        if values is None:
            jgd = self._jgd.pivot(pivot_col)
        else:
            jgd = self._jgd.pivot(pivot_col, values)
        return GroupedData(jgd, self._df)

    @since(2.3)
    def apply(self, udf):
        """
        Maps each group of the current :class:`DataFrame` using a pandas udf and returns the result
        as a `DataFrame`.

        The user-defined function should take a `pandas.DataFrame` and return another
        `pandas.DataFrame`. For each group, all columns are passed together as a `pandas.DataFrame`
        to the user-function and the returned `pandas.DataFrame`s are combined as a
        :class:`DataFrame`.
        The returned `pandas.DataFrame` can be of arbitrary length and its schema must match the
        returnType of the pandas udf.

        This function does not support partial aggregation, and requires shuffling all the data in
        the :class:`DataFrame`.

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
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])
        jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)


def _test():
    import doctest
    from pyspark.sql import Row, SparkSession
    import pyspark.sql.group
    globs = pyspark.sql.group.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.group tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))
    globs['df3'] = sc.parallelize([Row(name='Alice', age=2, height=80),
                                   Row(name='Bob', age=5, height=85)]).toDF()
    globs['df4'] = sc.parallelize([Row(course="dotNET", year=2012, earnings=10000),
                                   Row(course="Java",   year=2012, earnings=20000),
                                   Row(course="dotNET", year=2012, earnings=5000),
                                   Row(course="dotNET", year=2013, earnings=48000),
                                   Row(course="Java",   year=2013, earnings=30000)]).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.group, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
