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

from pyspark.sql.column import Column, _to_seq
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.group_ops import PandasGroupedOpsMixin
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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


class GroupedData(PandasGroupedOpsMixin):
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.

    .. versionadded:: 1.3
    """

    def __init__(self, jgd, df):
        self._jgd = jgd
        self._df = df
        self.sql_ctx = df.sql_ctx

    def agg(self, *exprs):
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be:

        1. built-in aggregation functions, such as `avg`, `max`, `min`, `sum`, `count`

        2. group aggregate pandas UDFs, created with :func:`pyspark.sql.functions.pandas_udf`

           .. note:: There is no partial aggregation with group aggregate UDFs, i.e.,
               a full shuffle is required. Also, all the data of a group will be loaded into
               memory, so the user should be aware of the potential OOM risk if data is skewed
               and certain groups are too large to fit in memory.

           .. seealso:: :func:`pyspark.sql.functions.pandas_udf`

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        exprs : dict
            a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        Notes
        -----
        Built-in aggregation functions and group aggregate pandas UDFs cannot be mixed
        in a single call to this function.

        Examples
        --------
        >>> gdf = df.groupBy(df.name)
        >>> sorted(gdf.agg({"*": "count"}).collect())
        [Row(name='Alice', count(1)=1), Row(name='Bob', count(1)=1)]

        >>> from pyspark.sql import functions as F
        >>> sorted(gdf.agg(F.min(df.age)).collect())
        [Row(name='Alice', min(age)=2), Row(name='Bob', min(age)=5)]

        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> @pandas_udf('int', PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
        ... def min_udf(v):
        ...     return v.min()
        >>> sorted(gdf.agg(min_udf(df.age)).collect())  # doctest: +SKIP
        [Row(name='Alice', min_udf(age)=2), Row(name='Bob', min_udf(age)=5)]
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
    def count(self):
        """Counts the number of records for each group.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> sorted(df.groupBy(df.age).count().collect())
        [Row(age=2, count=1), Row(age=5, count=1)]
        """

    @df_varargs_api
    def mean(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df.groupBy().mean('age').collect()
        [Row(avg(age)=3.5)]
        >>> df3.groupBy().mean('age', 'height').collect()
        [Row(avg(age)=3.5, avg(height)=82.5)]
        """

    @df_varargs_api
    def avg(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df.groupBy().avg('age').collect()
        [Row(avg(age)=3.5)]
        >>> df3.groupBy().avg('age', 'height').collect()
        [Row(avg(age)=3.5, avg(height)=82.5)]
        """

    @df_varargs_api
    def max(self, *cols):
        """Computes the max value for each numeric columns for each group.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.groupBy().max('age').collect()
        [Row(max(age)=5)]
        >>> df3.groupBy().max('age', 'height').collect()
        [Row(max(age)=5, max(height)=85)]
        """

    @df_varargs_api
    def min(self, *cols):
        """Computes the min value for each numeric column for each group.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df.groupBy().min('age').collect()
        [Row(min(age)=2)]
        >>> df3.groupBy().min('age', 'height').collect()
        [Row(min(age)=2, min(height)=80)]
        """

    @df_varargs_api
    def sum(self, *cols):
        """Computes the sum for each numeric columns for each group.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df.groupBy().sum('age').collect()
        [Row(sum(age)=7)]
        >>> df3.groupBy().sum('age', 'height').collect()
        [Row(sum(age)=7, sum(height)=165)]
        """

    def pivot(self, pivot_col, values=None):
        """
        Pivots a column of the current :class:`DataFrame` and perform the specified aggregation.
        There are two versions of pivot function: one that requires the caller to specify the list
        of distinct values to pivot on, and one that does not. The latter is more concise but less
        efficient, because Spark needs to first compute the list of distinct values internally.

        .. versionadded:: 1.6.0

        Parameters
        ----------
        pivot_col : str
            Name of the column to pivot.
        values : list, optional
            List of values that will be translated to columns in the output DataFrame.

        Examples
        --------
        # Compute the sum of earnings for each year by course with each course as a separate column

        >>> df4.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings").collect()
        [Row(year=2012, dotNET=15000, Java=20000), Row(year=2013, dotNET=48000, Java=30000)]

        # Or without specifying column values (less efficient)

        >>> df4.groupBy("year").pivot("course").sum("earnings").collect()
        [Row(year=2012, Java=20000, dotNET=15000), Row(year=2013, Java=30000, dotNET=48000)]
        >>> df5.groupBy("sales.year").pivot("sales.course").sum("sales.earnings").collect()
        [Row(year=2012, Java=20000, dotNET=15000), Row(year=2013, Java=30000, dotNET=48000)]
        """
        if values is None:
            jgd = self._jgd.pivot(pivot_col)
        else:
            jgd = self._jgd.pivot(pivot_col, values)
        return GroupedData(jgd, self._df)


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
    globs['df5'] = sc.parallelize([
        Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=10000)),
        Row(training="junior", sales=Row(course="Java",   year=2012, earnings=20000)),
        Row(training="expert", sales=Row(course="dotNET", year=2012, earnings=5000)),
        Row(training="junior", sales=Row(course="dotNET", year=2013, earnings=48000)),
        Row(training="expert", sales=Row(course="Java",   year=2013, earnings=30000))]).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.group, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
