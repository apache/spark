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

from pyspark import since
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame


class CoGroupedData(object):
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
    def apply(self, udf):
        """
        Applies a function to each cogroup using a pandas udf and returns the result
        as a `DataFrame`.

        The user-defined function should take two `pandas.DataFrame` and return another
        `pandas.DataFrame`.  For each side of the cogroup, all columns are passed together as a
        `pandas.DataFrame` to the user-function and the returned `pandas.DataFrame` are combined as
        a :class:`DataFrame`.

        The returned `pandas.DataFrame` can be of arbitrary length and its schema must match the
        returnType of the pandas udf.

        .. note:: This function requires a full shuffle. All the data of a cogroup will be loaded
            into memory, so the user should be aware of the potential OOM risk if data is skewed
            and certain groups are too large to fit in memory.

        .. note:: Experimental

        :param udf: a cogrouped map user-defined function returned by
            :func:`pyspark.sql.functions.pandas_udf`.

        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df1 = spark.createDataFrame(
        ...     [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
        ...     ("time", "id", "v1"))
        >>> df2 = spark.createDataFrame(
        ...     [(20000101, 1, "x"), (20000101, 2, "y")],
        ...     ("time", "id", "v2"))
        >>> @pandas_udf("time int, id int, v1 double, v2 string",
        ...             PandasUDFType.COGROUPED_MAP)  # doctest: +SKIP
        ... def asof_join(l, r):
        ...     return pd.merge_asof(l, r, on="time", by="id")
        >>> df1.groupby("id").cogroup(df2.groupby("id")).apply(asof_join).show()  # doctest: +SKIP
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

        >>> @pandas_udf("time int, id int, v1 double, v2 string",
        ...             PandasUDFType.COGROUPED_MAP)  # doctest: +SKIP
        ... def asof_join(k, l, r):
        ...     if k == (1,):
        ...         return pd.merge_asof(l, r, on="time", by="id")
        ...     else:
        ...         return pd.DataFrame(columns=['time', 'id', 'v1', 'v2'])
        >>> df1.groupby("id").cogroup(df2.groupby("id")).apply(asof_join).show()  # doctest: +SKIP
        +--------+---+---+---+
        |    time| id| v1| v2|
        +--------+---+---+---+
        |20000101|  1|1.0|  x|
        |20000102|  1|3.0|  x|
        +--------+---+---+---+

        .. seealso:: :meth:`pyspark.sql.functions.pandas_udf`

        """
        # Columns are special because hasattr always return True
        if isinstance(udf, Column) or not hasattr(udf, 'func') \
           or udf.evalType != PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            raise ValueError("Invalid udf: the udf argument must be a pandas_udf of type "
                             "COGROUPED_MAP.")
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
    import pyspark.sql.cogroup
    globs = pyspark.sql.cogroup.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.cogroup tests")\
        .getOrCreate()
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.cogroup, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
