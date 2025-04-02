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

import os
import string
from typing import Any, Dict, Optional, Union, List, Sequence, Mapping, Tuple
import uuid
import warnings

import pandas as pd

from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.namespace import _get_index_map
from pyspark import pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.utils import get_lit_sql_str
from pyspark.pandas.utils import default_session
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.series import Series
from pyspark.sql.utils import is_remote


__all__ = ["sql"]


# This is not used in this file. It's for legacy sql_processor.
_CAPTURE_SCOPES = 3


def sql(
    query: str,
    index_col: Optional[Union[str, List[str]]] = None,
    args: Optional[Union[Dict[str, Any], List]] = None,
    **kwargs: Any,
) -> DataFrame:
    """
    Execute a SQL query and return the result as a pandas-on-Spark DataFrame.

    This function acts as a standard Python string formatter with understanding
    the following variable types:

        * pandas-on-Spark DataFrame
        * pandas-on-Spark Series
        * pandas DataFrame
        * pandas Series
        * string

    Also the method can bind named parameters to SQL literals from `args`.

    .. note::
        pandas-on-Spark DataFrame is not supported for Spark Connect.

    Parameters
    ----------
    query : str
        the SQL query
    index_col : str or list of str, optional
        Column names to be used in Spark to represent pandas-on-Spark's index. The index name
        in pandas-on-Spark is ignored. By default, the index is always lost.

        .. note:: If you want to preserve the index, explicitly use :func:`DataFrame.reset_index`,
            and pass it to the SQL statement with `index_col` parameter.

            For example,

            >>> psdf = ps.DataFrame({"A": [1, 2, 3], "B":[4, 5, 6]}, index=['a', 'b', 'c'])
            >>> new_psdf = psdf.reset_index()
            >>> ps.sql("SELECT * FROM {new_psdf}", index_col="index", new_psdf=new_psdf)
            ... # doctest: +NORMALIZE_WHITESPACE
                   A  B
            index
            a      1  4
            b      2  5
            c      3  6

            For MultiIndex,

            >>> psdf = ps.DataFrame(
            ...     {"A": [1, 2, 3], "B": [4, 5, 6]},
            ...     index=pd.MultiIndex.from_tuples(
            ...         [("a", "b"), ("c", "d"), ("e", "f")], names=["index1", "index2"]
            ...     ),
            ... )
            >>> new_psdf = psdf.reset_index()
            >>> ps.sql(
            ...     "SELECT * FROM {new_psdf}", index_col=["index1", "index2"], new_psdf=new_psdf)
            ... # doctest: +NORMALIZE_WHITESPACE
                           A  B
            index1 index2
            a      b       1  4
            c      d       2  5
            e      f       3  6

            Also note that the index name(s) should be matched to the existing name.
    args : dict or list
        A dictionary of parameter names to Python objects or a list of Python objects
        that can be converted to SQL literal expressions. See
        `Supported Data Types <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_
        for supported value types in Python.
        For example, dictionary keys: "rank", "name", "birthdate";
        dictionary values: 1, "Steven", datetime.date(2023, 4, 2).
        A value can be also a `Column` of a literal or collection constructor functions such
        as `map()`, `array()`, `struct()`, in that case it is taken as is.

        .. versionadded:: 3.4.0

        .. versionchanged:: 3.5.0
            Added positional parameters.

    kwargs
        other variables that the user want to set that can be referenced in the query

    Returns
    -------
    pandas-on-Spark DataFrame

    Examples
    --------

    Calling a built-in SQL function.

    >>> ps.sql("SELECT * FROM range(10) where id > 7")
       id
    0   8
    1   9

    >>> ps.sql("SELECT * FROM range(10) WHERE id > {bound1} AND id < {bound2}", bound1=7, bound2=9)
       id
    0   8

    >>> mydf = ps.range(10)
    >>> x = tuple(range(4))
    >>> ps.sql("SELECT {ser} FROM {mydf} WHERE id IN {x}", ser=mydf.id, mydf=mydf, x=x)
       id
    0   0
    1   1
    2   2
    3   3

    Mixing pandas-on-Spark and pandas DataFrames in a join operation. Note that the index is
    dropped.

    >>> ps.sql('''
    ...   SELECT m1.a, m2.b
    ...   FROM {table1} m1 INNER JOIN {table2} m2
    ...   ON m1.key = m2.key
    ...   ORDER BY m1.a, m2.b''',
    ...   table1=ps.DataFrame({"a": [1,2], "key": ["a", "b"]}),
    ...   table2=pd.DataFrame({"b": [3,4,5], "key": ["a", "b", "b"]}))
       a  b
    0  1  3
    1  2  4
    2  2  5

    Also, it is possible to query using Series.

    >>> psdf = ps.DataFrame({"A": [1, 2, 3], "B":[4, 5, 6]}, index=['a', 'b', 'c'])
    >>> ps.sql("SELECT {mydf.A} FROM {mydf}", mydf=psdf)
       A
    0  1
    1  2
    2  3

    And substitute named parameters with the `:` prefix by SQL literals.

    >>> ps.sql("SELECT * FROM range(10) WHERE id > :bound1", args={"bound1":7})
       id
    0   8
    1   9

    Or positional parameters marked by `?` in the SQL query by SQL literals.

    >>> ps.sql("SELECT * FROM range(10) WHERE id > ?", args=[7])
       id
    0   8
    1   9
    """
    if os.environ.get("PYSPARK_PANDAS_SQL_LEGACY") == "1":
        from pyspark.pandas import sql_processor

        warnings.warn(
            "Deprecated in 3.3.0, and the legacy behavior "
            "will be removed in the future releases.",
            FutureWarning,
        )
        return sql_processor.sql(query, index_col=index_col, **kwargs)

    session = default_session()
    formatter = PandasSQLStringFormatter(session)
    try:
        if not is_remote():
            sdf = session.sql(formatter.format(query, **kwargs), args)
        else:
            ps_query = formatter.format(query, **kwargs)
            # here the new_kwargs stores the views
            new_kwargs = {}
            for psdf, name in formatter._temp_views:
                new_kwargs[name] = psdf._to_spark()
            # delegate views to spark.sql
            sdf = session.sql(ps_query, args, **new_kwargs)
    finally:
        formatter.clear()

    index_spark_columns, index_names = _get_index_map(sdf, index_col)

    return DataFrame(
        InternalFrame(
            spark_frame=sdf, index_spark_columns=index_spark_columns, index_names=index_names
        )
    )


class PandasSQLStringFormatter(string.Formatter):
    """
    A standard ``string.Formatter`` in Python that can understand pandas-on-Spark instances
    with basic Python objects. This object must be clear after the use for single SQL
    query; cannot be reused across multiple SQL queries without cleaning.
    """

    def __init__(self, session: SparkSession) -> None:
        self._session: SparkSession = session
        self._temp_views: List[Tuple[DataFrame, str]] = []
        self._ref_sers: List[Tuple[Series, str]] = []

    def vformat(self, format_string: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> str:
        ret = super(PandasSQLStringFormatter, self).vformat(format_string, args, kwargs)

        for ref, n in self._ref_sers:
            if not any((ref is v for v in df._pssers.values()) for df, _ in self._temp_views):
                # If referred DataFrame does not hold the given Series, raise an error.
                raise ValueError("The series in {%s} does not refer any dataframe specified." % n)
        return ret

    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        obj, first = super(PandasSQLStringFormatter, self).get_field(field_name, args, kwargs)
        return self._convert_value(obj, field_name), first

    def _convert_value(self, val: Any, name: str) -> Optional[str]:
        """
        Converts the given value into a SQL string.
        """
        if isinstance(val, pd.Series):
            # Return the column name from pandas Series directly.
            return ps.from_pandas(val).to_frame()._to_spark().columns[0]
        elif isinstance(val, Series):
            # Return the column name of pandas-on-Spark Series iff its DataFrame was
            # referred. The check will be done in `vformat` after we parse all.
            self._ref_sers.append((val, name))
            return val.to_frame()._to_spark().columns[0]
        elif isinstance(val, (DataFrame, pd.DataFrame)):
            df_name = "_pandas_api_%s" % str(uuid.uuid4()).replace("-", "")

            if not is_remote():
                if isinstance(val, pd.DataFrame):
                    # Don't store temp view for plain pandas instances
                    # because it is unable to know which pandas DataFrame
                    # holds which Series.
                    val = ps.from_pandas(val)
                else:
                    for df, n in self._temp_views:
                        if df is val:
                            return n
                    self._temp_views.append((val, df_name))
                val._to_spark().createOrReplaceTempView(df_name)
                return df_name
            else:
                if isinstance(val, pd.DataFrame):
                    # Always convert pd.DataFrame to ps.DataFrame, and record it in _temp_views.
                    val = ps.from_pandas(val)

                for df, n in self._temp_views:
                    if df is val:
                        return n
                self._temp_views.append((val, name))
                # In Spark Connect, keep the original view name here (not the UUID one),
                # the reformatted query is like: 'select * from {tbl} where A > 1'
                # and then delegate the view operations to spark.sql.
                return "{" + name + "}"
        elif isinstance(val, str):
            return get_lit_sql_str(val)
        else:
            return val

    def clear(self) -> None:
        # In Spark Connect, views are created and dropped in Connect Server
        if not is_remote():
            for _, n in self._temp_views:
                self._session.catalog.dropTempView(n)
        self._temp_views = []
        self._ref_sers = []


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.sql_formatter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.sql_formatter.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.sql_formatter tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.sql_formatter,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
