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

import _string  # type: ignore[import]
from typing import Any, Dict, Optional, Union, List  # noqa: F401 (SPARK-34943)
import inspect
import pandas as pd

from pyspark.sql import SparkSession, DataFrame as SDataFrame  # noqa: F401 (SPARK-34943)

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas.utils import default_session
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.series import Series
from pyspark.pandas.internal import InternalFrame
from pyspark.pandas.namespace import _get_index_map


__all__ = ["sql"]

from builtins import globals as builtin_globals
from builtins import locals as builtin_locals


def sql(
    query: str,
    index_col: Optional[Union[str, List[str]]] = None,
    globals: Optional[Dict[str, Any]] = None,
    locals: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> DataFrame:
    """
    Execute a SQL query and return the result as a pandas-on-Spark DataFrame.

    This function also supports embedding Python variables (locals, globals, and parameters)
    in the SQL statement by wrapping them in curly braces. See examples section for details.

    In addition to the locals, globals and parameters, the function will also attempt
    to determine if the program currently runs in an IPython (or Jupyter) environment
    and to import the variables from this environment. The variables have the same
    precedence as globals.

    The following variable types are supported:

        * string
        * int
        * float
        * list, tuple, range of above types
        * pandas-on-Spark DataFrame
        * pandas-on-Spark Series
        * pandas DataFrame

    Parameters
    ----------
    query : str
        the SQL query
    index_col : str or list of str, optional
        Column names to be used in Spark to represent pandas-on-Spark's index. The index name
        in pandas-on-Spark is ignored. By default, the index is always lost.

        .. note:: If you want to preserve the index, explicitly use :func:`DataFrame.reset_index`,
            and pass it to the sql statement with `index_col` parameter.

            For example,

            >>> psdf = ps.DataFrame({"A": [1, 2, 3], "B":[4, 5, 6]}, index=['a', 'b', 'c'])
            >>> psdf_reset_index = psdf.reset_index()
            >>> ps.sql("SELECT * FROM {psdf_reset_index}", index_col="index")
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
            >>> psdf_reset_index = psdf.reset_index()
            >>> ps.sql("SELECT * FROM {psdf_reset_index}", index_col=["index1", "index2"])
            ... # doctest: +NORMALIZE_WHITESPACE
                           A  B
            index1 index2
            a      b       1  4
            c      d       2  5
            e      f       3  6

            Also note that the index name(s) should be matched to the existing name.

    globals : dict, optional
        the dictionary of global variables, if explicitly set by the user
    locals : dict, optional
        the dictionary of local variables, if explicitly set by the user
    kwargs
        other variables that the user may want to set manually that can be referenced in the query

    Returns
    -------
    pandas-on-Spark DataFrame

    Examples
    --------

    Calling a built-in SQL function.

    >>> ps.sql("select * from range(10) where id > 7")
       id
    0   8
    1   9

    A query can also reference a local variable or parameter by wrapping them in curly braces:

    >>> bound1 = 7
    >>> ps.sql("select * from range(10) where id > {bound1} and id < {bound2}", bound2=9)
       id
    0   8

    You can also wrap a DataFrame with curly braces to query it directly. Note that when you do
    that, the indexes, if any, automatically become top level columns.

    >>> mydf = ps.range(10)
    >>> x = range(4)
    >>> ps.sql("SELECT * from {mydf} WHERE id IN {x}")
       id
    0   0
    1   1
    2   2
    3   3

    Queries can also be arbitrarily nested in functions:

    >>> def statement():
    ...     mydf2 = ps.DataFrame({"x": range(2)})
    ...     return ps.sql("SELECT * from {mydf2}")
    >>> statement()
       x
    0  0
    1  1

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

    >>> myser = ps.Series({'a': [1.0, 2.0, 3.0], 'b': [15.0, 30.0, 45.0]})
    >>> ps.sql("SELECT * from {myser}")
                        0
    0     [1.0, 2.0, 3.0]
    1  [15.0, 30.0, 45.0]
    """
    if globals is None:
        globals = _get_ipython_scope()
    _globals = builtin_globals() if globals is None else dict(globals)
    _locals = builtin_locals() if locals is None else dict(locals)
    # The default choice is the globals
    _dict = dict(_globals)
    # The vars:
    _scope = _get_local_scope()
    _dict.update(_scope)
    # Then the locals
    _dict.update(_locals)
    # Highest order of precedence is the locals
    _dict.update(kwargs)
    return SQLProcessor(_dict, query, default_session()).execute(index_col)


_CAPTURE_SCOPES = 2


def _get_local_scope() -> Dict[str, Any]:
    # Get 2 scopes above (_get_local_scope -> sql -> ...) to capture the vars there.
    try:
        return inspect.stack()[_CAPTURE_SCOPES][0].f_locals
    except Exception as e:
        # TODO (rxin, thunterdb): use a more narrow scope exception.
        # See https://github.com/pyspark.pandas/pull/448
        return {}


def _get_ipython_scope() -> Dict[str, Any]:
    """
    Tries to extract the dictionary of variables if the program is running
    in an IPython notebook environment.
    """
    try:
        from IPython import get_ipython  # type: ignore[import]

        shell = get_ipython()
        return shell.user_ns
    except Exception as e:
        # TODO (rxin, thunterdb): use a more narrow scope exception.
        # See https://github.com/pyspark.pandas/pull/448
        return None


# Originally from pymysql package
_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = "\\0"
_escape_table[ord("\\")] = "\\\\"
_escape_table[ord("\n")] = "\\n"
_escape_table[ord("\r")] = "\\r"
_escape_table[ord("\032")] = "\\Z"
_escape_table[ord('"')] = '\\"'
_escape_table[ord("'")] = "\\'"


def escape_sql_string(value: str) -> str:
    """Escapes value without adding quotes.

    >>> escape_sql_string("foo\\nbar")
    'foo\\\\nbar'

    >>> escape_sql_string("'abc'de")
    "\\\\'abc\\\\'de"

    >>> escape_sql_string('"abc"de')
    '\\\\"abc\\\\"de'
    """
    return value.translate(_escape_table)


class SQLProcessor(object):
    def __init__(self, scope: Dict[str, Any], statement: str, session: SparkSession):
        self._scope = scope
        self._statement = statement
        # All the temporary views created when executing this statement
        # The key is the name of the variable in {}
        # The value is the cached Spark Dataframe.
        self._temp_views = {}  # type: Dict[str, SDataFrame]
        # All the other variables, converted to a normalized form.
        # The normalized form is typically a string
        self._cached_vars = {}  # type: Dict[str, Any]
        # The SQL statement after:
        # - all the dataframes have been registered as temporary views
        # - all the values have been converted normalized to equivalent SQL representations
        self._normalized_statement = None  # type: Optional[str]
        self._session = session

    def execute(self, index_col: Optional[Union[str, List[str]]]) -> DataFrame:
        """
        Returns a DataFrame for which the SQL statement has been executed by
        the underlying SQL engine.

        >>> str0 = 'abc'
        >>> ps.sql("select {str0}")
           abc
        0  abc

        >>> str1 = 'abc"abc'
        >>> str2 = "abc'abc"
        >>> ps.sql("select {str0}, {str1}, {str2}")
           abc  abc"abc  abc'abc
        0  abc  abc"abc  abc'abc

        >>> strs = ['a', 'b']
        >>> ps.sql("select 'a' in {strs} as cond1, 'c' in {strs} as cond2")
           cond1  cond2
        0   True  False
        """
        blocks = _string.formatter_parser(self._statement)
        # TODO: use a string builder
        res = ""
        try:
            for (pre, inner, _, _) in blocks:
                var_next = "" if inner is None else self._convert(inner)
                res = res + pre + var_next
            self._normalized_statement = res

            sdf = self._session.sql(self._normalized_statement)
        finally:
            for v in self._temp_views:
                self._session.catalog.dropTempView(v)

        index_spark_columns, index_names = _get_index_map(sdf, index_col)

        return DataFrame(
            InternalFrame(
                spark_frame=sdf, index_spark_columns=index_spark_columns, index_names=index_names
            )
        )

    def _convert(self, key: str) -> Any:
        """
        Given a {} key, returns an equivalent SQL representation.
        This conversion performs all the necessary escaping so that the string
        returned can be directly injected into the SQL statement.
        """
        # Already cached?
        if key in self._cached_vars:
            return self._cached_vars[key]
        # Analyze:
        if key not in self._scope:
            raise ValueError(
                "The key {} in the SQL statement was not found in global,"
                " local or parameters variables".format(key)
            )
        var = self._scope[key]
        fillin = self._convert_var(var)
        self._cached_vars[key] = fillin
        return fillin

    def _convert_var(self, var: Any) -> Any:
        """
        Converts a python object into a string that is legal SQL.
        """
        if isinstance(var, (int, float)):
            return str(var)
        if isinstance(var, Series):
            return self._convert_var(var.to_dataframe())
        if isinstance(var, pd.DataFrame):
            return self._convert_var(ps.DataFrame(var))
        if isinstance(var, DataFrame):
            df_id = "pandas_on_spark_" + str(id(var))
            if df_id not in self._temp_views:
                sdf = var.to_spark()
                sdf.createOrReplaceTempView(df_id)
                self._temp_views[df_id] = sdf
            return df_id
        if isinstance(var, str):
            return '"' + escape_sql_string(var) + '"'
        if isinstance(var, list):
            return "(" + ", ".join([self._convert_var(v) for v in var]) + ")"
        if isinstance(var, (tuple, range)):
            return self._convert_var(list(var))
        raise ValueError("Unsupported variable type {}: {}".format(type(var).__name__, str(var)))


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.sql_processor

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.sql_processor.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.sql_processor tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.sql_processor,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
