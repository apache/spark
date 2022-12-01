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
from pyspark.sql.connect.column import Column, LiteralExpression, ColumnReference

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName

# TODO(SPARK-40538) Add support for the missing PySpark functions.


def _to_col(col: "ColumnOrName") -> Column:
    return col if isinstance(col, Column) else column(col)


def col(x: str) -> Column:
    return Column(ColumnReference(x))


column = col


def lit(x: Any) -> Column:
    return Column(LiteralExpression(x))


def asc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given column name.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> df = spark.range(5)
    >>> df = df.sort(desc("id"))
    >>> df.show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+

    Sort by the column 'id' in the ascending order.

    >>> df.orderBy(asc("id")).show()
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    +---+
    """
    return _to_col(col).asc()


def asc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values return before non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(1, "Bob"),
    ...                              (0, None),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  2|Alice|
    |  1|  Bob|
    +---+-----+

    """
    return _to_col(col).asc_nulls_first()


def asc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  2|Alice|
    |  1|  Bob|
    |  0| null|
    +---+-----+

    """
    return _to_col(col).asc_nulls_last()


def desc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given column name.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> spark.range(5).orderBy(desc("id")).show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+
    """
    return _to_col(col).desc()


def desc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear before non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| null|
    |  1|  Bob|
    |  2|Alice|
    +---+-----+

    """
    return _to_col(col).desc_nulls_first()


def desc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  1|  Bob|
    |  2|Alice|
    |  0| null|
    +---+-----+

    """
    return _to_col(col).desc_nulls_last()
