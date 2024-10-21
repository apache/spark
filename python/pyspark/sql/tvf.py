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
from typing import Optional, TYPE_CHECKING

from pyspark.errors import PySparkValueError
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName

__all__ = ["TableValuedFunction"]


class TableValuedFunction:
    """
    Interface for creating table-valued functions in Spark SQL.
    """

    def __init__(self, sparkSession: SparkSession):
        self._sparkSession = sparkSession

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        """
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        start : int
            the start value
        end : int, optional
            the end value (exclusive)
        step : int, optional
            the incremental step (default: 1)
        numPartitions : int, optional
            the number of partitions of the DataFrame

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> spark.tvf.range(1, 7, 2).show()
        +---+
        | id|
        +---+
        |  1|
        |  3|
        |  5|
        +---+

        If only one argument is specified, it will be used as the end value.

        >>> spark.tvf.range(3).show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        +---+
        """
        return self._sparkSession.range(start, end, step, numPartitions)

    def explode(self, collection: "ColumnOrName") -> DataFrame:
        """
        Returns a :class:`DataFrame` containing a new row for each element
        in the given array or map.
        Uses the default column name `col` for elements in the array and
        `key` and `value` for elements in the map unless specified otherwise.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        collection : :class:`~pyspark.sql.Column` or str
            Target column to work on.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.explode`

        Examples
        --------
        Example 1: Exploding an array column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode(sf.array(sf.lit(1), sf.lit(2), sf.lit(3))).show()
        +---+
        |col|
        +---+
        |  1|
        |  2|
        |  3|
        +---+

        Example 2: Exploding a map column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode(
        ...     sf.create_map(sf.lit("a"), sf.lit("b"), sf.lit("c"), sf.lit("d"))
        ... ).show()
        +---+-----+
        |key|value|
        +---+-----+
        |  a|    b|
        |  c|    d|
        +---+-----+

        Example 3: Exploding an array of struct column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode(sf.array(
        ...     sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
        ...     sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4))
        ... )).select("col.*").show()
        +---+---+
        |  a|  b|
        +---+---+
        |  1|  2|
        |  3|  4|
        +---+---+

        Example 4: Exploding an empty array column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode(sf.array()).show()
        +---+
        |col|
        +---+
        +---+

        Example 5: Exploding an empty map column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode(sf.create_map()).show()
        +---+-----+
        |key|value|
        +---+-----+
        +---+-----+
        """
        return self._fn("explode", collection)

    def explode_outer(self, collection: "ColumnOrName") -> DataFrame:
        """
        Returns a :class:`DataFrame` containing a new row for each element with position
        in the given array or map.
        Unlike explode, if the array/map is null or empty then null is produced.
        Uses the default column name `col` for elements in the array and
        `key` and `value` for elements in the map unless specified otherwise.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        collection : :class:`~pyspark.sql.Column` or str
            target column to work on.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.explode_outer`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.explode_outer(sf.array(sf.lit("foo"), sf.lit("bar"))).show()
        +---+
        |col|
        +---+
        |foo|
        |bar|
        +---+
        >>> spark.tvf.explode_outer(sf.array()).show()
        +----+
        | col|
        +----+
        |NULL|
        +----+
        >>> spark.tvf.explode_outer(sf.create_map(sf.lit("x"), sf.lit(1.0))).show()
        +---+-----+
        |key|value|
        +---+-----+
        |  x|  1.0|
        +---+-----+
        >>> spark.tvf.explode_outer(sf.create_map()).show()
        +----+-----+
        | key|value|
        +----+-----+
        |NULL| NULL|
        +----+-----+
        """
        return self._fn("explode_outer", collection)

    def inline(self, input: "ColumnOrName") -> DataFrame:
        """
        Explodes an array of structs into a table.

        This function takes an input column containing an array of structs and returns a
        new column where each struct in the array is exploded into a separate row.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        input : :class:`~pyspark.sql.Column` or str
            Input column of values to explode.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.inline`

        Examples
        --------
        Example 1: Using inline with a single struct array

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.inline(sf.array(
        ...     sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
        ...     sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4))
        ... )).show()
        +---+---+
        |  a|  b|
        +---+---+
        |  1|  2|
        |  3|  4|
        +---+---+

        Example 2: Using inline with an empty struct array column

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.inline(sf.array().astype("array<struct<a:int,b:int>>")).show()
        +---+---+
        |  a|  b|
        +---+---+
        +---+---+

        Example 3: Using inline with a struct array column containing null values

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.inline(sf.array(
        ...     sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
        ...     sf.lit(None),
        ...     sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4))
        ... )).show()
        +----+----+
        |   a|   b|
        +----+----+
        |   1|   2|
        |NULL|NULL|
        |   3|   4|
        +----+----+
        """
        return self._fn("inline", input)

    def inline_outer(self, input: "ColumnOrName") -> DataFrame:
        """
        Explodes an array of structs into a table.
        Unlike inline, if the array is null or empty then null is produced for each nested column.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        input : :class:`~pyspark.sql.Column` or str
            input column of values to explode.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.inline_outer`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.inline_outer(sf.array(
        ...     sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
        ...     sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4))
        ... )).show()
        +---+---+
        |  a|  b|
        +---+---+
        |  1|  2|
        |  3|  4|
        +---+---+
        >>> spark.tvf.inline_outer(sf.array().astype("array<struct<a:int,b:int>>")).show()
        +----+----+
        |   a|   b|
        +----+----+
        |NULL|NULL|
        +----+----+
        """
        return self._fn("inline_outer", input)

    def json_tuple(self, input: "ColumnOrName", *fields: "ColumnOrName") -> DataFrame:
        """
        Creates a new row for a json column according to the given field names.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        input : :class:`~pyspark.sql.Column` or str
            string column in json format
        fields : :class:`~pyspark.sql.Column` or str
            a field or fields to extract

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.json_tuple`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.json_tuple(
        ...     sf.lit('{"f1": "value1", "f2": "value2"}'), sf.lit("f1"), sf.lit("f2")
        ... ).show()
        +------+------+
        |    c0|    c1|
        +------+------+
        |value1|value2|
        +------+------+
        """
        from pyspark.sql.classic.column import _to_seq, _to_java_column

        if len(fields) == 0:
            raise PySparkValueError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "field"},
            )

        sc = self._sparkSession.sparkContext
        return DataFrame(
            self._sparkSession._jsparkSession.tvf().json_tuple(
                _to_java_column(input), _to_seq(sc, fields, _to_java_column)
            ),
            self._sparkSession,
        )

    def posexplode(self, collection: "ColumnOrName") -> DataFrame:
        """
        Returns a :class:`DataFrame` containing a new row for each element with position
        in the given array or map.
        Uses the default column name `pos` for position, and `col` for elements in the
        array and `key` and `value` for elements in the map unless specified otherwise.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        collection : :class:`~pyspark.sql.Column` or str
            target column to work on.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.posexplode`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.posexplode(sf.array(sf.lit(1), sf.lit(2), sf.lit(3))).show()
        +---+---+
        |pos|col|
        +---+---+
        |  0|  1|
        |  1|  2|
        |  2|  3|
        +---+---+
        >>> spark.tvf.posexplode(sf.create_map(sf.lit("a"), sf.lit("b"))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        |  0|  a|    b|
        +---+---+-----+
        """
        return self._fn("posexplode", collection)

    def posexplode_outer(self, collection: "ColumnOrName") -> DataFrame:
        """
        Returns a :class:`DataFrame` containing a new row for each element with position
        in the given array or map.
        Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced.
        Uses the default column name `pos` for position, and `col` for elements in the
        array and `key` and `value` for elements in the map unless specified otherwise.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        collection : :class:`~pyspark.sql.Column` or str
            target column to work on.

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.posexplode_outer`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.posexplode_outer(sf.array(sf.lit("foo"), sf.lit("bar"))).show()
        +---+---+
        |pos|col|
        +---+---+
        |  0|foo|
        |  1|bar|
        +---+---+
        >>> spark.tvf.posexplode_outer(sf.array()).show()
        +----+----+
        | pos| col|
        +----+----+
        |NULL|NULL|
        +----+----+
        >>> spark.tvf.posexplode_outer(sf.create_map(sf.lit("x"), sf.lit(1.0))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        |  0|  x|  1.0|
        +---+---+-----+
        >>> spark.tvf.posexplode_outer(sf.create_map()).show()
        +----+----+-----+
        | pos| key|value|
        +----+----+-----+
        |NULL|NULL| NULL|
        +----+----+-----+
        """
        return self._fn("posexplode_outer", collection)

    def stack(self, n: "ColumnOrName", *fields: "ColumnOrName") -> DataFrame:
        """
        Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default
        unless specified otherwise.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        n : :class:`~pyspark.sql.Column` or str
            the number of rows to separate
        fields : :class:`~pyspark.sql.Column` or str
            input elements to be separated

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        :meth:`pyspark.sql.functions.stack`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.stack(sf.lit(2), sf.lit(1), sf.lit(2), sf.lit(3)).show()
        +----+----+
        |col0|col1|
        +----+----+
        |   1|   2|
        |   3|NULL|
        +----+----+
        """
        from pyspark.sql.classic.column import _to_seq, _to_java_column

        sc = self._sparkSession.sparkContext
        return DataFrame(
            self._sparkSession._jsparkSession.tvf().stack(
                _to_java_column(n), _to_seq(sc, fields, _to_java_column)
            ),
            self._sparkSession,
        )

    def collations(self) -> DataFrame:
        """
        Get all of the Spark SQL string collations.

        .. versionadded:: 4.0.0

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> spark.tvf.collations().show()
        +-------+-------+-------------+...
        |CATALOG| SCHEMA|         NAME|...
        +-------+-------+-------------+...
        ...
        +-------+-------+-------------+...
        """
        return self._fn("collations")

    def sql_keywords(self) -> DataFrame:
        """
        Get Spark SQL keywords.

        .. versionadded:: 4.0.0

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> spark.tvf.sql_keywords().show()
        +-------------+--------+
        |      keyword|reserved|
        +-------------+--------+
        ...
        +-------------+--------+...
        """
        return self._fn("sql_keywords")

    def variant_explode(self, input: "ColumnOrName") -> DataFrame:
        """
        Separates a variant object/array into multiple rows containing its fields/elements.

        Its result schema is `struct<pos int, key string, value variant>`. `pos` is the position of
        the field/element in its parent object/array, and `value` is the field/element value.
        `key` is the field name when exploding a variant object, or is NULL when exploding a variant
        array. It ignores any input that is not a variant array/object, including SQL NULL, variant
        null, and any other variant values.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        input : :class:`~pyspark.sql.Column` or str
            input column of values to explode.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        Example 1: Using variant_explode with a variant array

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.variant_explode(sf.parse_json(sf.lit('["hello", "world"]'))).show()
        +---+----+-------+
        |pos| key|  value|
        +---+----+-------+
        |  0|NULL|"hello"|
        |  1|NULL|"world"|
        +---+----+-------+

        Example 2: Using variant_explode with a variant object

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.variant_explode(sf.parse_json(sf.lit('{"a": true, "b": 3.14}'))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        |  0|  a| true|
        |  1|  b| 3.14|
        +---+---+-----+

        Example 3: Using variant_explode with an empty variant array

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.variant_explode(sf.parse_json(sf.lit('[]'))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        +---+---+-----+

        Example 4: Using variant_explode with an empty variant object

        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.variant_explode(sf.parse_json(sf.lit('{}'))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        +---+---+-----+
        """
        return self._fn("variant_explode", input)

    def variant_explode_outer(self, input: "ColumnOrName") -> DataFrame:
        """
        Separates a variant object/array into multiple rows containing its fields/elements.
        Unlike variant_explode, if the given variant is not a variant object/array, or
        the variant object/array is null or empty, then null is produced.

        Its result schema is `struct<pos int, key string, value variant>`. `pos` is the position of
        the field/element in its parent object/array, and `value` is the field/element value.
        `key` is the field name when exploding a variant object, or is NULL when exploding a variant
        array. It ignores any input that is not a variant array/object, including SQL NULL, variant
        null, and any other variant values.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        input : :class:`~pyspark.sql.Column` or str
            input column of values to explode.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> import pyspark.sql.functions as sf
        >>> spark.tvf.variant_explode_outer(sf.parse_json(sf.lit('["hello", "world"]'))).show()
        +---+----+-------+
        |pos| key|  value|
        +---+----+-------+
        |  0|NULL|"hello"|
        |  1|NULL|"world"|
        +---+----+-------+
        >>> spark.tvf.variant_explode_outer(sf.parse_json(sf.lit('[]'))).show()
        +----+----+-----+
        | pos| key|value|
        +----+----+-----+
        |NULL|NULL| NULL|
        +----+----+-----+
        >>> spark.tvf.variant_explode_outer(sf.parse_json(sf.lit('{"a": true, "b": 3.14}'))).show()
        +---+---+-----+
        |pos|key|value|
        +---+---+-----+
        |  0|  a| true|
        |  1|  b| 3.14|
        +---+---+-----+
        >>> spark.tvf.variant_explode_outer(sf.parse_json(sf.lit('{}'))).show()
        +----+----+-----+
        | pos| key|value|
        +----+----+-----+
        |NULL|NULL| NULL|
        +----+----+-----+
        """
        return self._fn("variant_explode_outer", input)

    def _fn(self, functionName: str, *args: "ColumnOrName") -> DataFrame:
        from pyspark.sql.classic.column import _to_java_column

        return DataFrame(
            getattr(self._sparkSession._jsparkSession.tvf(), functionName)(
                *(_to_java_column(arg) for arg in args)
            ),
            self._sparkSession,
        )


def _test() -> None:
    import os
    import doctest
    import sys
    import pyspark.sql.tvf

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.tvf.__dict__.copy()
    globs["spark"] = SparkSession.builder.master("local[4]").appName("sql.tvf tests").getOrCreate()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.tvf,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    globs["spark"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
