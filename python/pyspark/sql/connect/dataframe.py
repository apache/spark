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

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
    overload,
    Callable,
    cast,
    Type,
)

import pandas
import warnings
from collections.abc import Iterable

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.readwriter import DataFrameWriter
from pyspark.sql.connect.column import (
    Column,
    scalar_function,
    sql_expression,
)
from pyspark.sql.connect.functions import col, lit
from pyspark.sql.types import (
    StructType,
    Row,
)
from pyspark import _NoValue
from pyspark._globals import _NoValueType

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName, LiteralType, OptionalPrimitiveType
    from pyspark.sql.connect.session import SparkSession


class GroupedData(object):
    def __init__(self, df: "DataFrame", *grouping_cols: Union[Column, str]) -> None:
        self._df = df
        self._grouping_cols = [x if isinstance(x, Column) else df[x] for x in grouping_cols]

    @overload
    def agg(self, *exprs: Column) -> "DataFrame":
        ...

    @overload
    def agg(self, __exprs: Dict[str, str]) -> "DataFrame":
        ...

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
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

        .. versionadded:: 3.4.0

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
        >>> from pyspark.sql import functions as F
        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name).agg({"*": "count"}).sort("name").show()
        +-----+--------+
        | name|count(1)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       2|
        +-----+--------+

        Group-by name, and calculate the minimum age.

        >>> df.groupBy(df.name).agg(F.min(df.age)).sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # Convert the dict into key value pairs
            measures = [scalar_function(exprs[0][k], col(k)) for k in exprs[0]]
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            measures = cast(List[Column], list(exprs))

        res = DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                measures=measures,
            ),
            session=self._df._session,
        )
        return res

    def _map_cols_to_expression(self, fun: str, param: Union[Column, str]) -> Sequence[Column]:
        return [
            scalar_function(fun, col(param)) if isinstance(param, str) else param,
        ]

    def min(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("min", col)
        return self.agg(*expr)

    def max(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("max", col)
        return self.agg(*expr)

    def sum(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("sum", col)
        return self.agg(*expr)

    def avg(self, col: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_expression("avg", col)
        return self.agg(*expr)

    def count(self) -> "DataFrame":
        return self.agg(scalar_function("count", lit(1)))


class DataFrame(object):
    """Every DataFrame object essentially is a Relation that is refined using the
    member functions. Calling a method on a dataframe will essentially return a copy
    of the DataFrame with the changes applied.
    """

    def __init__(
        self,
        session: "SparkSession",
        data: Optional[List[Any]] = None,
        schema: Optional[StructType] = None,
    ):
        """Creates a new data frame"""
        self._schema = schema
        self._plan: Optional[plan.LogicalPlan] = None
        self._session: "SparkSession" = session

    def __repr__(self) -> str:
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    @property
    def write(self) -> "DataFrameWriter":
        assert self._plan is not None
        return DataFrameWriter(self._plan, self._session)

    @classmethod
    def withPlan(cls, plan: plan.LogicalPlan, session: "SparkSession") -> "DataFrame":
        """Main initialization method used to construct a new data frame with a child plan."""
        new_frame = DataFrame(session=session)
        new_frame._plan = plan
        return new_frame

    def isEmpty(self) -> bool:
        """Returns ``True`` if this :class:`DataFrame` is empty.

        .. versionadded:: 3.4.0

        Returns
        -------
        bool
            Whether it's empty DataFrame or not.
        """
        return len(self.take(1)) == 0

    def select(self, *cols: "ColumnOrName") -> "DataFrame":
        return DataFrame.withPlan(plan.Project(self._plan, *cols), session=self._session)

    def selectExpr(self, *expr: Union[str, List[str]]) -> "DataFrame":
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`DataFrame`
            A DataFrame with new/old columns transformed by expressions.
        """
        sql_expr = []
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]  # type: ignore[assignment]
        for element in expr:
            if isinstance(element, str):
                sql_expr.append(sql_expression(element))
            else:
                sql_expr.extend([sql_expression(e) for e in element])

        return DataFrame.withPlan(plan.Project(self._plan, *sql_expr), session=self._session)

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        if not exprs:
            raise ValueError("Argument 'exprs' must not be empty")

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            measures = [scalar_function(f, col(e)) for e, f in exprs[0].items()]
            return self.groupBy().agg(*measures)
        else:
            # other expressions
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Expression"
            exprs = cast(Tuple[Column, ...], exprs)
            return self.groupBy().agg(*exprs)

    def alias(self, alias: str) -> "DataFrame":
        return DataFrame.withPlan(plan.SubqueryAlias(self._plan, alias), session=self._session)

    def approxQuantile(self, col: Column, probabilities: Any, relativeError: Any) -> "DataFrame":
        ...

    def colRegex(self, regex: str) -> "DataFrame":
        ...

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Returns all column names and their data types as a list.

        .. versionadded:: 3.4.0

        Returns
        -------
        list
            List of columns as tuple pairs.
        """
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self) -> List[str]:
        """Returns the list of columns of the current data frame."""
        if self._plan is None:
            return []

        return self.schema.names

    def sparkSession(self) -> "SparkSession":
        """Returns Spark session that created this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`SparkSession`
        """
        return self._session

    def count(self) -> int:
        """Returns the number of rows in this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Returns
        -------
        int
            Number of rows.
        """
        pdd = self.agg(scalar_function("count", lit(1))).toPandas()
        return pdd.iloc[0, 0]

    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        """
        Returns the cartesian product with another :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the cartesian product.

        Returns
        -------
        :class:`DataFrame`
            Joined DataFrame.
        """
        if self._plan is None:
            raise Exception("Cannot cartesian join when self._plan is empty.")
        if other._plan is None:
            raise Exception("Cannot cartesian join when other._plan is empty.")

        return DataFrame.withPlan(
            plan.Join(left=self._plan, right=other._plan, on=None, how="cross"),
            session=self._session,
        )

    def coalesce(self, numPartitions: int) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` that has exactly `numPartitions` partitions.

        Coalesce does not trigger a shuffle.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        numPartitions : int
            specify the target number of partitions

        Returns
        -------
        :class:`DataFrame`
        """
        if not numPartitions > 0:
            raise ValueError("numPartitions must be positive.")
        return DataFrame.withPlan(
            plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=False),
            self._session,
        )

    def repartition(self, numPartitions: int) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` that has exactly `numPartitions` partitions.

        Repartition will shuffle source partition into partitions specified by numPartitions.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        numPartitions : int
            specify the target number of partitions

        Returns
        -------
        :class:`DataFrame`
        """
        if not numPartitions > 0:
            raise ValueError("numPartitions must be positive.")
        return DataFrame.withPlan(
            plan.Repartition(self._plan, num_partitions=numPartitions, shuffle=True),
            self._session,
        )

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "DataFrame":
        """Return a new :class:`DataFrame` with duplicate rows removed,
        optionally only deduplicating based on certain columns.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        subset : List of column names, optional
            List of columns to use for duplicate comparison (default All columns).

        Returns
        -------
        :class:`DataFrame`
            DataFrame without duplicated rows.
        """
        if subset is None:
            return DataFrame.withPlan(
                plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
            )
        else:
            return DataFrame.withPlan(
                plan.Deduplicate(child=self._plan, column_names=subset), session=self._session
            )

    drop_duplicates = dropDuplicates

    def distinct(self) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`DataFrame`
            DataFrame with distinct rows.
        """
        return DataFrame.withPlan(
            plan.Deduplicate(child=self._plan, all_columns_as_keys=True), session=self._session
        )

    def drop(self, *cols: "ColumnOrName") -> "DataFrame":
        """Returns a new :class:`DataFrame` without specified columns.
        This is a no-op if schema doesn't contain the given column name(s).

        .. versionadded:: 3.4.0

        Parameters
        ----------
        cols: str or :class:`Column`
            a name of the column, or the :class:`Column` to drop

        Returns
        -------
        :class:`DataFrame`
            DataFrame without given columns.
        """
        _cols = list(cols)
        if any(not isinstance(c, (str, Column)) for c in _cols):
            raise TypeError(
                f"'cols' must contains strings or Columns, but got {type(cols).__name__}"
            )
        if len(_cols) == 0:
            raise ValueError("'cols' must be non-empty")

        return DataFrame.withPlan(
            plan.Drop(
                child=self._plan,
                columns=_cols,
            ),
            session=self._session,
        )

    def filter(self, condition: Union[Column, str]) -> "DataFrame":
        """Filters rows using the given condition.

        :func:`where` is an alias for :func:`filter`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        condition : :class:`Column` or str
            a :class:`Column` of :class:`types.BooleanType`
            or a string of SQL expression.

        Returns
        -------
        :class:`DataFrame`
            Filtered DataFrame.
        """
        if isinstance(condition, str):
            expr = sql_expression(condition)
        else:
            expr = condition
        return DataFrame.withPlan(plan.Filter(child=self._plan, filter=expr), session=self._session)

    def first(self) -> Optional[Row]:
        """Returns the first row as a :class:`Row`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`Row`
           First row if :class:`DataFrame` is not empty, otherwise ``None``.
        """
        return self.head()

    def groupBy(self, *cols: "ColumnOrName") -> GroupedData:
        return GroupedData(self, *cols)

    @overload
    def head(self) -> Optional[Row]:
        ...

    @overload
    def head(self, n: int) -> List[Row]:
        ...

    def head(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        """Returns the first ``n`` rows.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        n : int, optional
            default 1. Number of rows to return.

        Returns
        -------
        If n is greater than 1, return a list of :class:`Row`.
        If n is 1, return a single Row.
        """
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    def take(self, num: int) -> List[Row]:
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records..

        Returns
        -------
        list
            List of rows
        """
        return self.limit(num).collect()

    # TODO: extend `on` to also be type List[Column].
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> "DataFrame":
        if self._plan is None:
            raise Exception("Cannot join when self._plan is empty.")
        if other._plan is None:
            raise Exception("Cannot join when other._plan is empty.")

        return DataFrame.withPlan(
            plan.Join(left=self._plan, right=other._plan, on=on, how=how),
            session=self._session,
        )

    def limit(self, n: int) -> "DataFrame":
        """Limits the result count to the number specified.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        :class:`DataFrame`
            Subset of the records
        """
        return DataFrame.withPlan(plan.Limit(child=self._plan, limit=n), session=self._session)

    def offset(self, n: int) -> "DataFrame":
        """Returns a new :class: `DataFrame` by skipping the first `n` rows.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        :class:`DataFrame`
            Subset of the records
        """
        return DataFrame.withPlan(plan.Offset(child=self._plan, offset=n), session=self._session)

    def tail(self, num: int) -> List[Row]:
        """
        Returns the last ``num`` rows as a :class:`list` of :class:`Row`.

        Running tail requires moving data into the application's driver process, and doing so with
        a very large ``num`` can crash the driver process with OutOfMemoryError.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        list
            List of rows
        """
        return DataFrame.withPlan(
            plan.Tail(child=self._plan, limit=num), session=self._session
        ).collect()

    def sort(self, *cols: "ColumnOrName") -> "DataFrame":
        """Sort by a specific column"""
        return DataFrame.withPlan(
            plan.Sort(self._plan, columns=list(cols), is_global=True), session=self._session
        )

    orderBy = sort

    def sortWithinPartitions(self, *cols: "ColumnOrName") -> "DataFrame":
        """Sort within each partition by a specific column"""
        return DataFrame.withPlan(
            plan.Sort(self._plan, columns=list(cols), is_global=False), session=self._session
        )

    def sample(
        self,
        fraction: float,
        *,
        withReplacement: bool = False,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        """Returns a sampled subset of this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        withReplacement : bool, optional
            Sample with replacement or not (default ``False``).
        fraction : float
            Fraction of rows to generate, range [0.0, 1.0].
        seed : int, optional
            Seed for sampling (default a random seed).

        Returns
        -------
        :class:`DataFrame`
            Sampled rows from given DataFrame.

        Notes
        -----
        This is not guaranteed to provide exactly the fraction specified of the total
        count of the given :class:`DataFrame`.

        `fraction` is required and, `withReplacement` and `seed` are optional.
        """
        if not isinstance(fraction, float):
            raise TypeError(f"'fraction' must be float, but got {type(fraction).__name__}")
        if not isinstance(withReplacement, bool):
            raise TypeError(
                f"'withReplacement' must be bool, but got {type(withReplacement).__name__}"
            )
        if seed is not None and not isinstance(seed, int):
            raise TypeError(f"'seed' must be None or int, but got {type(seed).__name__}")

        return DataFrame.withPlan(
            plan.Sample(
                child=self._plan,
                lower_bound=0.0,
                upper_bound=fraction,
                with_replacement=withReplacement,
                seed=seed,
            ),
            session=self._session,
        )

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` by renaming an existing column.
        This is a no-op if schema doesn't contain the given column name.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        existing : str
            string, name of the existing column to rename.
        new : str
            string, new name of the column.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with renamed column.
        """
        return self.withColumnsRenamed({existing: new})

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by renaming multiple columns.
        This is a no-op if schema doesn't contain the given column names.

        .. versionadded:: 3.4.0
           Added support for multiple columns renaming

        Parameters
        ----------
        colsMap : dict
            a dict of existing column names and corresponding desired column names.
            Currently, only single map is supported.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with renamed columns.

        See Also
        --------
        :meth:`withColumnRenamed`
        """
        if not isinstance(colsMap, dict):
            raise TypeError("colsMap must be dict of existing column name and new column name.")

        return DataFrame.withPlan(plan.RenameColumnsNameByName(self._plan, colsMap), self._session)

    def _show_string(
        self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False
    ) -> str:
        if not isinstance(n, int) or isinstance(n, bool):
            raise TypeError("Parameter 'n' (number of rows) must be an int")
        if not isinstance(vertical, bool):
            raise TypeError("Parameter 'vertical' must be a bool")

        _truncate: int = -1
        if isinstance(truncate, bool) and truncate:
            _truncate = 20
        else:
            try:
                _truncate = int(truncate)
            except ValueError:
                raise TypeError(
                    "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )

        pdf = DataFrame.withPlan(
            plan.ShowString(child=self._plan, numRows=n, truncate=_truncate, vertical=vertical),
            session=self._session,
        ).toPandas()
        assert pdf is not None
        return pdf["show_string"][0]

    def withColumns(self, colsMap: Dict[str, Column]) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by adding multiple columns or replacing the
        existing columns that have the same names.

        The colsMap is a map of column name and column, the column must only refer to attributes
        supplied by this Dataset. It is an error to add columns that refer to some other Dataset.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        colsMap : dict
            a dict of column name and :class:`Column`.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new or replaced columns.
        """
        if not isinstance(colsMap, dict):
            raise TypeError("colsMap must be dict of column name and column.")

        return DataFrame.withPlan(
            plan.WithColumns(self._plan, colsMap),
            session=self._session,
        )

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by adding a column or replacing the
        existing column that has the same name.

        The column expression must be an expression over this :class:`DataFrame`; attempting to add
        a column from some other :class:`DataFrame` will raise an error.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        colName : str
            string, name of the new column.
        col : :class:`Column`
            a :class:`Column` expression for the new column.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new or replaced column.
        """
        if not isinstance(col, Column):
            raise TypeError("col should be Column")
        return DataFrame.withPlan(
            plan.WithColumns(self._plan, {colName: col}),
            session=self._session,
        )

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        """
        Prints the first ``n`` rows to the console.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        n : int, optional
            Number of rows to show.
        truncate : bool or int, optional
            If set to ``True``, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
        vertical : bool, optional
            If set to ``True``, print output rows vertically (one line
            per column value).
        """
        print(self._show_string(n, truncate, vertical))

    def union(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be unioned

        Returns
        -------
        :class:`DataFrame`

        See Also
        --------
        DataFrame.unionAll

        Notes
        -----
        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        Also as standard in SQL, this function resolves columns by position (not by name).
        """
        return self.unionAll(other)

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame

        Notes
        -----
        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        Also as standard in SQL, this function resolves columns by position (not by name).

        :func:`unionAll` is an alias to :func:`union`

        See Also
        --------
        DataFrame.union
        """
        if other._plan is None:
            raise ValueError("Argument to Union does not contain a valid plan.")
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "union", is_all=True), session=self._session
        )

    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
        union (that does deduplication of elements), use this function followed by :func:`distinct`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.
        allowMissingColumns : bool, optional, default False
           Specify whether to allow missing columns.

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame.
        """
        if other._plan is None:
            raise ValueError("Argument to UnionByName does not contain a valid plan.")
        return DataFrame.withPlan(
            plan.SetOperation(
                self._plan, other._plan, "union", is_all=True, by_name=allowMissingColumns
            ),
            session=self._session,
        )

    def exceptAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame` but
        not in another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `EXCEPT ALL` in SQL.
        As standard in SQL, this function resolves columns by position (not by name).

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            The other :class:`DataFrame` to compare to.

        Returns
        -------
        :class:`DataFrame`
        """
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "except", is_all=True), session=self._session
        )

    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows only in
        both this :class:`DataFrame` and another :class:`DataFrame`.
        Note that any duplicates are removed. To preserve duplicates
        use :func:`intersectAll`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame.

        Notes
        -----
        This is equivalent to `INTERSECT` in SQL.
        """
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "intersect", is_all=False),
            session=self._session,
        )

    def intersectAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in both this :class:`DataFrame`
        and another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `INTERSECT ALL` in SQL. As standard in SQL, this function
        resolves columns by position (not by name).

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame.
        """
        return DataFrame.withPlan(
            plan.SetOperation(self._plan, other._plan, "intersect", is_all=True),
            session=self._session,
        )

    def where(self, condition: Union[Column, str]) -> "DataFrame":
        return self.filter(condition)

    @property
    def na(self) -> "DataFrameNaFunctions":
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`DataFrameNaFunctions`
        """
        return DataFrameNaFunctions(self)

    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Replace null values, alias for ``na.fill()``.
        :func:`DataFrame.fillna` and :func:`DataFrameNaFunctions.fill` are aliases of each other.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        value : int, float, string, bool or dict
            Value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, float, boolean, or string.
        subset : str, tuple or list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and cols contains a non-string column,
            then the non-string column is simply ignored.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with replaced null values.
        """
        if not isinstance(value, (float, int, str, bool, dict)):
            raise TypeError(
                f"value should be a float, int, string, bool or dict, "
                f"but got {type(value).__name__}"
            )
        if isinstance(value, dict):
            if len(value) == 0:
                raise ValueError("value dict can not be empty")
            for c, v in value.items():
                if not isinstance(c, str):
                    raise TypeError(
                        f"key type of dict should be string, but got {type(c).__name__}"
                    )
                if not isinstance(v, (bool, int, float, str)):
                    raise TypeError(
                        f"value type of dict should be float, int, string or bool, "
                        f"but got {type(v).__name__}"
                    )

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise TypeError(
                            f"cols should be a str, tuple[str] or list[str], "
                            f"but got {type(c).__name__}"
                        )
                _cols = list(subset)
            else:
                raise TypeError(
                    f"cols should be a str, tuple[str] or list[str], "
                    f"but got {type(subset).__name__}"
                )

        if isinstance(value, dict):
            _cols = list(value.keys())
            _values = [value[c] for c in _cols]
        else:
            _values = [value]

        return DataFrame.withPlan(
            plan.NAFill(child=self._plan, cols=_cols, values=_values),
            session=self._session,
        )

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` omitting rows with null values.
        :func:`DataFrame.dropna` and :func:`DataFrameNaFunctions.drop` are aliases of each other.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        how : str, optional
            'any' or 'all'.
            If 'any', drop a row if it contains any nulls.
            If 'all', drop a row only if all its values are null.
        thresh: int, optional
            default None
            If specified, drop rows that have less than `thresh` non-null values.
            This overwrites the `how` parameter.
        subset : str, tuple or list, optional
            optional list of column names to consider.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with null only rows excluded.
        """
        min_non_nulls: Optional[int] = None

        if how is not None:
            if not isinstance(how, str):
                raise TypeError(f"how should be a str, but got {type(how).__name__}")
            if how == "all":
                min_non_nulls = 1
            elif how == "any":
                min_non_nulls = None
            else:
                raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if thresh is not None:
            if not isinstance(thresh, int):
                raise TypeError(f"thresh should be a int, but got {type(thresh).__name__}")

            # 'thresh' overwrites 'how'
            min_non_nulls = thresh

        _cols: List[str] = []
        if subset is not None:
            if isinstance(subset, str):
                _cols = [subset]
            elif isinstance(subset, (tuple, list)):
                for c in subset:
                    if not isinstance(c, str):
                        raise TypeError(
                            f"cols should be a str, tuple[str] or list[str], "
                            f"but got {type(c).__name__}"
                        )
                _cols = list(subset)
            else:
                raise TypeError(
                    f"cols should be a str, tuple[str] or list[str], "
                    f"but got {type(subset).__name__}"
                )

        return DataFrame.withPlan(
            plan.NADrop(child=self._plan, cols=_cols, min_non_nulls=min_non_nulls),
            session=self._session,
        )

    def replace(
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` replacing a value with another value.
        :func:`DataFrame.replace` and :func:`DataFrameNaFunctions.replace` are
        aliases of each other.
        Values to_replace and value must have the same type and can only be numerics, booleans,
        or strings. Value can have None. When replacing, the new value will be cast
        to the type of the existing column.
        For numeric replacements all values to be replaced should have unique
        floating point representation. In case of conflicts (for example with `{42: -1, 42.0: 1}`)
        and arbitrary replacement will be used.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        to_replace : bool, int, float, string, list or dict
            Value to be replaced.
            If the value is a dict, then `value` is ignored or can be omitted, and `to_replace`
            must be a mapping between a value and a replacement.
        value : bool, int, float, string or None, optional
            The replacement value must be a bool, int, float, string or None. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.
        subset : list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with replaced values.
        """
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Helper functions
        def all_of(types: Union[Type, Tuple[Type, ...]]) -> Callable[[Iterable], bool]:
            """Given a type or tuple of types and a sequence of xs
            check if each x is instance of type(s)

            >>> all_of(bool)([True, False])
            True
            >>> all_of(str)(["a", 1])
            False
            """

            def all_of_(xs: Iterable) -> bool:
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types + (dict,)):
            raise TypeError(
                "to_replace should be a bool, float, int, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace))
            )

        if (
            not isinstance(value, valid_types)
            and value is not None
            and not isinstance(to_replace, dict)
        ):
            raise TypeError(
                "If to_replace is not a dict, value should be "
                "a bool, float, int, string, list, tuple or None. "
                "Got {0}".format(type(value))
            )

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError(
                    "to_replace and value lists should be of the same length. "
                    "Got {0} and {1}".format(len(to_replace), len(value))
                )

        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise TypeError(
                "subset should be a list or tuple of column names, "
                "column name or None. Got {0}".format(type(subset))
            )

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, cast("Iterable[Optional[Union[float, str]]]", value)))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(
            all_of_type(rep_dict.keys())
            and all_of_type(x for x in rep_dict.values() if x is not None)
            for all_of_type in [all_of_bool, all_of_str, all_of_numeric]
        ):
            raise ValueError("Mixed type replacements are not supported")

        return DataFrame.withPlan(
            plan.NAReplace(child=self._plan, cols=subset, replacements=rep_dict),
            session=self._session,
        )

    @property
    def stat(self) -> "DataFrameStatFunctions":
        """Returns a :class:`DataFrameStatFunctions` for statistic functions.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`DataFrameStatFunctions`
        """
        return DataFrameStatFunctions(self)

    def summary(self, *statistics: str) -> "DataFrame":
        """Computes specified statistics for numeric and string columns.

        .. versionadded:: 3.4.0

        Available statistics are:
        count
        mean
        stddev
        min
        max
        arbitrary approximate percentiles specified as a percentage (e.g. 75%)
        count_distinct
        approx_count_distinct

        Notes
        -----
        If no statistics are given, this function computes 'count', 'mean', 'stddev', 'min',
        'approximate quartiles' (percentiles at 25%, 50%, and 75%), and 'max'.
        This function is meant for exploratory data analysis, as we make no guarantee about the
        backward compatibility of the schema of the resulting :class:`DataFrame`. If you want to
        programmatically compute summary statistics, use the `agg` function instead.

        Parameters
        ----------
        statistics : str, list, optional
             Statistics from above list to be computed.

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame that computes specified statistics for given DataFrame.
        """
        _statistics: List[str] = list(statistics)
        for s in _statistics:
            if not isinstance(s, str):
                raise TypeError(f"'statistics' must be list[str], but got {type(s).__name__}")
        return DataFrame.withPlan(
            plan.StatSummary(child=self._plan, statistics=_statistics),
            session=self._session,
        )

    def describe(self, *cols: str) -> "DataFrame":
        """Computes basic statistics for numeric and string columns.

        .. versionadded:: 3.4.0

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.
        Use summary for expanded statistics and control over which statistics to compute.

        Parameters
        ----------
        cols : str, list, optional
             Column name or list of column names to describe by (default All columns).

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame that describes (provides statistics) given DataFrame.
        """
        _cols: List[str] = list(cols)
        for s in _cols:
            if not isinstance(s, str):
                raise TypeError(f"'cols' must be list[str], but got {type(s).__name__}")
        return DataFrame.withPlan(
            plan.StatDescribe(child=self._plan, cols=_cols),
            session=self._session,
        )

    def crosstab(self, col1: str, col2: str) -> "DataFrame":
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency
        table. The number of distinct values for each column should be less than 1e4. At most 1e6
        non-zero pair frequencies will be returned.
        The first column of each row will be the distinct values of `col1` and the column names
        will be the distinct values of `col2`. The name of the first column will be `$col1_$col2`.
        Pairs that have no occurrences will have zero as their counts.
        :func:`DataFrame.crosstab` and :func:`DataFrameStatFunctions.crosstab` are aliases.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        col1 : str
            The name of the first column. Distinct items will make the first item of
            each row.
        col2 : str
            The name of the second column. Distinct items will make the column names
            of the :class:`DataFrame`.

        Returns
        -------
        :class:`DataFrame`
            Frequency matrix of two columns.
        """
        if not isinstance(col1, str):
            raise TypeError(f"'col1' must be str, but got {type(col1).__name__}")
        if not isinstance(col2, str):
            raise TypeError(f"'col2' must be str, but got {type(col2).__name__}")
        return DataFrame.withPlan(
            plan.StatCrosstab(child=self._plan, col1=col1, col2=col2),
            session=self._session,
        )

    def _get_alias(self) -> Optional[str]:
        p = self._plan
        while p is not None:
            if isinstance(p, plan.Project) and p.alias:
                return p.alias
            p = p._child
        return None

    def __getattr__(self, name: str) -> "Column":
        return self[name]

    def __getitem__(self, name: str) -> "Column":
        # Check for alias
        alias = self._get_alias()
        if alias is not None:
            return col(alias)
        else:
            return col(name)

    def _print_plan(self) -> str:
        if self._plan:
            return self._plan.print()
        return ""

    def collect(self) -> List[Row]:
        pdf = self.toPandas()
        if pdf is not None:
            return list(pdf.apply(lambda row: Row(**row), axis=1))
        else:
            return []

    def toPandas(self) -> "pandas.DataFrame":
        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._to_pandas(query)

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`StructType`
        """
        if self._schema is None:
            if self._plan is not None:
                query = self._plan.to_proto(self._session.client)
                if self._session is None:
                    raise Exception("Cannot analyze without SparkSession.")
                self._schema = self._session.client.schema(query)
                return self._schema
            else:
                raise Exception("Empty plan.")
        else:
            return self._schema

    @property
    def isLocal(self) -> bool:
        """Returns ``True`` if the :func:`collect` and :func:`take` methods can be run locally
        (without any Spark executors).

        .. versionadded:: 3.4.0

        Returns
        -------
        bool
        """
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).is_local

    @property
    def isStreaming(self) -> bool:
        """Returns ``True`` if this :class:`DataFrame` contains one or more sources that
        continuously return data as it arrives. A :class:`DataFrame` that reads data from a
        streaming source must be executed as a :class:`StreamingQuery` using the :func:`start`
        method in :class:`DataStreamWriter`.  Methods that return a single answer, (e.g.,
        :func:`count` or :func:`collect`) will throw an :class:`AnalysisException` when there
        is a streaming source present.

        .. versionadded:: 3.4.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        bool
            Whether it's streaming DataFrame or not.
        """
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).is_streaming

    def _tree_string(self) -> str:
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).tree_string

    def printSchema(self) -> None:
        """Prints out the schema in the tree format.

        .. versionadded:: 3.4.0

        Returns
        -------
        None
        """
        print(self._tree_string())

    def inputFiles(self) -> List[str]:
        """
        Returns a best-effort snapshot of the files that compose this :class:`DataFrame`.
        This method simply asks each constituent BaseRelation for its respective files and
        takes the union of all results. Depending on the source relations, this may not find
        all input files. Duplicates are removed.

        .. versionadded:: 3.4.0

        Returns
        -------
        list
            List of file paths.
        """
        if self._plan is None:
            raise Exception("Cannot analyze on empty plan.")
        query = self._plan.to_proto(self._session.client)
        return self._session.client._analyze(query).input_files

    def toDF(self, *cols: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` that with new specified column names

        Parameters
        ----------
        *cols : tuple
            a tuple of string new column name or :class:`Column`. The length of the
            list needs to be the same as the number of columns in the initial
            :class:`DataFrame`

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new column names.
        """
        return DataFrame.withPlan(plan.RenameColumns(self._plan, list(cols)), self._session)

    def transform(self, func: Callable[..., "DataFrame"], *args: Any, **kwargs: Any) -> "DataFrame":
        """Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        func : function
            a function that takes and returns a :class:`DataFrame`.
        *args
            Positional arguments to pass to func.

        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        :class:`DataFrame`
            Transformed DataFrame.

        Examples
        --------
        >>> from pyspark.sql.connect.functions import col
        >>> df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
        >>> def cast_all_to_int(input_df):
        ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])
        >>> def sort_columns_asc(input_df):
        ...     return input_df.select(*sorted(input_df.columns))
        >>> df.transform(cast_all_to_int).transform(sort_columns_asc).show()
        +-----+---+
        |float|int|
        +-----+---+
        |    1|  1|
        |    2|  2|
        +-----+---+

        >>> def add_n(input_df, n):
        ...     return input_df.select([(col(col_name) + n).alias(col_name)
        ...                             for col_name in input_df.columns])
        >>> df.transform(add_n, 1).transform(add_n, n=10).show()
        +---+-----+
        |int|float|
        +---+-----+
        | 12| 12.0|
        | 13| 13.0|
        +---+-----+
        """
        result = func(self, *args, **kwargs)
        assert isinstance(
            result, DataFrame
        ), "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        return result

    def _explain_string(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> str:
        if extended is not None and mode is not None:
            raise ValueError("extended and mode should not be set together.")

        # For the no argument case: df.explain()
        is_no_argument = extended is None and mode is None

        # For the cases below:
        #   explain(True)
        #   explain(extended=False)
        is_extended_case = isinstance(extended, bool) and mode is None

        # For the case when extended is mode:
        #   df.explain("formatted")
        is_extended_as_mode = isinstance(extended, str) and mode is None

        # For the mode specified:
        #   df.explain(mode="formatted")
        is_mode_case = extended is None and isinstance(mode, str)

        if not (is_no_argument or is_extended_case or is_extended_as_mode or is_mode_case):
            argtypes = [str(type(arg)) for arg in [extended, mode] if arg is not None]
            raise TypeError(
                "extended (optional) and mode (optional) should be a string "
                "and bool; however, got [%s]." % ", ".join(argtypes)
            )

        # Sets an explain mode depending on a given argument
        if is_no_argument:
            explain_mode = "simple"
        elif is_extended_case:
            explain_mode = "extended" if extended else "simple"
        elif is_mode_case:
            explain_mode = cast(str, mode)
        elif is_extended_as_mode:
            explain_mode = cast(str, extended)

        if self._plan is not None:
            query = self._plan.to_proto(self._session.client)
            if self._session is None:
                raise Exception("Cannot analyze without SparkSession.")
            return self._session.client.explain_string(query, explain_mode)
        else:
            return ""

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        """Retruns plans in string for debugging purpose.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, returns only the physical plan.
            When this is a string without specifying the ``mode``, it works as the mode is
            specified.
        mode : str, optional
            specifies the expected output format of plans.

            * ``simple``: Print only a physical plan.
            * ``extended``: Print both logical and physical plans.
            * ``codegen``: Print a physical plan and generated codes if they are available.
            * ``cost``: Print a logical plan and statistics if they are available.
            * ``formatted``: Split explain output into two sections: a physical plan outline \
                and node details.
        """
        print(self._explain_string(extended=extended, mode=mode))

    def createTempView(self, name: str) -> None:
        """Creates a local temporary view with this :class:`DataFrame`.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        name : str
            Name of the view.
        """
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=False
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    def createOrReplaceTempView(self, name: str) -> None:
        """Creates or replaces a local temporary view with this :class:`DataFrame`.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        name : str
            Name of the view.
        """
        command = plan.CreateView(
            child=self._plan, name=name, is_global=False, replace=True
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    def createGlobalTempView(self, name: str) -> None:
        """Creates a global temporary view with this :class:`DataFrame`.

        The lifetime of this temporary view is tied to this Spark application.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        name : str
            Name of the view.
        """
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=False
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Creates or replaces a global temporary view using the given name.

        The lifetime of this temporary view is tied to this Spark application.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        name : str
            Name of the view.
        """
        command = plan.CreateView(
            child=self._plan, name=name, is_global=True, replace=True
        ).command(session=self._session.client)
        self._session.client.execute_command(command)

    def rdd(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("RDD Support for Spark Connect is not implemented.")

    def unpersist(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("unpersist() is not implemented.")

    def cache(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("cache() is not implemented.")

    def persist(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("persist() is not implemented.")

    def withWatermark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("withWatermark() is not implemented.")

    def observe(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("observe() is not implemented.")

    def foreach(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreach() is not implemented.")

    def foreachPartition(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("foreachPartition() is not implemented.")

    def toLocalIterator(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("toLocalIterator() is not implemented.")

    def checkpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("checkpoint() is not implemented.")

    def localCheckpoint(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("localCheckpoint() is not implemented.")

    def to_pandas_on_spark(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("to_pandas_on_spark() is not implemented.")

    def pandas_api(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("pandas_api() is not implemented.")

    def registerTempTable(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("registerTempTable() is not implemented.")

    def storageLevel(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("storageLevel() is not implemented.")

    def mapInPandas(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("mapInPandas() is not implemented.")

    def mapInArrow(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("mapInArrow() is not implemented.")

    def writeStream(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("writeStream() is not implemented.")

    def toJSON(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("toJSON() is not implemented.")

    def _repr_html_(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("_repr_html_() is not implemented.")

    def semanticHash(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("semanticHash() is not implemented.")

    def sameSemantics(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("sameSemantics() is not implemented.")


class DataFrameNaFunctions:
    """Functionality for working with missing data in :class:`DataFrame`.

    .. versionadded:: 3.4.0
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def replace(
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self.df.replace(to_replace, value, subset)

    replace.__doc__ = DataFrame.replace.__doc__


class DataFrameStatFunctions:
    """Functionality for statistic functions with :class:`DataFrame`.

    .. versionadded:: 3.4.0
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__
