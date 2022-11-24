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
)

import pandas

import pyspark.sql.connect.plan as plan
from pyspark.sql.connect.column import (
    Column,
    Expression,
    LiteralExpression,
    SQLExpression,
)
from pyspark.sql.types import (
    StructType,
    Row,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.typing import ColumnOrString, ExpressionOrString, LiteralType
    from pyspark.sql.connect.client import RemoteSparkSession

ColumnOrName = Union[Column, str]


class GroupingFrame(object):

    MeasuresType = Union[Sequence[Tuple["ExpressionOrString", str]], Dict[str, str]]
    OptMeasuresType = Optional[MeasuresType]

    def __init__(self, df: "DataFrame", *grouping_cols: Union[Column, str]) -> None:
        self._df = df
        self._grouping_cols = [x if isinstance(x, Column) else df[x] for x in grouping_cols]

    def agg(self, exprs: Optional[MeasuresType] = None) -> "DataFrame":

        # Normalize the dictionary into a list of tuples.
        if isinstance(exprs, Dict):
            measures = list(exprs.items())
        elif isinstance(exprs, List):
            measures = exprs
        else:
            measures = []

        res = DataFrame.withPlan(
            plan.Aggregate(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                measures=measures,
            ),
            session=self._df._session,
        )
        return res

    def _map_cols_to_dict(self, fun: str, cols: List[Union[Column, str]]) -> Dict[str, str]:
        return {x if isinstance(x, str) else x.name(): fun for x in cols}

    def min(self, *cols: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("min", list(cols))
        return self.agg(expr)

    def max(self, *cols: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("max", list(cols))
        return self.agg(expr)

    def sum(self, *cols: Union[Column, str]) -> "DataFrame":
        expr = self._map_cols_to_dict("sum", list(cols))
        return self.agg(expr)

    def count(self) -> "DataFrame":
        return self.agg([(LiteralExpression(1), "count")])


class DataFrame(object):
    """Every DataFrame object essentially is a Relation that is refined using the
    member functions. Calling a method on a dataframe will essentially return a copy
    of the DataFrame with the changes applied.
    """

    def __init__(
        self,
        session: "RemoteSparkSession",
        data: Optional[List[Any]] = None,
        schema: Optional[StructType] = None,
    ):
        """Creates a new data frame"""
        self._schema = schema
        self._plan: Optional[plan.LogicalPlan] = None
        self._cache: Dict[str, Any] = {}
        self._session: "RemoteSparkSession" = session

    def __repr__(self) -> str:
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    @classmethod
    def withPlan(cls, plan: plan.LogicalPlan, session: "RemoteSparkSession") -> "DataFrame":
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

    def select(self, *cols: "ExpressionOrString") -> "DataFrame":
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
                sql_expr.append(SQLExpression(element))
            else:
                sql_expr.extend([SQLExpression(e) for e in element])

        return DataFrame.withPlan(plan.Project(self._plan, *sql_expr), session=self._session)

    def agg(self, exprs: Optional[GroupingFrame.MeasuresType]) -> "DataFrame":
        return self.groupBy().agg(exprs)

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

    def sparkSession(self) -> "RemoteSparkSession":
        """Returns Spark session that created this :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`RemoteSparkSession`
        """
        return self._session

    def count(self) -> int:
        """Returns the number of rows in the data frame"""
        pdd = self.agg([(LiteralExpression(1), "count")]).toPandas()
        if pdd is None:
            raise Exception("Empty result")
        return pdd.iloc[0, 0]

    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        ...

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

    def describe(self, cols: List[Column]) -> Any:
        ...

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

    def drop(self, *cols: "ColumnOrString") -> "DataFrame":
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

    def filter(self, condition: Expression) -> "DataFrame":
        return DataFrame.withPlan(
            plan.Filter(child=self._plan, filter=condition), session=self._session
        )

    def first(self) -> Optional[Row]:
        """Returns the first row as a :class:`Row`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`Row`
           First row if :class:`DataFrame` is not empty, otherwise ``None``.
        """
        return self.head()

    def groupBy(self, *cols: "ColumnOrString") -> GroupingFrame:
        return GroupingFrame(self, *cols)

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
            or whataver number is available.

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
        on: Optional[Union[str, List[str], Column]] = None,
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
        return DataFrame.withPlan(plan.Limit(child=self._plan, limit=n), session=self._session)

    def offset(self, n: int) -> "DataFrame":
        return DataFrame.withPlan(plan.Offset(child=self._plan, offset=n), session=self._session)

    def sort(self, *cols: "ColumnOrString") -> "DataFrame":
        """Sort by a specific column"""
        return DataFrame.withPlan(
            plan.Sort(self._plan, columns=list(cols), is_global=True), session=self._session
        )

    def sortWithinPartitions(self, *cols: "ColumnOrString") -> "DataFrame":
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
        return self.unionAll(other)

    def unionAll(self, other: "DataFrame") -> "DataFrame":
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

    def where(self, condition: Expression) -> "DataFrame":
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
        _statistics: List[str] = list(statistics)
        for s in _statistics:
            if not isinstance(s, str):
                raise TypeError(f"'statistics' must be list[str], but got {type(s).__name__}")
        return DataFrame.withPlan(
            plan.StatSummary(child=self._plan, statistics=_statistics),
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
            return Column(alias)
        else:
            return Column(name)

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

    def toPandas(self) -> Optional["pandas.DataFrame"]:
        if self._plan is None:
            raise Exception("Cannot collect on empty plan.")
        if self._session is None:
            raise Exception("Cannot collect on empty session.")
        query = self._plan.to_proto(self._session)
        return self._session._to_pandas(query)

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
                query = self._plan.to_proto(self._session)
                if self._session is None:
                    raise Exception("Cannot analyze without RemoteSparkSession.")
                self._schema = self._session.schema(query)
                return self._schema
            else:
                raise Exception("Empty plan.")
        else:
            return self._schema

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

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> str:
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
            query = self._plan.to_proto(self._session)
            if self._session is None:
                raise Exception("Cannot analyze without RemoteSparkSession.")
            return self._session.explain_string(query, explain_mode)
        else:
            return ""

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
        ).command(session=self._session)
        self._session.execute_command(command)

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
        ).command(session=self._session)
        self._session.execute_command(command)


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


class DataFrameStatFunctions:
    """Functionality for statistic functions with :class:`DataFrame`.

    .. versionadded:: 3.4.0
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__
