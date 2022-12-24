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

from typing import Any, List, Optional, Sequence, Union, cast, TYPE_CHECKING, Mapping, Dict
import functools
import pyarrow as pa

from pyspark.sql.types import DataType

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import SortOrder, ColumnReference
from pyspark.sql.connect.types import pyspark_types_to_proto_types

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName
    from pyspark.sql.connect.client import SparkConnectClient


class InputValidationError(Exception):
    pass


class LogicalPlan(object):

    INDENT = 2

    def __init__(self, child: Optional["LogicalPlan"]) -> None:
        self._child = child

    def unresolved_attr(self, colName: str) -> proto.Expression:
        """Creates an unresolved attribute from a column name."""
        exp = proto.Expression()
        exp.unresolved_attribute.unparsed_identifier = colName
        return exp

    def to_attr_or_expression(
        self, col: "ColumnOrName", session: "SparkConnectClient"
    ) -> proto.Expression:
        """Returns either an instance of an unresolved attribute or the serialized
        expression value of the column."""
        if type(col) is str:
            return self.unresolved_attr(col)
        else:
            return cast(Column, col).to_plan(session)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        ...

    def command(self, session: "SparkConnectClient") -> proto.Command:
        ...

    def _verify(self, session: "SparkConnectClient") -> bool:
        """This method is used to verify that the current logical plan
        can be serialized to Proto and back and afterwards is identical."""
        plan = proto.Plan()
        plan.root.CopyFrom(self.plan(session))

        serialized_plan = plan.SerializeToString()
        test_plan = proto.Plan()
        test_plan.ParseFromString(serialized_plan)

        return test_plan == plan

    def to_proto(self, session: "SparkConnectClient", debug: bool = False) -> proto.Plan:
        """
        Generates connect proto plan based on this LogicalPlan.

        Parameters
        ----------
        session : :class:`SparkConnectClient`, optional.
            a session that connects remote spark cluster.
        debug: bool
            if enabled, the proto plan will be printed.
        """
        plan = proto.Plan()
        plan.root.CopyFrom(self.plan(session))

        if debug:
            print(plan)

        return plan

    def print(self, indent: int = 0) -> str:
        ...

    def _repr_html_(self) -> str:
        ...

    def _child_repr_(self) -> str:
        return self._child._repr_html_() if self._child is not None else ""


class DataSource(LogicalPlan):
    """A datasource with a format and optional a schema from which Spark reads data"""

    def __init__(
        self,
        format: str = "",
        schema: Optional[str] = None,
        options: Optional[Mapping[str, str]] = None,
    ) -> None:
        super().__init__(None)
        self.format = format
        self.schema = schema
        self.options = options

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = proto.Relation()
        if self.format is not None:
            plan.read.data_source.format = self.format
        if self.schema is not None:
            plan.read.data_source.schema = self.schema
        if self.options is not None:
            for k in self.options.keys():
                v = self.options.get(k)
                if v is not None:
                    plan.read.data_source.options[k] = v
        return plan

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>DataSource</b><br />
                format: {self.format}
                schema: {self.schema}
                options: {self.options}
            </li>
        </ul>
        """


class Read(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self.table_name = table_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = proto.Relation()
        plan.read.named_table.unparsed_identifier = self.table_name
        return plan

    def print(self, indent: int = 0) -> str:
        return f"{' ' * indent}<Read table_name={self.table_name}>\n"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Read</b><br />
                table name: {self.table_name}
            </li>
        </ul>
        """


class LocalRelation(LogicalPlan):
    """Creates a LocalRelation plan object based on a PyArrow Table."""

    def __init__(
        self,
        table: "pa.Table",
        schema: Optional[Union[DataType, str]] = None,
    ) -> None:
        super().__init__(None)
        assert table is not None and isinstance(table, pa.Table)
        self._table = table

        if schema is not None:
            assert isinstance(schema, (DataType, str))
        self._schema = schema

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, self._table.schema) as writer:
            for b in self._table.to_batches():
                writer.write_batch(b)

        plan = proto.Relation()
        plan.local_relation.data = sink.getvalue().to_pybytes()
        if self._schema is not None:
            if isinstance(self._schema, DataType):
                plan.local_relation.datatype.CopyFrom(pyspark_types_to_proto_types(self._schema))
            elif isinstance(self._schema, str):
                plan.local_relation.datatype_str = self._schema
        return plan

    def print(self, indent: int = 0) -> str:
        return f"{' ' * indent}<LocalRelation>\n"

    def _repr_html_(self) -> str:
        return """
        <ul>
            <li>LocalRelation</li>
        </ul>
        """


class ShowString(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], numRows: int, truncate: int, vertical: bool
    ) -> None:
        super().__init__(child)
        self.numRows = numRows
        self.truncate = truncate
        self.vertical = vertical

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.show_string.input.CopyFrom(self._child.plan(session))
        plan.show_string.numRows = self.numRows
        plan.show_string.truncate = self.truncate
        plan.show_string.vertical = self.vertical
        return plan

    def print(self, indent: int = 0) -> str:
        return (
            f"{' ' * indent}"
            f"<ShowString numRows='{self.numRows}', "
            f"truncate='{self.truncate}', "
            f"vertical='{self.vertical}'>"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>ShowString</b><br />
              NumRows: {self.numRows} <br />
              Truncate: {self.truncate} <br />
              Vertical: {self.vertical} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class Project(LogicalPlan):
    """Logical plan object for a projection.

    All input arguments are directly serialized into the corresponding protocol buffer
    objects. This class only provides very limited error handling and input validation.

    To be compatible with PySpark, we validate that the input arguments are all
    expressions to be able to serialize them to the server.

    """

    def __init__(self, child: Optional["LogicalPlan"], *columns: "ColumnOrName") -> None:
        super().__init__(child)
        self._raw_columns = list(columns)
        self.alias: Optional[str] = None
        self._verify_expressions()

    def _verify_expressions(self) -> None:
        """Ensures that all input arguments are instances of Expression or String."""
        for c in self._raw_columns:
            if not isinstance(c, (Column, str)):
                raise InputValidationError(
                    f"Only Column or String can be used for projections: '{c}'."
                )

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        proj_exprs = []
        for c in self._raw_columns:
            if isinstance(c, Column):
                proj_exprs.append(c.to_plan(session))
            elif c == "*":
                exp = proto.Expression()
                exp.unresolved_star.SetInParent()
                proj_exprs.append(exp)
            else:
                proj_exprs.append(self.unresolved_attr(c))

        plan = proto.Relation()
        plan.project.input.CopyFrom(self._child.plan(session))
        plan.project.expressions.extend(proj_exprs)
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Project cols={self._raw_columns}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Project</b><br />
                Columns: {",".join([str(c) for c in self._raw_columns])}
                {self._child._repr_html_() if self._child is not None else ""}
            </li>
        </uL>
        """


class WithColumns(LogicalPlan):
    """Logical plan object for a withColumns operation."""

    def __init__(self, child: Optional["LogicalPlan"], cols_map: Mapping[str, Column]) -> None:
        super().__init__(child)
        self._cols_map = cols_map

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.with_columns.input.CopyFrom(self._child.plan(session))
        for k, v in self._cols_map.items():
            name_expr = proto.Expression.Alias()
            name_expr.name.append(k)
            name_expr.expr.CopyFrom(v.to_plan(session))
            plan.with_columns.name_expr_list.append(name_expr)
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<WithColumns cols={self._cols_map}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>WithColumns</b><br />
                Column Map: {self._cols_map}
                {self._child._repr_html_() if self._child is not None else ""}
            </li>
        </uL>
        """


class Hint(LogicalPlan):
    """Logical plan object for a Hint operation."""

    def __init__(self, child: Optional["LogicalPlan"], name: str, params: List[Any]) -> None:
        super().__init__(child)
        self.name = name
        self.params = params

    def _convert_value(self, v: Any) -> proto.Expression.Literal:
        value = proto.Expression.Literal()
        if v is None:
            value.null = True
        elif isinstance(v, int):
            value.integer = v
        elif isinstance(v, str):
            value.string = v
        else:
            raise ValueError(f"Could not convert literal for type {type(v)}")
        return value

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.hint.input.CopyFrom(self._child.plan(session))
        plan.hint.name = self.name
        for v in self.params:
            plan.hint.parameters.append(self._convert_value(v))
        return plan

    def print(self, indent: int = 0) -> str:
        return f"""{" " * indent}<Hint name='{self.name}', parameters='{self.params}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>Hint</b><br />
              name: {self.name} <br />
              parameters: {self.params} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class Filter(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], filter: Column) -> None:
        super().__init__(child)
        self.filter = filter

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.filter.input.CopyFrom(self._child.plan(session))
        plan.filter.condition.CopyFrom(self.filter.to_plan(session))
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Filter filter={self.filter}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Filter</b><br />
                Condition: {self.filter}
                {self._child_repr_()}
            </li>
        </uL>
        """


class Limit(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], limit: int) -> None:
        super().__init__(child)
        self.limit = limit

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.limit.input.CopyFrom(self._child.plan(session))
        plan.limit.limit = self.limit
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Limit limit={self.limit}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Limit</b><br />
                Limit: {self.limit} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Tail(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], limit: int) -> None:
        super().__init__(child)
        self.limit = limit

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.tail.input.CopyFrom(self._child.plan(session))
        plan.tail.limit = self.limit
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Tail limit={self.limit}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Tail</b><br />
                Limit: {self.limit} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Offset(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], offset: int = 0) -> None:
        super().__init__(child)
        self.offset = offset

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.offset.input.CopyFrom(self._child.plan(session))
        plan.offset.offset = self.offset
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Offset={self.offset}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Limit</b><br />
                Offset: {self.offset} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Deduplicate(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        all_columns_as_keys: bool = False,
        column_names: Optional[List[str]] = None,
    ) -> None:
        super().__init__(child)
        self.all_columns_as_keys = all_columns_as_keys
        self.column_names = column_names

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.deduplicate.input.CopyFrom(self._child.plan(session))
        plan.deduplicate.all_columns_as_keys = self.all_columns_as_keys
        if self.column_names is not None:
            plan.deduplicate.column_names.extend(self.column_names)
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{' ' * indent}<all_columns_as_keys={self.all_columns_as_keys} "
            f"column_names={self.column_names}>\n{c_buf}"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b></b>Deduplicate<br />
                all_columns_as_keys: {self.all_columns_as_keys} <br />
                column_names: {self.column_names} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Sort(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columns: List["ColumnOrName"],
        is_global: bool,
    ) -> None:
        super().__init__(child)
        self.columns = columns
        self.is_global = is_global

    def _convert_col(
        self, col: "ColumnOrName", session: "SparkConnectClient"
    ) -> proto.Expression.SortOrder:
        sort: Optional[SortOrder] = None
        if isinstance(col, Column):
            if isinstance(col._expr, SortOrder):
                sort = col._expr
            else:
                sort = SortOrder(col._expr)
        else:
            sort = SortOrder(ColumnReference(name=col))
        assert sort is not None

        return sort.to_plan(session).sort_order

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.sort.input.CopyFrom(self._child.plan(session))
        plan.sort.order.extend([self._convert_col(c, session) for c in self.columns])
        plan.sort.is_global = self.is_global
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Sort columns={self.columns}, global={self.is_global}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Sort</b><br />
                {", ".join([str(c) for c in self.columns])}
                global: {self.is_global} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Drop(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columns: List[Union[Column, str]],
    ) -> None:
        super().__init__(child)
        assert len(columns) > 0 and all(isinstance(c, (Column, str)) for c in columns)
        self.columns = columns

    def _convert_to_expr(
        self, col: Union[Column, str], session: "SparkConnectClient"
    ) -> proto.Expression:
        expr = proto.Expression()
        if isinstance(col, Column):
            expr.CopyFrom(col.to_plan(session))
        else:
            expr.CopyFrom(self.unresolved_attr(col))
        return expr

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.drop.input.CopyFrom(self._child.plan(session))
        plan.drop.cols.extend([self._convert_to_expr(c, session) for c in self.columns])
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<Drop columns={self.columns}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Drop</b><br />
                columns: {self.columns} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Sample(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        lower_bound: float,
        upper_bound: float,
        with_replacement: bool,
        seed: Optional[int],
        force_stable_sort: bool = False,
    ) -> None:
        super().__init__(child)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.with_replacement = with_replacement
        self.seed = seed
        self.force_stable_sort = force_stable_sort

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.sample.input.CopyFrom(self._child.plan(session))
        plan.sample.lower_bound = self.lower_bound
        plan.sample.upper_bound = self.upper_bound
        plan.sample.with_replacement = self.with_replacement
        if self.seed is not None:
            plan.sample.seed = self.seed
        plan.sample.force_stable_sort = self.force_stable_sort
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{' ' * indent}"
            f"<Sample lowerBound={self.lower_bound}, upperBound={self.upper_bound}, "
            f"withReplacement={self.with_replacement}, seed={self.seed}>"
            f"\n{c_buf}"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Sample</b><br />
                LowerBound: {self.lower_bound} <br />
                UpperBound: {self.upper_bound} <br />
                WithReplacement: {self.with_replacement} <br />
                Seed: {self.seed} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Aggregate(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        group_type: str,
        grouping_cols: Sequence[Column],
        aggregate_cols: Sequence[Column],
        pivot_col: Optional[Column],
        pivot_values: Optional[Sequence[Any]],
    ) -> None:
        super().__init__(child)

        assert isinstance(group_type, str) and group_type in ["groupby", "rollup", "cube", "pivot"]
        self._group_type = group_type

        assert isinstance(grouping_cols, list) and all(isinstance(c, Column) for c in grouping_cols)
        self._grouping_cols = grouping_cols

        assert isinstance(aggregate_cols, list) and all(
            isinstance(c, Column) for c in aggregate_cols
        )
        self._aggregate_cols = aggregate_cols

        if group_type == "pivot":
            assert pivot_col is not None and isinstance(pivot_col, Column)
            assert pivot_values is None or isinstance(pivot_values, list)
        else:
            assert pivot_col is None
            assert pivot_values is None

        self._pivot_col = pivot_col
        self._pivot_values = pivot_values

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        from pyspark.sql.connect.functions import lit

        assert self._child is not None

        agg = proto.Relation()

        agg.aggregate.input.CopyFrom(self._child.plan(session))

        agg.aggregate.grouping_expressions.extend([c.to_plan(session) for c in self._grouping_cols])
        agg.aggregate.aggregate_expressions.extend(
            [c.to_plan(session) for c in self._aggregate_cols]
        )

        if self._group_type == "groupby":
            agg.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY
        elif self._group_type == "rollup":
            agg.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP
        elif self._group_type == "cube":
            agg.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_CUBE
        elif self._group_type == "pivot":
            agg.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_PIVOT
            assert self._pivot_col is not None
            agg.aggregate.pivot.col.CopyFrom(self._pivot_col.to_plan(session))
            if self._pivot_values is not None and len(self._pivot_values) > 0:
                agg.aggregate.pivot.values.extend(
                    [lit(v).to_plan(session).literal for v in self._pivot_values]
                )

        return agg

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{' ' * indent}<Groupby={self._grouping_cols}"
            f"Aggregate={self._aggregate_cols}>\n{c_buf}"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Aggregation</b><br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class Join(LogicalPlan):
    def __init__(
        self,
        left: Optional["LogicalPlan"],
        right: "LogicalPlan",
        on: Optional[Union[str, List[str], Column, List[Column]]],
        how: Optional[str],
    ) -> None:
        super().__init__(left)
        self.left = cast(LogicalPlan, left)
        self.right = right
        self.on = on
        if how is None:
            join_type = proto.Join.JoinType.JOIN_TYPE_INNER
        elif how == "inner":
            join_type = proto.Join.JoinType.JOIN_TYPE_INNER
        elif how in ["outer", "full", "fullouter"]:
            join_type = proto.Join.JoinType.JOIN_TYPE_FULL_OUTER
        elif how in ["leftouter", "left"]:
            join_type = proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER
        elif how in ["rightouter", "right"]:
            join_type = proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
        elif how in ["leftsemi", "semi"]:
            join_type = proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI
        elif how in ["leftanti", "anti"]:
            join_type = proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI
        elif how == "cross":
            join_type = proto.Join.JoinType.JOIN_TYPE_CROSS
        else:
            raise NotImplementedError(
                """
                Unsupported join type: %s. Supported join types include:
                "inner", "outer", "full", "fullouter", "full_outer",
                "leftouter", "left", "left_outer", "rightouter",
                "right", "right_outer", "leftsemi", "left_semi",
                "semi", "leftanti", "left_anti", "anti", "cross",
                """
                % how
            )
        self.how = join_type

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        rel = proto.Relation()
        rel.join.left.CopyFrom(self.left.plan(session))
        rel.join.right.CopyFrom(self.right.plan(session))
        if self.on is not None:
            if not isinstance(self.on, list):
                if isinstance(self.on, str):
                    rel.join.using_columns.append(self.on)
                else:
                    rel.join.join_condition.CopyFrom(self.to_attr_or_expression(self.on, session))
            elif len(self.on) > 0:
                if isinstance(self.on[0], str):
                    rel.join.using_columns.extend(cast(str, self.on))
                else:
                    merge_column = functools.reduce(lambda c1, c2: c1 & c2, self.on)
                    rel.join.join_condition.CopyFrom(cast(Column, merge_column).to_plan(session))
        rel.join.join_type = self.how
        return rel

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        o = " " * (indent + LogicalPlan.INDENT)
        n = indent + LogicalPlan.INDENT * 2
        return (
            f"{i}<Join on={self.on} how={self.how}>\n{o}"
            f"left=\n{self.left.print(n)}\n{o}right=\n{self.right.print(n)}"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Join</b><br />
                Left: {self.left._repr_html_()}
                Right: {self.right._repr_html_()}
            </li>
        </uL>
        """


class SetOperation(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        other: Optional["LogicalPlan"],
        set_op: str,
        is_all: bool = True,
        by_name: bool = False,
    ) -> None:
        super().__init__(child)
        self.other = other
        self.by_name = by_name
        self.is_all = is_all
        self.set_op = set_op

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        rel = proto.Relation()
        if self._child is not None:
            rel.set_op.left_input.CopyFrom(self._child.plan(session))
        if self.other is not None:
            rel.set_op.right_input.CopyFrom(self.other.plan(session))
        if self.set_op == "union":
            rel.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_UNION
        elif self.set_op == "intersect":
            rel.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_INTERSECT
        elif self.set_op == "except":
            rel.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_EXCEPT
        else:
            raise NotImplementedError(
                """
                Unsupported set operation type: %s.
                """
                % rel.set_op.set_op_type
            )

        rel.set_op.is_all = self.is_all
        rel.set_op.by_name = self.by_name
        return rel

    def print(self, indent: int = 0) -> str:
        assert self._child is not None
        assert self.other is not None

        i = " " * indent
        o = " " * (indent + LogicalPlan.INDENT)
        n = indent + LogicalPlan.INDENT * 2
        return (
            f"{i}SetOperation\n{o}child1=\n{self._child.print(n)}"
            f"\n{o}child2=\n{self.other.print(n)}"
        )

    def _repr_html_(self) -> str:
        assert self._child is not None
        assert self.other is not None

        return f"""
        <ul>
            <li>
                <b>SetOperation</b><br />
                Left: {self._child._repr_html_()}
                Right: {self.other._repr_html_()}
            </li>
        </uL>
        """


class Repartition(LogicalPlan):
    """Repartition Relation into a different number of partitions."""

    def __init__(self, child: Optional["LogicalPlan"], num_partitions: int, shuffle: bool) -> None:
        super().__init__(child)
        self._num_partitions = num_partitions
        self._shuffle = shuffle

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        rel = proto.Relation()
        if self._child is not None:
            rel.repartition.input.CopyFrom(self._child.plan(session))
        rel.repartition.shuffle = self._shuffle
        rel.repartition.num_partitions = self._num_partitions
        return rel

    def print(self, indent: int = 0) -> str:
        plan_name = "repartition" if self._shuffle else "coalesce"
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<{plan_name} num_partitions={self._num_partitions}>\n{c_buf}"

    def _repr_html_(self) -> str:
        plan_name = "repartition" if self._shuffle else "coalesce"
        return f"""
        <ul>
           <li>
              <b>{plan_name}</b><br />
              Child: {self._child_repr_()}
              num_partitions: {self._num_partitions}
           </li>
        </ul>
        """


class SubqueryAlias(LogicalPlan):
    """Alias for a relation."""

    def __init__(self, child: Optional["LogicalPlan"], alias: str) -> None:
        super().__init__(child)
        self._alias = alias

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        rel = proto.Relation()
        if self._child is not None:
            rel.subquery_alias.input.CopyFrom(self._child.plan(session))
        rel.subquery_alias.alias = self._alias
        return rel

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{' ' * indent}<SubqueryAlias alias={self._alias}>\n{c_buf}"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>SubqueryAlias</b><br />
              Child: {self._child_repr_()}
              Alias: {self._alias}
           </li>
        </ul>
        """


class SQL(LogicalPlan):
    def __init__(self, query: str) -> None:
        super().__init__(None)
        self._query = query

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        rel = proto.Relation()
        rel.sql.query = self._query
        return rel

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        sub_query = self._query.replace("\n", "")[:50]
        return f"""{i}<SQL query='{sub_query}...'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>SQL</b><br />
              Statement: <pre>{self._query}</pre>
           </li>
        </ul>
        """


class Range(LogicalPlan):
    def __init__(
        self,
        start: int,
        end: int,
        step: int,
        num_partitions: Optional[int] = None,
    ) -> None:
        super().__init__(None)
        self._start = start
        self._end = end
        self._step = step
        self._num_partitions = num_partitions

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        rel = proto.Relation()
        rel.range.start = self._start
        rel.range.end = self._end
        rel.range.step = self._step
        if self._num_partitions is not None:
            rel.range.num_partitions = self._num_partitions
        return rel

    def print(self, indent: int = 0) -> str:
        return (
            f"{' ' * indent}"
            f"<Range start={self._start}, end={self._end}, "
            f"step={self._step}, num_partitions={self._num_partitions}>"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Range</b><br />
                Start: {self._start} <br />
                End: {self._end} <br />
                Step: {self._step} <br />
                NumPartitions: {self._num_partitions} <br />
                {self._child_repr_()}
            </li>
        </uL>
        """


class ToSchema(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], schema: DataType) -> None:
        super().__init__(child)
        self._schema = schema

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None

        plan = proto.Relation()
        plan.to_schema.input.CopyFrom(self._child.plan(session))
        plan.to_schema.schema.CopyFrom(pyspark_types_to_proto_types(self._schema))
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<ToSchema schema='{self._schema}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>ToSchema</b><br />
              schema: {self._schema} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class RenameColumnsNameByName(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], colsMap: Mapping[str, str]) -> None:
        super().__init__(child)
        self._colsMap = colsMap

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None

        plan = proto.Relation()
        plan.rename_columns_by_name_to_name_map.input.CopyFrom(self._child.plan(session))
        for k, v in self._colsMap.items():
            plan.rename_columns_by_name_to_name_map.rename_columns_map[k] = v
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<RenameColumnsNameByName ColsMap='{self._colsMap}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>RenameColumns</b><br />
              ColsMap: {self._colsMap} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class Unpivot(LogicalPlan):
    """Logical plan object for a unpivot operation."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        ids: List["ColumnOrName"],
        values: List["ColumnOrName"],
        variable_column_name: str,
        value_column_name: str,
    ) -> None:
        super().__init__(child)
        self.ids = ids
        self.values = values
        self.variable_column_name = variable_column_name
        self.value_column_name = value_column_name

    def col_to_expr(self, col: "ColumnOrName", session: "SparkConnectClient") -> proto.Expression:
        if isinstance(col, Column):
            return col.to_plan(session)
        else:
            return self.unresolved_attr(col)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None

        plan = proto.Relation()
        plan.unpivot.input.CopyFrom(self._child.plan(session))
        plan.unpivot.ids.extend([self.col_to_expr(x, session) for x in self.ids])
        plan.unpivot.values.extend([self.col_to_expr(x, session) for x in self.values])
        plan.unpivot.variable_column_name = self.variable_column_name
        plan.unpivot.value_column_name = self.value_column_name
        return plan

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{' ' * indent}"
            f"<Unpivot ids={self.ids}, values={self.values}, "
            f"variable_column_name={self.variable_column_name}, "
            f"value_column_name={self.value_column_name}>"
            f"\n{c_buf}"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
            <li>
                <b>Unpivot</b><br />
                ids: {self.ids}
                values: {self.values}
                variable_column_name: {self.variable_column_name}
                value_column_name: {self.value_column_name}
                {self._child._repr_html_() if self._child is not None else ""}
            </li>
        </uL>
        """


class NAFill(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], cols: Optional[List[str]], values: List[Any]
    ) -> None:
        super().__init__(child)

        assert (
            isinstance(values, list)
            and len(values) > 0
            and all(isinstance(v, (bool, int, float, str)) for v in values)
        )

        if cols is not None and len(cols) > 0:
            assert isinstance(cols, list) and all(isinstance(c, str) for c in cols)
            if len(values) > 1:
                assert len(cols) == len(values)

        self.cols = cols
        self.values = values

    def _convert_value(self, v: Any) -> proto.Expression.Literal:
        value = proto.Expression.Literal()
        if isinstance(v, bool):
            value.boolean = v
        elif isinstance(v, int):
            value.long = v
        elif isinstance(v, float):
            value.double = v
        else:
            value.string = v
        return value

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.fill_na.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.fill_na.cols.extend(self.cols)
        plan.fill_na.values.extend([self._convert_value(v) for v in self.values])
        return plan

    def print(self, indent: int = 0) -> str:
        return f"""{" " * indent}<NAFill cols='{self.cols}', values='{self.values}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>NAFill</b><br />
              Cols: {self.cols} <br />
              Values: {self.values} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class NADrop(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        cols: Optional[List[str]],
        min_non_nulls: Optional[int],
    ) -> None:
        super().__init__(child)

        self.cols = cols
        self.min_non_nulls = min_non_nulls

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.drop_na.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.drop_na.cols.extend(self.cols)
        if self.min_non_nulls is not None:
            plan.drop_na.min_non_nulls = self.min_non_nulls
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"{i}" f"<NADrop cols='{self.cols}' " f"min_non_nulls='{self.min_non_nulls}'>"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>NADrop</b><br />
              Cols: {self.cols} <br />
              Min_non_nulls: {self.min_non_nulls} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class NAReplace(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        cols: Optional[List[str]],
        replacements: Dict[Any, Any],
    ) -> None:
        super().__init__(child)

        for old_value, new_value in replacements.items():
            if old_value is not None:
                assert isinstance(old_value, (bool, int, float, str))
            if new_value is not None:
                assert isinstance(new_value, (bool, int, float, str))

        self.cols = cols
        self.replacements = replacements

    def _convert_value(self, v: Any) -> proto.Expression.Literal:
        value = proto.Expression.Literal()
        if v is None:
            value.null = True
        elif isinstance(v, bool):
            value.boolean = v
        elif isinstance(v, (int, float)):
            value.double = float(v)
        else:
            value.string = v
        return value

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.replace.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.replace.cols.extend(self.cols)
        if len(self.replacements) > 0:
            for old_value, new_value in self.replacements.items():
                replacement = proto.NAReplace.Replacement()
                replacement.old_value.CopyFrom(self._convert_value(old_value))
                replacement.new_value.CopyFrom(self._convert_value(new_value))
                plan.replace.replacements.append(replacement)
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"{i}" f"<NAReplace cols='{self.cols}' " f"replacements='{self.replacements}'>"

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>NADrop</b><br />
              Cols: {self.cols} <br />
              Replacements: {self.replacements} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class StatSummary(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], statistics: List[str]) -> None:
        super().__init__(child)
        self.statistics = statistics

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.summary.input.CopyFrom(self._child.plan(session))
        plan.summary.statistics.extend(self.statistics)
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<Summary statistics='{self.statistics}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>Summary</b><br />
              Statistics: {self.statistics} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class StatDescribe(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], cols: List[str]) -> None:
        super().__init__(child)
        self.cols = cols

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.describe.input.CopyFrom(self._child.plan(session))
        plan.describe.cols.extend(self.cols)
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<Describe cols='{self.cols}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>Describe</b><br />
              Cols: {self.cols} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class StatCrosstab(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], col1: str, col2: str) -> None:
        super().__init__(child)
        self.col1 = col1
        self.col2 = col2

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None

        plan = proto.Relation()
        plan.crosstab.input.CopyFrom(self._child.plan(session))
        plan.crosstab.col1 = self.col1
        plan.crosstab.col2 = self.col2
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<Crosstab col1='{self.col1}' col2='{self.col2}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>Crosstab</b><br />
              Col1: {self.col1} <br />
              Col2: {self.col2} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class RenameColumns(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], cols: Sequence[str]) -> None:
        super().__init__(child)
        self._cols = cols

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None

        plan = proto.Relation()
        plan.rename_columns_by_same_length_names.input.CopyFrom(self._child.plan(session))
        plan.rename_columns_by_same_length_names.column_names.extend(self._cols)
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return f"""{i}<RenameColumns cols='{self._cols}'>"""

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>RenameColumns</b><br />
              cols: {self._cols} <br />
              {self._child_repr_()}
           </li>
        </ul>
        """


class CreateView(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], name: str, is_global: bool, replace: bool
    ) -> None:
        super().__init__(child)
        self._name = name
        self._is_gloal = is_global
        self._replace = replace

    def command(self, session: "SparkConnectClient") -> proto.Command:
        assert self._child is not None

        plan = proto.Command()
        plan.create_dataframe_view.replace = self._replace
        plan.create_dataframe_view.is_global = self._is_gloal
        plan.create_dataframe_view.name = self._name
        plan.create_dataframe_view.input.CopyFrom(self._child.plan(session))
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return (
            f"{i}"
            f"<CreateView name='{self._name}' "
            f"is_global='{self._is_gloal} "
            f"replace='{self._replace}'>"
        )

    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>CreateView</b><br />
              name: {self._name} <br />
              is_global: {self._is_gloal} <br />
              replace: {self._replace} <br />
            {self._child_repr_()}
           </li>
        </ul>
        """


class WriteOperation(LogicalPlan):
    def __init__(self, child: "LogicalPlan") -> None:
        super(WriteOperation, self).__init__(child)
        self.source: Optional[str] = None
        self.path: Optional[str] = None
        self.table_name: Optional[str] = None
        self.mode: Optional[str] = None
        self.sort_cols: List[str] = []
        self.partitioning_cols: List[str] = []
        self.options: dict[str, Optional[str]] = {}
        self.num_buckets: int = -1
        self.bucket_cols: List[str] = []

    def command(self, session: "SparkConnectClient") -> proto.Command:
        assert self._child is not None
        plan = proto.Command()
        plan.write_operation.input.CopyFrom(self._child.plan(session))
        if self.source is not None:
            plan.write_operation.source = self.source
        plan.write_operation.sort_column_names.extend(self.sort_cols)
        plan.write_operation.partitioning_columns.extend(self.partitioning_cols)

        if self.num_buckets > 0:
            plan.write_operation.bucket_by.bucket_column_names.extend(self.bucket_cols)
            plan.write_operation.bucket_by.num_buckets = self.num_buckets

        for k in self.options:
            if self.options[k] is None:
                del plan.write_operation.options[k]
            else:
                plan.write_operation.options[k] = cast(str, self.options[k])

        if self.table_name is not None:
            plan.write_operation.table_name = self.table_name
        elif self.path is not None:
            plan.write_operation.path = self.path
        else:
            raise AssertionError(
                "Invalid configuration of WriteCommand, neither path or table_name present."
            )

        if self.mode is not None:
            wm = self.mode.lower()
            if wm == "append":
                plan.write_operation.mode = proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
            elif wm == "overwrite":
                plan.write_operation.mode = proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
            elif wm == "error":
                plan.write_operation.mode = proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
            elif wm == "ignore":
                plan.write_operation.mode = proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
            else:
                raise ValueError(f"Unknown SaveMode value for DataFrame: {self.mode}")
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return (
            f"{i}"
            f"<WriteOperation source='{self.source}' "
            f"path='{self.path} "
            f"table_name='{self.table_name}' "
            f"mode='{self.mode}' "
            f"sort_cols='{self.sort_cols}' "
            f"partitioning_cols='{self.partitioning_cols}' "
            f"num_buckets='{self.num_buckets}' "
            f"bucket_cols='{self.bucket_cols}' "
            f"options='{self.options}'>"
        )

    def _repr_html_(self) -> str:
        return (
            f"<uL><li>WriteOperation <br />source='{self.source}'<br />"
            f"path: '{self.path}<br />"
            f"table_name: '{self.table_name}' <br />"
            f"mode: '{self.mode}' <br />"
            f"sort_cols: '{self.sort_cols}' <br />"
            f"partitioning_cols: '{self.partitioning_cols}' <br />"
            f"num_buckets: '{self.num_buckets}' <br />"
            f"bucket_cols: '{self.bucket_cols}' <br />"
            f"options: '{self.options}'<br />"
            f"</li></ul>"
        )
        pass
