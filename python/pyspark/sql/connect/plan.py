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
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
    Mapping,
)

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.column import (
    Column,
    Expression,
    SortOrder,
)


if TYPE_CHECKING:
    from pyspark.sql.connect.typing import ColumnOrString, ExpressionOrString
    from pyspark.sql.connect.client import RemoteSparkSession


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
        self, col: "ColumnOrString", session: "RemoteSparkSession"
    ) -> proto.Expression:
        """Returns either an instance of an unresolved attribute or the serialized
        expression value of the column."""
        if type(col) is str:
            return self.unresolved_attr(col)
        else:
            return cast(Column, col).to_plan(session)

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        ...

    def command(self, session: "RemoteSparkSession") -> proto.Command:
        ...

    def _verify(self, session: "RemoteSparkSession") -> bool:
        """This method is used to verify that the current logical plan
        can be serialized to Proto and back and afterwards is identical."""
        plan = proto.Plan()
        plan.root.CopyFrom(self.plan(session))

        serialized_plan = plan.SerializeToString()
        test_plan = proto.Plan()
        test_plan.ParseFromString(serialized_plan)

        return test_plan == plan

    def to_proto(self, session: "RemoteSparkSession", debug: bool = False) -> proto.Plan:
        """
        Generates connect proto plan based on this LogicalPlan.

        Parameters
        ----------
        session : :class:`RemoteSparkSession`, optional.
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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


class ShowString(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], numRows: int, truncate: int, vertical: bool
    ) -> None:
        super().__init__(child)
        self.numRows = numRows
        self.truncate = truncate
        self.vertical = vertical

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def __init__(self, child: Optional["LogicalPlan"], *columns: "ExpressionOrString") -> None:
        super().__init__(child)
        self._raw_columns = list(columns)
        self.alias: Optional[str] = None
        self._verify_expressions()

    def _verify_expressions(self) -> None:
        """Ensures that all input arguments are instances of Expression or String."""
        for c in self._raw_columns:
            if not isinstance(c, (Expression, str)):
                raise InputValidationError(
                    f"Only Expressions or String can be used for projections: '{c}'."
                )

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        proj_exprs = []
        for c in self._raw_columns:
            if isinstance(c, Expression):
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


class Filter(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], filter: Expression) -> None:
        super().__init__(child)
        self.filter = filter

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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


class Offset(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], offset: int = 0) -> None:
        super().__init__(child)
        self.offset = offset

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
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
        columns: List[Union[SortOrder, Column, str]],
        is_global: bool,
    ) -> None:
        super().__init__(child)
        self.columns = columns
        self.is_global = is_global

    def col_to_sort_field(
        self, col: Union[SortOrder, Column, str], session: "RemoteSparkSession"
    ) -> proto.Sort.SortField:
        if isinstance(col, SortOrder):
            sf = proto.Sort.SortField()
            sf.expression.CopyFrom(col.ref.to_plan(session))
            sf.direction = (
                proto.Sort.SortDirection.SORT_DIRECTION_ASCENDING
                if col.ascending
                else proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING
            )
            sf.nulls = (
                proto.Sort.SortNulls.SORT_NULLS_FIRST
                if not col.nullsLast
                else proto.Sort.SortNulls.SORT_NULLS_LAST
            )
            return sf
        else:
            sf = proto.Sort.SortField()
            # Check string
            if isinstance(col, Column):
                sf.expression.CopyFrom(col.to_plan(session))
            else:
                sf.expression.CopyFrom(self.unresolved_attr(col))
            sf.direction = proto.Sort.SortDirection.SORT_DIRECTION_ASCENDING
            sf.nulls = proto.Sort.SortNulls.SORT_NULLS_LAST
            return sf

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.sort.input.CopyFrom(self._child.plan(session))
        plan.sort.sort_fields.extend([self.col_to_sort_field(x, session) for x in self.columns])
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
        self, col: Union[Column, str], session: "RemoteSparkSession"
    ) -> proto.Expression:
        expr = proto.Expression()
        if isinstance(col, Column):
            expr.CopyFrom(col.to_plan(session))
        else:
            expr.CopyFrom(self.unresolved_attr(col))
        return expr

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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
    ) -> None:
        super().__init__(child)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.with_replacement = with_replacement
        self.seed = seed

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.sample.input.CopyFrom(self._child.plan(session))
        plan.sample.lower_bound = self.lower_bound
        plan.sample.upper_bound = self.upper_bound
        plan.sample.with_replacement = self.with_replacement
        if self.seed is not None:
            plan.sample.seed = self.seed
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
    MeasureType = Tuple["ExpressionOrString", str]
    MeasuresType = Sequence[MeasureType]
    OptMeasuresType = Optional[MeasuresType]

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        grouping_cols: List[Column],
        measures: OptMeasuresType,
    ) -> None:
        super().__init__(child)
        self.grouping_cols = grouping_cols
        self.measures = measures if measures is not None else []

    def _convert_measure(self, m: MeasureType, session: "RemoteSparkSession") -> proto.Expression:
        exp, fun = m
        proto_expr = proto.Expression()
        measure = proto_expr.unresolved_function
        measure.parts.append(fun)
        if type(exp) is str:
            measure.arguments.append(self.unresolved_attr(exp))
        else:
            measure.arguments.append(cast(Expression, exp).to_plan(session))
        return proto_expr

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        groupings = [x.to_plan(session) for x in self.grouping_cols]

        agg = proto.Relation()
        agg.aggregate.input.CopyFrom(self._child.plan(session))
        agg.aggregate.result_expressions.extend(
            list(map(lambda x: self._convert_measure(x, session), self.measures))
        )

        agg.aggregate.grouping_expressions.extend(groupings)
        return agg

    def print(self, indent: int = 0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{' ' * indent}<Sort columns={self.grouping_cols}"
            f"measures={self.measures}>\n{c_buf}"
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
        on: Optional[Union[str, List[str], Column]],
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
        else:
            raise NotImplementedError(
                """
                Unsupported join type: %s. Supported join types include:
                "inner", "outer", "full", "fullouter", "full_outer",
                "leftouter", "left", "left_outer", "rightouter",
                "right", "right_outer", "leftsemi", "left_semi",
                "semi", "leftanti", "left_anti", "anti",
                """
                % how
            )
        self.how = join_type

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        rel = proto.Relation()
        rel.join.left.CopyFrom(self.left.plan(session))
        rel.join.right.CopyFrom(self.right.plan(session))
        if self.on is not None:
            if not isinstance(self.on, list):
                if isinstance(self.on, str):
                    rel.join.using_columns.append(self.on)
                else:
                    rel.join.join_condition.CopyFrom(self.to_attr_or_expression(self.on, session))
            else:
                rel.join.using_columns.extend(self.on)
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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
            value.i64 = v
        elif isinstance(v, float):
            value.fp64 = v
        else:
            value.string = v
        return value

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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


class StatSummary(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], statistics: List[str]) -> None:
        super().__init__(child)
        self.statistics = statistics

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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


class StatCrosstab(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], col1: str, col2: str) -> None:
        super().__init__(child)
        self.col1 = col1
        self.col2 = col2

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
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


class CreateView(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], name: str, is_global: bool, replace: bool
    ) -> None:
        super().__init__(child)
        self._name = name
        self._is_gloal = is_global
        self._replace = replace

    def command(self, session: "RemoteSparkSession") -> proto.Command:
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
