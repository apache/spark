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

# mypy: disable-error-code="operator"

from pyspark.resource import ResourceProfile
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import (
    Any,
    List,
    Optional,
    Type,
    Sequence,
    Union,
    cast,
    TYPE_CHECKING,
    Mapping,
    Dict,
    Tuple,
)
import functools
import json
import pickle
from threading import Lock
from inspect import signature, isclass

import pyarrow as pa

from pyspark.serializers import CloudPickleSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import DataType

import pyspark.sql.connect.proto as proto
from pyspark.sql.column import Column
from pyspark.sql.connect.conversion import storage_level_to_proto
from pyspark.sql.connect.expressions import Expression
from pyspark.sql.connect.types import pyspark_types_to_proto_types, UnparsedDataType
from pyspark.errors import (
    PySparkValueError,
    PySparkPicklingError,
    IllegalArgumentException,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.udf import UserDefinedFunction
    from pyspark.sql.connect.observation import Observation


class LogicalPlan:
    _lock: Lock = Lock()
    _nextPlanId: int = 0

    INDENT = 2

    def __init__(self, child: Optional["LogicalPlan"]) -> None:
        self._child = child
        self._plan_id = LogicalPlan._fresh_plan_id()

    @staticmethod
    def _fresh_plan_id() -> int:
        plan_id: Optional[int] = None
        with LogicalPlan._lock:
            plan_id = LogicalPlan._nextPlanId
            LogicalPlan._nextPlanId += 1

        assert plan_id is not None
        return plan_id

    def _create_proto_relation(self) -> proto.Relation:
        plan = proto.Relation()
        plan.common.plan_id = self._plan_id
        return plan

    def plan(self, session: "SparkConnectClient") -> proto.Relation:  # type: ignore[empty-body]
        ...

    def command(self, session: "SparkConnectClient") -> proto.Command:  # type: ignore[empty-body]
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

    @property
    def observations(self) -> Dict[str, "Observation"]:
        if self._child is None:
            return {}
        else:
            return self._child.observations

    def _parameters_to_print(self, parameters: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Extracts the parameters that are able to be printed. It looks up the signature
        in the constructor of this :class:`LogicalPlan`, and retrieves the variables
        from this instance by the same name (or the name with prefix `_`)  defined
        in the constructor.

        Parameters
        ----------
        parameters : map
            Parameter mapping from ``inspect.signature(...).parameters``

        Returns
        -------
        dict
            A dictionary consisting of a string name and variable found in this
            :class:`LogicalPlan`.

        Notes
        -----
        :class:`LogicalPlan` itself is filtered out and considered as a non-printable
        parameter.

        Examples
        --------
        The example below returns a dictionary from `self._start`, `self._end`,
        `self._num_partitions`.

        >>> rg = Range(0, 10, 1)
        >>> rg._parameters_to_print(signature(rg.__class__.__init__).parameters)
        {'start': 0, 'end': 10, 'step': 1, 'num_partitions': None}

        If the child is defined, it is not considered as a printable instance

        >>> project = Project(rg, "value")
        >>> project._parameters_to_print(signature(project.__class__.__init__).parameters)
        {'columns': ['value']}
        """
        params = {}
        for name, tpe in parameters.items():
            # LogicalPlan is not to print, e.g., LogicalPlan
            is_logical_plan = isclass(tpe.annotation) and isinstance(tpe.annotation, LogicalPlan)
            # Look up the string argument defined as a forward reference e.g., "LogicalPlan"
            is_forwardref_logical_plan = getattr(tpe.annotation, "__forward_arg__", "").endswith(
                "LogicalPlan"
            )
            # Wrapped LogicalPlan, e.g., Optional[LogicalPlan]
            is_nested_logical_plan = any(
                isclass(a) and issubclass(a, LogicalPlan)
                for a in getattr(tpe.annotation, "__args__", ())
            )
            # Wrapped forward reference of LogicalPlan, e.g., Optional["LogicalPlan"].
            is_nested_forwardref_logical_plan = any(
                getattr(a, "__forward_arg__", "").endswith("LogicalPlan")
                for a in getattr(tpe.annotation, "__args__", ())
            )
            if (
                not is_logical_plan
                and not is_forwardref_logical_plan
                and not is_nested_logical_plan
                and not is_nested_forwardref_logical_plan
            ):
                # Searches self.name or self._name
                try:
                    params[name] = getattr(self, name)
                except AttributeError:
                    try:
                        params[name] = getattr(self, "_" + name)
                    except AttributeError:
                        pass  # Simpy ignore
        return params

    def print(self, indent: int = 0) -> str:
        """
        Print the simple string representation of the current :class:`LogicalPlan`.

        Parameters
        ----------
        indent : int
            The number of leading spaces for the output string.

        Returns
        -------
        str
            Simple string representation of this :class:`LogicalPlan`.
        """
        params = self._parameters_to_print(signature(self.__class__.__init__).parameters)
        pretty_params = [f"{name}='{param}'" for name, param in params.items()]
        if len(pretty_params) == 0:
            pretty_str = ""
        else:
            pretty_str = " " + ", ".join(pretty_params)
        return f"{' ' * indent}<{self.__class__.__name__}{pretty_str}>\n{self._child_print(indent)}"

    def _repr_html_(self) -> str:
        """Returns a  :class:`LogicalPlan` with HTML code. This is generally called in third-party
        systems such as Jupyter.

        Returns
        -------
        str
            HTML representation of this :class:`LogicalPlan`.
        """
        params = self._parameters_to_print(signature(self.__class__.__init__).parameters)
        pretty_params = [
            f"\n              {name}: " f"{param} <br/>" for name, param in params.items()
        ]
        if len(pretty_params) == 0:
            pretty_str = ""
        else:
            pretty_str = "".join(pretty_params)
        return f"""
        <ul>
           <li>
              <b>{self.__class__.__name__}</b><br/>{pretty_str}
              {self._child_repr()}
           </li>
        </ul>
        """

    def _child_print(self, indent: int) -> str:
        return self._child.print(indent + LogicalPlan.INDENT) if self._child else ""

    def _child_repr(self) -> str:
        return self._child._repr_html_() if self._child is not None else ""


class DataSource(LogicalPlan):
    """A datasource with a format and optional a schema from which Spark reads data"""

    def __init__(
        self,
        format: Optional[str] = None,
        schema: Optional[str] = None,
        options: Optional[Mapping[str, str]] = None,
        paths: Optional[List[str]] = None,
        predicates: Optional[List[str]] = None,
        is_streaming: Optional[bool] = None,
    ) -> None:
        super().__init__(None)

        assert format is None or isinstance(format, str)
        assert schema is None or isinstance(schema, str)

        if options is not None:
            for k, v in options.items():
                assert isinstance(k, str)
                assert isinstance(v, str)

        if paths is not None:
            assert isinstance(paths, list)
            assert all(isinstance(path, str) for path in paths)

        if predicates is not None:
            assert isinstance(predicates, list)
            assert all(isinstance(predicate, str) for predicate in predicates)

        self._format = format
        self._schema = schema
        self._options = options
        self._paths = paths
        self._predicates = predicates
        self._is_streaming = is_streaming

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        if self._format is not None:
            plan.read.data_source.format = self._format
        if self._schema is not None:
            plan.read.data_source.schema = self._schema
        if self._options is not None and len(self._options) > 0:
            for k, v in self._options.items():
                plan.read.data_source.options[k] = v
        if self._paths is not None and len(self._paths) > 0:
            plan.read.data_source.paths.extend(self._paths)
        if self._predicates is not None and len(self._predicates) > 0:
            plan.read.data_source.predicates.extend(self._predicates)
        if self._is_streaming is not None:
            plan.read.is_streaming = self._is_streaming
        return plan


class Read(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        options: Optional[Dict[str, str]] = None,
        is_streaming: Optional[bool] = None,
    ) -> None:
        super().__init__(None)
        self.table_name = table_name
        self.options = options or {}
        self._is_streaming = is_streaming

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.read.named_table.unparsed_identifier = self.table_name
        if self._is_streaming is not None:
            plan.read.is_streaming = self._is_streaming
        for k, v in self.options.items():
            plan.read.named_table.options[k] = v
        return plan

    def print(self, indent: int = 0) -> str:
        return f"{' ' * indent}<Read table_name={self.table_name}>\n"


class LocalRelation(LogicalPlan):
    """Creates a LocalRelation plan object based on a PyArrow Table."""

    def __init__(
        self,
        table: Optional["pa.Table"],
        schema: Optional[str] = None,
    ) -> None:
        super().__init__(None)

        if table is None:
            assert schema is not None
        else:
            assert isinstance(table, pa.Table)

        assert schema is None or isinstance(schema, str)

        self._table = table

        self._schema = schema

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        if self._table is not None:
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, self._table.schema) as writer:
                for b in self._table.to_batches():
                    writer.write_batch(b)
            plan.local_relation.data = sink.getvalue().to_pybytes()

        if self._schema is not None:
            plan.local_relation.schema = self._schema
        return plan

    def serialize(self, session: "SparkConnectClient") -> bytes:
        p = self.plan(session)
        return bytes(p.local_relation.SerializeToString())

    def print(self, indent: int = 0) -> str:
        return f"{' ' * indent}<LocalRelation>\n"

    def _repr_html_(self) -> str:
        return """
        <ul>
            <li><b>LocalRelation</b></li>
        </ul>
        """


class CachedLocalRelation(LogicalPlan):
    """Creates a CachedLocalRelation plan object based on a hash of a LocalRelation."""

    def __init__(self, hash: str) -> None:
        super().__init__(None)

        self._hash = hash

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        clr = plan.cached_local_relation

        clr.hash = self._hash

        return plan

    def print(self, indent: int = 0) -> str:
        return f"{' ' * indent}<CachedLocalRelation>\n"

    def _repr_html_(self) -> str:
        return """
        <ul>
            <li><b>CachedLocalRelation</b></li>
        </ul>
        """


class ShowString(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], num_rows: int, truncate: int, vertical: bool
    ) -> None:
        super().__init__(child)
        self.num_rows = num_rows
        self.truncate = truncate
        self.vertical = vertical

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.show_string.input.CopyFrom(self._child.plan(session))
        plan.show_string.num_rows = self.num_rows
        plan.show_string.truncate = self.truncate
        plan.show_string.vertical = self.vertical
        return plan


class HtmlString(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], num_rows: int, truncate: int) -> None:
        super().__init__(child)
        self.num_rows = num_rows
        self.truncate = truncate

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.html_string.input.CopyFrom(self._child.plan(session))
        plan.html_string.num_rows = self.num_rows
        plan.html_string.truncate = self.truncate
        return plan


class Project(LogicalPlan):
    """Logical plan object for a projection.

    All input arguments are directly serialized into the corresponding protocol buffer
    objects. This class only provides very limited error handling and input validation.

    To be compatible with PySpark, we validate that the input arguments are all
    expressions to be able to serialize them to the server.

    """

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columns: List[Column],
    ) -> None:
        super().__init__(child)
        assert all(isinstance(c, Column) for c in columns)
        self._columns = columns

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.project.input.CopyFrom(self._child.plan(session))
        plan.project.expressions.extend([c.to_plan(session) for c in self._columns])
        return plan


class WithColumns(LogicalPlan):
    """Logical plan object for a withColumns operation."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columnNames: Sequence[str],
        columns: Sequence[Column],
        metadata: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(child)

        assert isinstance(columnNames, list)
        assert len(columnNames) > 0
        assert all(isinstance(c, str) for c in columnNames)

        assert isinstance(columns, list)
        assert len(columns) == len(columnNames)
        assert all(isinstance(c, Column) for c in columns)

        if metadata is not None:
            assert isinstance(metadata, list)
            assert len(metadata) == len(columnNames)
            for m in metadata:
                assert isinstance(m, str)
                # validate json string
                assert m == "" or json.loads(m) is not None

        self._columnNames = columnNames
        self._columns = columns
        self._metadata = metadata

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.with_columns.input.CopyFrom(self._child.plan(session))

        for i in range(0, len(self._columnNames)):
            alias = proto.Expression.Alias()
            alias.expr.CopyFrom(self._columns[i].to_plan(session))
            alias.name.append(self._columnNames[i])
            if self._metadata is not None:
                alias.metadata = self._metadata[i]
            plan.with_columns.aliases.append(alias)

        return plan


class WithWatermark(LogicalPlan):
    """Logical plan object for a WithWatermark operation."""

    def __init__(self, child: Optional["LogicalPlan"], event_time: str, delay_threshold: str):
        super().__init__(child)
        self._event_time = event_time
        self._delay_threshold = delay_threshold

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.with_watermark.input.CopyFrom(self._child.plan(session))
        plan.with_watermark.event_time = self._event_time
        plan.with_watermark.delay_threshold = self._delay_threshold
        return plan


class CachedRemoteRelation(LogicalPlan):
    """Logical plan object for a DataFrame reference which represents a DataFrame that's been
    cached on the server with a given id."""

    def __init__(self, relationId: str):
        super().__init__(None)
        self._relationId = relationId

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.cached_remote_relation.relation_id = self._relationId
        return plan


class Hint(LogicalPlan):
    """Logical plan object for a Hint operation."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        name: str,
        parameters: Sequence[Column],
    ) -> None:
        super().__init__(child)

        assert isinstance(name, str)

        self._name = name

        assert parameters is not None and isinstance(parameters, List)
        for param in parameters:
            assert isinstance(param, Column)

        self._parameters = parameters

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.hint.input.CopyFrom(self._child.plan(session))
        plan.hint.name = self._name
        plan.hint.parameters.extend([param.to_plan(session) for param in self._parameters])
        return plan


class Filter(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], filter: Column) -> None:
        super().__init__(child)
        self.filter = filter

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.filter.input.CopyFrom(self._child.plan(session))
        plan.filter.condition.CopyFrom(self.filter.to_plan(session))
        return plan


class Limit(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], limit: int) -> None:
        super().__init__(child)
        self.limit = limit

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.limit.input.CopyFrom(self._child.plan(session))
        plan.limit.limit = self.limit
        return plan


class Tail(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], limit: int) -> None:
        super().__init__(child)
        self.limit = limit

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.tail.input.CopyFrom(self._child.plan(session))
        plan.tail.limit = self.limit
        return plan


class Offset(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], offset: int = 0) -> None:
        super().__init__(child)
        self.offset = offset

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.offset.input.CopyFrom(self._child.plan(session))
        plan.offset.offset = self.offset
        return plan


class Deduplicate(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        all_columns_as_keys: bool = False,
        column_names: Optional[List[str]] = None,
        within_watermark: bool = False,
    ) -> None:
        super().__init__(child)
        self.all_columns_as_keys = all_columns_as_keys
        self.column_names = column_names
        self.within_watermark = within_watermark

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.deduplicate.input.CopyFrom(self._child.plan(session))
        plan.deduplicate.all_columns_as_keys = self.all_columns_as_keys
        plan.deduplicate.within_watermark = self.within_watermark
        if self.column_names is not None:
            plan.deduplicate.column_names.extend(self.column_names)
        return plan


class Sort(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columns: List[Column],
        is_global: bool,
    ) -> None:
        super().__init__(child)

        assert all(isinstance(c, Column) for c in columns)
        assert isinstance(is_global, bool)

        self.columns = columns
        self.is_global = is_global

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.sort.input.CopyFrom(self._child.plan(session))
        plan.sort.order.extend([c.to_plan(session).sort_order for c in self.columns])
        plan.sort.is_global = self.is_global
        return plan


class Drop(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        columns: List[Union[Column, str]],
    ) -> None:
        super().__init__(child)
        if len(columns) > 0:
            assert all(isinstance(c, (Column, str)) for c in columns)
        self._columns = columns

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.drop.input.CopyFrom(self._child.plan(session))
        for c in self._columns:
            if isinstance(c, Column):
                plan.drop.columns.append(c.to_plan(session))
            else:
                plan.drop.column_names.append(c)
        return plan


class Sample(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        lower_bound: float,
        upper_bound: float,
        with_replacement: bool,
        seed: int,
        deterministic_order: bool = False,
    ) -> None:
        super().__init__(child)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.with_replacement = with_replacement
        self.seed = seed
        self.deterministic_order = deterministic_order

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.sample.input.CopyFrom(self._child.plan(session))
        plan.sample.lower_bound = self.lower_bound
        plan.sample.upper_bound = self.upper_bound
        plan.sample.with_replacement = self.with_replacement
        plan.sample.seed = self.seed
        plan.sample.deterministic_order = self.deterministic_order
        return plan


class Aggregate(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        group_type: str,
        grouping_cols: Sequence[Column],
        aggregate_cols: Sequence[Column],
        pivot_col: Optional[Column],
        pivot_values: Optional[Sequence[Column]],
        grouping_sets: Optional[Sequence[Sequence[Column]]],
    ) -> None:
        super().__init__(child)

        assert isinstance(group_type, str) and group_type in [
            "groupby",
            "rollup",
            "cube",
            "pivot",
            "grouping_sets",
        ]
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
        elif group_type == "grouping_sets":
            assert grouping_sets is None or isinstance(grouping_sets, list)
        else:
            assert pivot_col is None
            assert pivot_values is None
            assert grouping_sets is None

        self._pivot_col = pivot_col
        self._pivot_values = pivot_values
        self._grouping_sets = grouping_sets

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.aggregate.input.CopyFrom(self._child.plan(session))
        plan.aggregate.grouping_expressions.extend(
            [c.to_plan(session) for c in self._grouping_cols]
        )
        plan.aggregate.aggregate_expressions.extend(
            [c.to_plan(session) for c in self._aggregate_cols]
        )

        if self._group_type == "groupby":
            plan.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY
        elif self._group_type == "rollup":
            plan.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP
        elif self._group_type == "cube":
            plan.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_CUBE
        elif self._group_type == "pivot":
            plan.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_PIVOT
            assert self._pivot_col is not None
            plan.aggregate.pivot.col.CopyFrom(self._pivot_col.to_plan(session))
            if self._pivot_values is not None and len(self._pivot_values) > 0:
                plan.aggregate.pivot.values.extend(
                    [v.to_plan(session).literal for v in self._pivot_values]
                )
        elif self._group_type == "grouping_sets":
            plan.aggregate.group_type = proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS
            assert self._grouping_sets is not None
            for grouping_set in self._grouping_sets:
                plan.aggregate.grouping_sets.append(
                    proto.Aggregate.GroupingSets(
                        grouping_set=[c.to_plan(session) for c in grouping_set]
                    )
                )
        return plan


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
            raise IllegalArgumentException(
                error_class="UNSUPPORTED_JOIN_TYPE",
                message_parameters={"join_type": how},
            )
        self.how = join_type

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.join.left.CopyFrom(self.left.plan(session))
        plan.join.right.CopyFrom(self.right.plan(session))
        if self.on is not None:
            if not isinstance(self.on, list):
                if isinstance(self.on, str):
                    plan.join.using_columns.append(self.on)
                else:
                    plan.join.join_condition.CopyFrom(self.on.to_plan(session))
            elif len(self.on) > 0:
                if isinstance(self.on[0], str):
                    plan.join.using_columns.extend(cast(str, self.on))
                else:
                    merge_column = functools.reduce(lambda c1, c2: c1 & c2, self.on)
                    plan.join.join_condition.CopyFrom(cast(Column, merge_column).to_plan(session))
        plan.join.join_type = self.how
        return plan

    @property
    def observations(self) -> Dict[str, "Observation"]:
        return dict(**super().observations, **self.right.observations)

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


class AsOfJoin(LogicalPlan):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_as_of: Column,
        right_as_of: Column,
        on: Optional[Union[str, List[str], Column, List[Column]]],
        how: str,
        tolerance: Optional[Column],
        allow_exact_matches: bool,
        direction: str,
    ) -> None:
        super().__init__(left)
        self.left = left
        self.right = right
        self.left_as_of = left_as_of
        self.right_as_of = right_as_of
        self.on = on
        self.how = how
        self.tolerance = tolerance
        self.allow_exact_matches = allow_exact_matches
        self.direction = direction

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.as_of_join.left.CopyFrom(self.left.plan(session))
        plan.as_of_join.right.CopyFrom(self.right.plan(session))

        plan.as_of_join.left_as_of.CopyFrom(self.left_as_of.to_plan(session))
        plan.as_of_join.right_as_of.CopyFrom(self.right_as_of.to_plan(session))

        if self.on is not None:
            if not isinstance(self.on, list):
                if isinstance(self.on, str):
                    plan.as_of_join.using_columns.append(self.on)
                else:
                    plan.as_of_join.join_expr.CopyFrom(self.on.to_plan(session))
            elif len(self.on) > 0:
                if isinstance(self.on[0], str):
                    plan.as_of_join.using_columns.extend(cast(List[str], self.on))
                else:
                    merge_column = functools.reduce(lambda c1, c2: c1 & c2, self.on)
                    plan.as_of_join.join_expr.CopyFrom(cast(Column, merge_column).to_plan(session))

        plan.as_of_join.join_type = self.how

        if self.tolerance is not None:
            plan.as_of_join.tolerance.CopyFrom(self.tolerance.to_plan(session))

        plan.as_of_join.allow_exact_matches = self.allow_exact_matches
        plan.as_of_join.direction = self.direction

        return plan

    @property
    def observations(self) -> Dict[str, "Observation"]:
        return dict(**super().observations, **self.right.observations)

    def print(self, indent: int = 0) -> str:
        assert self.left is not None
        assert self.right is not None

        i = " " * indent
        o = " " * (indent + LogicalPlan.INDENT)
        n = indent + LogicalPlan.INDENT * 2
        return (
            f"{i}<AsOfJoin left_as_of={self.left_as_of}, right_as_of={self.right_as_of}, "
            f"on={self.on} how={self.how}>\n{o}"
            f"left=\n{self.left.print(n)}\n{o}right=\n{self.right.print(n)}"
        )

    def _repr_html_(self) -> str:
        assert self.left is not None
        assert self.right is not None

        return f"""
        <ul>
            <li>
                <b>AsOfJoin</b><br />
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
        allow_missing_columns: bool = False,
    ) -> None:
        super().__init__(child)
        self.other = other
        self.by_name = by_name
        self.is_all = is_all
        self.set_op = set_op
        self.allow_missing_columns = allow_missing_columns

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        if self._child is not None:
            plan.set_op.left_input.CopyFrom(self._child.plan(session))
        if self.other is not None:
            plan.set_op.right_input.CopyFrom(self.other.plan(session))
        if self.set_op == "union":
            plan.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_UNION
        elif self.set_op == "intersect":
            plan.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_INTERSECT
        elif self.set_op == "except":
            plan.set_op.set_op_type = proto.SetOperation.SET_OP_TYPE_EXCEPT
        else:
            raise PySparkValueError(
                error_class="UNSUPPORTED_OPERATION",
                message_parameters={"operation": self.set_op},
            )

        plan.set_op.is_all = self.is_all
        plan.set_op.by_name = self.by_name
        plan.set_op.allow_missing_columns = self.allow_missing_columns
        return plan

    @property
    def observations(self) -> Dict[str, "Observation"]:
        return dict(
            **super().observations,
            **(self.other.observations if self.other is not None else {}),
        )

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
        plan = self._create_proto_relation()
        if self._child is not None:
            plan.repartition.input.CopyFrom(self._child.plan(session))
        plan.repartition.shuffle = self._shuffle
        plan.repartition.num_partitions = self._num_partitions
        return plan


class RepartitionByExpression(LogicalPlan):
    """Repartition Relation into a different number of partitions using Expression"""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        num_partitions: Optional[int],
        columns: List[Column],
    ) -> None:
        super().__init__(child)
        self.num_partitions = num_partitions
        assert all(isinstance(c, Column) for c in columns)
        self.columns = columns

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.repartition_by_expression.partition_exprs.extend(
            [c.to_plan(session) for c in self.columns]
        )

        if self._child is not None:
            plan.repartition_by_expression.input.CopyFrom(self._child.plan(session))
        if self.num_partitions is not None:
            plan.repartition_by_expression.num_partitions = self.num_partitions
        return plan


class SubqueryAlias(LogicalPlan):
    """Alias for a relation."""

    def __init__(self, child: Optional["LogicalPlan"], alias: str) -> None:
        super().__init__(child)
        self._alias = alias

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        if self._child is not None:
            plan.subquery_alias.input.CopyFrom(self._child.plan(session))
        plan.subquery_alias.alias = self._alias
        return plan


class WithRelations(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        references: Sequence["LogicalPlan"],
    ) -> None:
        super().__init__(child)
        assert references is not None and len(references) > 0
        assert all(isinstance(ref, LogicalPlan) for ref in references)
        self._references = references

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        if self._child is not None:
            plan.with_relations.root.CopyFrom(self._child.plan(session))
        for ref in self._references:
            plan.with_relations.references.append(ref.plan(session))
        return plan


class SQL(LogicalPlan):
    def __init__(
        self,
        query: str,
        args: Optional[List[Column]] = None,
        named_args: Optional[Dict[str, Column]] = None,
        views: Optional[Sequence[SubqueryAlias]] = None,
    ) -> None:
        super().__init__(None)

        if args is not None:
            assert isinstance(args, List)
            assert all(isinstance(arg, Column) for arg in args)

        if named_args is not None:
            assert isinstance(named_args, Dict)
            for k, arg in named_args.items():
                assert isinstance(k, str)
                assert isinstance(arg, Column)

        if views is not None:
            assert isinstance(views, List)
            assert all(isinstance(v, SubqueryAlias) for v in views)
            if len(views) > 0:
                # reserved plan id for WithRelations
                self._plan_id_with_rel = LogicalPlan._fresh_plan_id()

        self._query = query
        self._args = args
        self._named_args = named_args
        self._views = views

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.sql.query = self._query

        if self._args is not None and len(self._args) > 0:
            plan.sql.pos_arguments.extend([arg.to_plan(session) for arg in self._args])
        if self._named_args is not None and len(self._named_args) > 0:
            for k, arg in self._named_args.items():
                plan.sql.named_arguments[k].CopyFrom(arg.to_plan(session))

        if self._views is not None and len(self._views) > 0:
            # build new plan like
            # with_relations [id 10]
            #     root: sql  [id 9]
            #     reference:
            #          view#1: [id 8]
            #          view#2: [id 5]
            sql_plan = plan
            plan = proto.Relation()
            plan.common.plan_id = self._plan_id_with_rel
            plan.with_relations.root.CopyFrom(sql_plan)
            plan.with_relations.references.extend([v.plan(session) for v in self._views])

        return plan

    def command(self, session: "SparkConnectClient") -> proto.Command:
        cmd = proto.Command()
        cmd.sql_command.input.CopyFrom(self.plan(session))
        return cmd


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
        plan = self._create_proto_relation()
        plan.range.start = self._start
        plan.range.end = self._end
        plan.range.step = self._step
        if self._num_partitions is not None:
            plan.range.num_partitions = self._num_partitions
        return plan


class ToSchema(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], schema: DataType) -> None:
        super().__init__(child)
        self._schema = schema

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.to_schema.input.CopyFrom(self._child.plan(session))
        plan.to_schema.schema.CopyFrom(pyspark_types_to_proto_types(self._schema))
        return plan


class WithColumnsRenamed(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], colsMap: Mapping[str, str]) -> None:
        super().__init__(child)
        self._colsMap = colsMap

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.with_columns_renamed.input.CopyFrom(self._child.plan(session))
        if len(self._colsMap) > 0:
            for k, v in self._colsMap.items():
                rename = proto.WithColumnsRenamed.Rename()
                rename.col_name = k
                rename.new_col_name = v
                plan.with_columns_renamed.renames.append(rename)
        return plan


class Unpivot(LogicalPlan):
    """Logical plan object for a unpivot operation."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        ids: List[Column],
        values: Optional[List[Column]],
        variable_column_name: str,
        value_column_name: str,
    ) -> None:
        super().__init__(child)
        self.ids = ids
        self.values = values
        self.variable_column_name = variable_column_name
        self.value_column_name = value_column_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.unpivot.input.CopyFrom(self._child.plan(session))
        plan.unpivot.ids.extend([id.to_plan(session) for id in self.ids])
        if self.values is not None:
            plan.unpivot.values.values.extend([v.to_plan(session) for v in self.values])
        plan.unpivot.variable_column_name = self.variable_column_name
        plan.unpivot.value_column_name = self.value_column_name
        return plan


class CollectMetrics(LogicalPlan):
    """Logical plan object for a CollectMetrics operation."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        observation: Union[str, "Observation"],
        exprs: List[Column],
    ) -> None:
        super().__init__(child)
        self._observation = observation
        assert all(isinstance(e, Column) for e in exprs)
        self._exprs = exprs

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.collect_metrics.input.CopyFrom(self._child.plan(session))
        plan.collect_metrics.name = (
            self._observation
            if isinstance(self._observation, str)
            else str(self._observation._name)
        )
        plan.collect_metrics.metrics.extend([e.to_plan(session) for e in self._exprs])
        return plan

    @property
    def observations(self) -> Dict[str, "Observation"]:
        from pyspark.sql.connect.observation import Observation

        if isinstance(self._observation, Observation):
            observations = {str(self._observation._name): self._observation}
        else:
            observations = {}
        return dict(**super().observations, **observations)


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
        plan = self._create_proto_relation()
        plan.fill_na.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.fill_na.cols.extend(self.cols)
        plan.fill_na.values.extend([self._convert_value(v) for v in self.values])
        return plan


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
        plan = self._create_proto_relation()
        plan.drop_na.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.drop_na.cols.extend(self.cols)
        if self.min_non_nulls is not None:
            plan.drop_na.min_non_nulls = self.min_non_nulls
        return plan


class NAReplace(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        cols: Optional[List[str]],
        replacements: Sequence[Tuple[Column, Column]],
    ) -> None:
        super().__init__(child)
        self.cols = cols

        assert replacements is not None and isinstance(replacements, List)
        for k, v in replacements:
            assert k is not None and isinstance(k, Column)
            assert v is not None and isinstance(v, Column)
        self.replacements = replacements

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.replace.input.CopyFrom(self._child.plan(session))
        if self.cols is not None and len(self.cols) > 0:
            plan.replace.cols.extend(self.cols)
        if len(self.replacements) > 0:
            for old_value, new_value in self.replacements:
                replacement = proto.NAReplace.Replacement()
                replacement.old_value.CopyFrom(old_value.to_plan(session).literal)
                replacement.new_value.CopyFrom(new_value.to_plan(session).literal)
                plan.replace.replacements.append(replacement)
        return plan


class StatSummary(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], statistics: List[str]) -> None:
        super().__init__(child)
        self.statistics = statistics

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.summary.input.CopyFrom(self._child.plan(session))
        plan.summary.statistics.extend(self.statistics)
        return plan


class StatDescribe(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], cols: List[str]) -> None:
        super().__init__(child)
        self.cols = cols

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.describe.input.CopyFrom(self._child.plan(session))
        plan.describe.cols.extend(self.cols)
        return plan


class StatCov(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], col1: str, col2: str) -> None:
        super().__init__(child)
        self._col1 = col1
        self._col2 = col2

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.cov.input.CopyFrom(self._child.plan(session))
        plan.cov.col1 = self._col1
        plan.cov.col2 = self._col2
        return plan


class StatApproxQuantile(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        cols: List[str],
        probabilities: List[float],
        relativeError: float,
    ) -> None:
        super().__init__(child)
        self._cols = cols
        self._probabilities = probabilities
        self._relativeError = relativeError

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.approx_quantile.input.CopyFrom(self._child.plan(session))
        plan.approx_quantile.cols.extend(self._cols)
        plan.approx_quantile.probabilities.extend(self._probabilities)
        plan.approx_quantile.relative_error = self._relativeError
        return plan


class StatCrosstab(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], col1: str, col2: str) -> None:
        super().__init__(child)
        self.col1 = col1
        self.col2 = col2

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.crosstab.input.CopyFrom(self._child.plan(session))
        plan.crosstab.col1 = self.col1
        plan.crosstab.col2 = self.col2
        return plan


class StatFreqItems(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        cols: List[str],
        support: float,
    ) -> None:
        super().__init__(child)
        self._cols = cols
        self._support = support

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.freq_items.input.CopyFrom(self._child.plan(session))
        plan.freq_items.cols.extend(self._cols)
        plan.freq_items.support = self._support
        return plan


class StatSampleBy(LogicalPlan):
    def __init__(
        self,
        child: Optional["LogicalPlan"],
        col: Column,
        fractions: Sequence[Tuple[Column, float]],
        seed: int,
    ) -> None:
        super().__init__(child)

        assert col is not None and isinstance(col, (Column, str))

        assert fractions is not None and isinstance(fractions, List)
        for k, v in fractions:
            assert k is not None and isinstance(k, Column)
            assert v is not None and isinstance(v, float)

        assert seed is None or isinstance(seed, int)

        self._col = col
        self._fractions = fractions
        self._seed = seed

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.sample_by.input.CopyFrom(self._child.plan(session))
        plan.sample_by.col.CopyFrom(self._col._expr.to_plan(session))
        if len(self._fractions) > 0:
            for k, v in self._fractions:
                fraction = proto.StatSampleBy.Fraction()
                fraction.stratum.CopyFrom(k.to_plan(session).literal)
                fraction.fraction = float(v)
                plan.sample_by.fractions.append(fraction)
        plan.sample_by.seed = self._seed
        return plan


class StatCorr(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], col1: str, col2: str, method: str) -> None:
        super().__init__(child)
        self._col1 = col1
        self._col2 = col2
        self._method = method

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.corr.input.CopyFrom(self._child.plan(session))
        plan.corr.col1 = self._col1
        plan.corr.col2 = self._col2
        plan.corr.method = self._method
        return plan


class ToDF(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], cols: Sequence[str]) -> None:
        super().__init__(child)
        self._cols = cols

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.to_df.input.CopyFrom(self._child.plan(session))
        plan.to_df.column_names.extend(self._cols)
        return plan


class CreateView(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], name: str, is_global: bool, replace: bool
    ) -> None:
        super().__init__(child)
        self._name = name
        self._is_global = is_global
        self._replace = replace

    def command(self, session: "SparkConnectClient") -> proto.Command:
        assert self._child is not None
        plan = proto.Command()

        plan.create_dataframe_view.replace = self._replace
        plan.create_dataframe_view.is_global = self._is_global
        plan.create_dataframe_view.name = self._name
        plan.create_dataframe_view.input.CopyFrom(self._child.plan(session))
        return plan


class WriteOperation(LogicalPlan):
    def __init__(self, child: "LogicalPlan") -> None:
        super(WriteOperation, self).__init__(child)
        self.source: Optional[str] = None
        self.path: Optional[str] = None
        self.table_name: Optional[str] = None
        self.table_save_method: Optional[str] = None
        self.mode: Optional[str] = None
        self.sort_cols: List[str] = []
        self.partitioning_cols: List[str] = []
        self.options: Dict[str, Optional[str]] = {}
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
                plan.write_operation.options.pop(k, None)
            else:
                plan.write_operation.options[k] = cast(str, self.options[k])

        if self.table_name is not None:
            plan.write_operation.table.table_name = self.table_name
            if self.table_save_method is not None:
                tsm = self.table_save_method.lower()
                if tsm == "save_as_table":
                    plan.write_operation.table.save_method = (
                        proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE  # noqa: E501
                    )
                elif tsm == "insert_into":
                    plan.write_operation.table.save_method = (
                        proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
                    )
                else:
                    raise PySparkValueError(
                        error_class="UNSUPPORTED_OPERATION",
                        message_parameters={"operation": tsm},
                    )
        elif self.path is not None:
            plan.write_operation.path = self.path

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
                raise PySparkValueError(
                    error_class="UNSUPPORTED_OPERATION",
                    message_parameters={"operation": self.mode},
                )
        return plan

    def print(self, indent: int = 0) -> str:
        i = " " * indent
        return (
            f"{i}"
            f"<WriteOperation source='{self.source}' "
            f"path='{self.path} "
            f"table_name='{self.table_name}' "
            f"table_save_method='{self.table_save_method}' "
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
            f"table_save_method: '{self.table_save_method}' <br />"
            f"mode: '{self.mode}' <br />"
            f"sort_cols: '{self.sort_cols}' <br />"
            f"partitioning_cols: '{self.partitioning_cols}' <br />"
            f"num_buckets: '{self.num_buckets}' <br />"
            f"bucket_cols: '{self.bucket_cols}' <br />"
            f"options: '{self.options}'<br />"
            f"</li></ul>"
        )


class WriteOperationV2(LogicalPlan):
    def __init__(self, child: "LogicalPlan", table_name: str) -> None:
        super(WriteOperationV2, self).__init__(child)
        self.table_name: Optional[str] = table_name
        self.provider: Optional[str] = None
        self.partitioning_columns: List[Column] = []
        self.options: dict[str, Optional[str]] = {}
        self.table_properties: dict[str, Optional[str]] = {}
        self.mode: Optional[str] = None
        self.overwrite_condition: Optional[Column] = None

    def command(self, session: "SparkConnectClient") -> proto.Command:
        assert self._child is not None
        plan = proto.Command()
        plan.write_operation_v2.input.CopyFrom(self._child.plan(session))
        if self.table_name is not None:
            plan.write_operation_v2.table_name = self.table_name
        if self.provider is not None:
            plan.write_operation_v2.provider = self.provider

        plan.write_operation_v2.partitioning_columns.extend(
            [c.to_plan(session) for c in self.partitioning_columns]
        )

        for k in self.options:
            if self.options[k] is None:
                plan.write_operation_v2.options.pop(k, None)
            else:
                plan.write_operation_v2.options[k] = cast(str, self.options[k])

        for k in self.table_properties:
            if self.table_properties[k] is None:
                plan.write_operation_v2.table_properties.pop(k, None)
            else:
                plan.write_operation_v2.table_properties[k] = cast(str, self.table_properties[k])

        if self.mode is not None:
            wm = self.mode.lower()
            if wm == "create":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_CREATE
            elif wm == "overwrite":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_OVERWRITE
                if self.overwrite_condition is not None:
                    plan.write_operation_v2.overwrite_condition.CopyFrom(
                        self.overwrite_condition.to_plan(session)
                    )
            elif wm == "overwrite_partitions":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS
            elif wm == "append":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_APPEND
            elif wm == "replace":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_REPLACE
            elif wm == "create_or_replace":
                plan.write_operation_v2.mode = proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE
            else:
                raise PySparkValueError(
                    error_class="UNSUPPORTED_OPERATION",
                    message_parameters={"operation": self.mode},
                )
        return plan


class WriteStreamOperation(LogicalPlan):
    def __init__(self, child: "LogicalPlan") -> None:
        super(WriteStreamOperation, self).__init__(child)
        self.write_op = proto.WriteStreamOperationStart()

    def command(self, session: "SparkConnectClient") -> proto.Command:
        assert self._child is not None
        self.write_op.input.CopyFrom(self._child.plan(session))
        cmd = proto.Command()
        cmd.write_stream_operation_start.CopyFrom(self.write_op)
        return cmd


class RemoveRemoteCachedRelation(LogicalPlan):
    def __init__(self, relation: CachedRemoteRelation) -> None:
        super().__init__(None)
        self._relation = relation

    def command(self, session: "SparkConnectClient") -> proto.Command:
        plan = self._create_proto_relation()
        plan.cached_remote_relation.relation_id = self._relation._relationId
        cmd = proto.Command()
        cmd.remove_cached_remote_relation_command.relation.CopyFrom(plan.cached_remote_relation)
        return cmd


class Checkpoint(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], local: bool, eager: bool) -> None:
        super().__init__(child)
        self._local = local
        self._eager = eager

    def command(self, session: "SparkConnectClient") -> proto.Command:
        cmd = proto.Command()
        assert self._child is not None
        cmd.checkpoint_command.CopyFrom(
            proto.CheckpointCommand(
                relation=self._child.plan(session),
                local=self._local,
                eager=self._eager,
            )
        )
        return cmd


# Catalog API (internal-only)
class CurrentDatabase(LogicalPlan):
    def __init__(self) -> None:
        super().__init__(None)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.current_database.SetInParent()
        return plan


class SetCurrentDatabase(LogicalPlan):
    def __init__(self, db_name: str) -> None:
        super().__init__(None)
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.set_current_database.db_name = self._db_name
        return plan


class ListDatabases(LogicalPlan):
    def __init__(self, pattern: Optional[str] = None) -> None:
        super().__init__(None)
        self._pattern = pattern

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.list_databases.SetInParent()
        if self._pattern is not None:
            plan.catalog.list_databases.pattern = self._pattern
        return plan


class ListTables(LogicalPlan):
    def __init__(self, db_name: Optional[str] = None, pattern: Optional[str] = None) -> None:
        super().__init__(None)
        self._db_name = db_name
        self._pattern = pattern

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.list_tables.SetInParent()
        if self._db_name is not None:
            plan.catalog.list_tables.db_name = self._db_name
        if self._pattern is not None:
            plan.catalog.list_tables.pattern = self._pattern
        return plan


class ListFunctions(LogicalPlan):
    def __init__(self, db_name: Optional[str] = None, pattern: Optional[str] = None) -> None:
        super().__init__(None)
        self._db_name = db_name
        self._pattern = pattern

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.list_functions.SetInParent()
        if self._db_name is not None:
            plan.catalog.list_functions.db_name = self._db_name
        if self._pattern is not None:
            plan.catalog.list_functions.pattern = self._pattern
        return plan


class ListColumns(LogicalPlan):
    def __init__(self, table_name: str, db_name: Optional[str] = None) -> None:
        super().__init__(None)
        self._table_name = table_name
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.list_columns.table_name = self._table_name
        if self._db_name is not None:
            plan.catalog.list_columns.db_name = self._db_name
        return plan


class GetDatabase(LogicalPlan):
    def __init__(self, db_name: str) -> None:
        super().__init__(None)
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.get_database.db_name = self._db_name
        return plan


class GetTable(LogicalPlan):
    def __init__(self, table_name: str, db_name: Optional[str] = None) -> None:
        super().__init__(None)
        self._table_name = table_name
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.get_table.table_name = self._table_name
        if self._db_name is not None:
            plan.catalog.get_table.db_name = self._db_name
        return plan


class GetFunction(LogicalPlan):
    def __init__(self, function_name: str, db_name: Optional[str] = None) -> None:
        super().__init__(None)
        self._function_name = function_name
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.get_function.function_name = self._function_name
        if self._db_name is not None:
            plan.catalog.get_function.db_name = self._db_name
        return plan


class DatabaseExists(LogicalPlan):
    def __init__(self, db_name: str) -> None:
        super().__init__(None)
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.database_exists.db_name = self._db_name
        return plan


class TableExists(LogicalPlan):
    def __init__(self, table_name: str, db_name: Optional[str] = None) -> None:
        super().__init__(None)
        self._table_name = table_name
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.table_exists.table_name = self._table_name
        if self._db_name is not None:
            plan.catalog.table_exists.db_name = self._db_name
        return plan


class FunctionExists(LogicalPlan):
    def __init__(self, function_name: str, db_name: Optional[str] = None) -> None:
        super().__init__(None)
        self._function_name = function_name
        self._db_name = db_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.function_exists.function_name = self._function_name
        if self._db_name is not None:
            plan.catalog.function_exists.db_name = self._db_name
        return plan


class CreateTable(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        path: str,
        source: Optional[str] = None,
        description: Optional[str] = None,
        schema: Optional[DataType] = None,
        options: Mapping[str, str] = {},
    ) -> None:
        super().__init__(None)
        self._table_name = table_name
        self._path = path
        self._source = source
        self._description = description
        self._schema = schema
        self._options = options

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.create_table.table_name = self._table_name
        if self._path is not None:
            plan.catalog.create_table.path = self._path
        if self._source is not None:
            plan.catalog.create_table.source = self._source
        if self._description is not None:
            plan.catalog.create_table.description = self._description
        if self._schema is not None:
            plan.catalog.create_table.schema.CopyFrom(pyspark_types_to_proto_types(self._schema))
        for k in self._options.keys():
            v = self._options.get(k)
            if v is not None:
                plan.catalog.create_table.options[k] = v
        return plan


class DropTempView(LogicalPlan):
    def __init__(self, view_name: str) -> None:
        super().__init__(None)
        self._view_name = view_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.drop_temp_view.view_name = self._view_name
        return plan


class DropGlobalTempView(LogicalPlan):
    def __init__(self, view_name: str) -> None:
        super().__init__(None)
        self._view_name = view_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.drop_global_temp_view.view_name = self._view_name
        return plan


class RecoverPartitions(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self._table_name = table_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.recover_partitions.table_name = self._table_name
        return plan


class IsCached(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self._table_name = table_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.is_cached.table_name = self._table_name
        return plan


class CacheTable(LogicalPlan):
    def __init__(self, table_name: str, storage_level: Optional[StorageLevel] = None) -> None:
        super().__init__(None)
        self._table_name = table_name
        self._storage_level = storage_level

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        _cache_table = proto.CacheTable(table_name=self._table_name)
        if self._storage_level:
            _cache_table.storage_level.CopyFrom(storage_level_to_proto(self._storage_level))
        plan.catalog.cache_table.CopyFrom(_cache_table)
        return plan


class UncacheTable(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self._table_name = table_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.uncache_table.table_name = self._table_name
        return plan


class ClearCache(LogicalPlan):
    def __init__(self) -> None:
        super().__init__(None)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.clear_cache.SetInParent()
        return plan


class RefreshTable(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self._table_name = table_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.refresh_table.table_name = self._table_name
        return plan


class RefreshByPath(LogicalPlan):
    def __init__(self, path: str) -> None:
        super().__init__(None)
        self._path = path

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.refresh_by_path.path = self._path
        return plan


class CurrentCatalog(LogicalPlan):
    def __init__(self) -> None:
        super().__init__(None)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.current_catalog.SetInParent()
        return plan


class SetCurrentCatalog(LogicalPlan):
    def __init__(self, catalog_name: str) -> None:
        super().__init__(None)
        self._catalog_name = catalog_name

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.set_current_catalog.catalog_name = self._catalog_name
        return plan


class ListCatalogs(LogicalPlan):
    def __init__(self, pattern: Optional[str] = None) -> None:
        super().__init__(None)
        self._pattern = pattern

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.catalog.list_catalogs.SetInParent()
        if self._pattern is not None:
            plan.catalog.list_catalogs.pattern = self._pattern
        return plan


class MapPartitions(LogicalPlan):
    """Logical plan object for a mapPartitions-equivalent API: mapInPandas, mapInArrow."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        function: "UserDefinedFunction",
        cols: List[str],
        is_barrier: bool,
        profile: Optional[ResourceProfile],
    ) -> None:
        super().__init__(child)

        self._function = function._build_common_inline_user_defined_function(*cols)
        self._is_barrier = is_barrier
        self._profile = profile

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.map_partitions.input.CopyFrom(self._child.plan(session))
        plan.map_partitions.func.CopyFrom(self._function.to_plan_udf(session))
        plan.map_partitions.is_barrier = self._is_barrier
        if self._profile is not None:
            plan.map_partitions.profile_id = self._profile.id
        return plan


class GroupMap(LogicalPlan):
    """Logical plan object for a Group Map API: apply, applyInPandas."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        grouping_cols: Sequence[Column],
        function: "UserDefinedFunction",
        cols: List[str],
    ):
        assert isinstance(grouping_cols, list) and all(isinstance(c, Column) for c in grouping_cols)

        super().__init__(child)
        self._grouping_cols = grouping_cols
        self._function = function._build_common_inline_user_defined_function(*cols)

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.group_map.input.CopyFrom(self._child.plan(session))
        plan.group_map.grouping_expressions.extend(
            [c.to_plan(session) for c in self._grouping_cols]
        )
        plan.group_map.func.CopyFrom(self._function.to_plan_udf(session))
        return plan


class CoGroupMap(LogicalPlan):
    """Logical plan object for a CoGroup Map API: applyInPandas."""

    def __init__(
        self,
        input: Optional["LogicalPlan"],
        input_grouping_cols: Sequence[Column],
        other: Optional["LogicalPlan"],
        other_grouping_cols: Sequence[Column],
        function: "UserDefinedFunction",
    ):
        assert isinstance(input_grouping_cols, list) and all(
            isinstance(c, Column) for c in input_grouping_cols
        )
        assert isinstance(other_grouping_cols, list) and all(
            isinstance(c, Column) for c in other_grouping_cols
        )

        super().__init__(input)
        self._input_grouping_cols = input_grouping_cols
        self._other_grouping_cols = other_grouping_cols
        self._other = cast(LogicalPlan, other)
        # The function takes entire DataFrame as inputs, no need to do
        # column binding (no input columns).
        self._function = function._build_common_inline_user_defined_function()

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.co_group_map.input.CopyFrom(self._child.plan(session))
        plan.co_group_map.input_grouping_expressions.extend(
            [c.to_plan(session) for c in self._input_grouping_cols]
        )
        plan.co_group_map.other.CopyFrom(self._other.plan(session))
        plan.co_group_map.other_grouping_expressions.extend(
            [c.to_plan(session) for c in self._other_grouping_cols]
        )
        plan.co_group_map.func.CopyFrom(self._function.to_plan_udf(session))
        return plan


class ApplyInPandasWithState(LogicalPlan):
    """Logical plan object for a applyInPandasWithState."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        grouping_cols: Sequence[Column],
        function: "UserDefinedFunction",
        output_schema: str,
        state_schema: str,
        output_mode: str,
        timeout_conf: str,
        cols: List[str],
    ):
        assert isinstance(grouping_cols, list) and all(isinstance(c, Column) for c in grouping_cols)

        super().__init__(child)
        self._grouping_cols = grouping_cols
        self._function = function._build_common_inline_user_defined_function(*cols)
        self._output_schema = output_schema
        self._state_schema = state_schema
        self._output_mode = output_mode
        self._timeout_conf = timeout_conf

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.apply_in_pandas_with_state.input.CopyFrom(self._child.plan(session))
        plan.apply_in_pandas_with_state.grouping_expressions.extend(
            [c.to_plan(session) for c in self._grouping_cols]
        )
        plan.apply_in_pandas_with_state.func.CopyFrom(self._function.to_plan_udf(session))
        plan.apply_in_pandas_with_state.output_schema = self._output_schema
        plan.apply_in_pandas_with_state.state_schema = self._state_schema
        plan.apply_in_pandas_with_state.output_mode = self._output_mode
        plan.apply_in_pandas_with_state.timeout_conf = self._timeout_conf
        return plan


class PythonUDTF:
    """Represents a Python user-defined table function."""

    def __init__(
        self,
        func: Type,
        return_type: Optional[Union[DataType, str]],
        eval_type: int,
        python_ver: str,
    ) -> None:
        self._func = func
        self._name = func.__name__
        self._return_type: Optional[DataType] = (
            None
            if return_type is None
            else UnparsedDataType(return_type)
            if isinstance(return_type, str)
            else return_type
        )
        self._eval_type = eval_type
        self._python_ver = python_ver

    def to_plan(self, session: "SparkConnectClient") -> proto.PythonUDTF:
        udtf = proto.PythonUDTF()
        if self._return_type is not None:
            udtf.return_type.CopyFrom(pyspark_types_to_proto_types(self._return_type))
        udtf.eval_type = self._eval_type
        try:
            udtf.command = CloudPickleSerializer().dumps(self._func)
        except pickle.PicklingError:
            raise PySparkPicklingError(
                error_class="UDTF_SERIALIZATION_ERROR",
                message_parameters={
                    "name": self._name,
                    "message": "Please check the stack trace and "
                    "make sure the function is serializable.",
                },
            )
        udtf.python_ver = self._python_ver
        return udtf

    def __repr__(self) -> str:
        return (
            f"PythonUDTF({self._name}, {self._return_type}, "
            f"{self._eval_type}, {self._python_ver})"
        )


class CommonInlineUserDefinedTableFunction(LogicalPlan):
    """
    Logical plan object for a user-defined table function with
    an inlined defined function body.
    """

    def __init__(
        self,
        function_name: str,
        function: PythonUDTF,
        deterministic: bool,
        arguments: Sequence[Expression],
    ) -> None:
        super().__init__(None)
        self._function_name = function_name
        self._deterministic = deterministic
        self._arguments = arguments
        self._function = function

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.common_inline_user_defined_table_function.function_name = self._function_name
        plan.common_inline_user_defined_table_function.deterministic = self._deterministic
        if len(self._arguments) > 0:
            plan.common_inline_user_defined_table_function.arguments.extend(
                [arg.to_plan(session) for arg in self._arguments]
            )
        plan.common_inline_user_defined_table_function.python_udtf.CopyFrom(
            self._function.to_plan(session)
        )
        return plan

    def udtf_plan(
        self, session: "SparkConnectClient"
    ) -> "proto.CommonInlineUserDefinedTableFunction":
        """
        Compared to `plan`, it returns a `proto.CommonInlineUserDefinedTableFunction`
        instead of a `proto.Relation`.
        """
        plan = proto.CommonInlineUserDefinedTableFunction()
        plan.function_name = self._function_name
        plan.deterministic = self._deterministic
        if len(self._arguments) > 0:
            plan.arguments.extend([arg.to_plan(session) for arg in self._arguments])
        plan.python_udtf.CopyFrom(
            cast(proto.PythonUDF, self._function.to_plan(session))  # type: ignore[arg-type]
        )
        return plan

    def __repr__(self) -> str:
        return f"{self._function_name}({', '.join([str(arg) for arg in self._arguments])})"


class PythonDataSource:
    """Represents a user-defined Python data source."""

    def __init__(self, data_source: Type, python_ver: str):
        self._data_source = data_source
        self._python_ver = python_ver

    def to_plan(self, session: "SparkConnectClient") -> proto.PythonDataSource:
        ds = proto.PythonDataSource()
        ds.command = CloudPickleSerializer().dumps(self._data_source)
        ds.python_ver = self._python_ver
        return ds


class CommonInlineUserDefinedDataSource(LogicalPlan):
    """Logical plan object for a user-defined data source"""

    def __init__(self, name: str, data_source: PythonDataSource) -> None:
        super().__init__(None)
        self._name = name
        self._data_source = data_source

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        plan = self._create_proto_relation()
        plan.common_inline_user_defined_data_source.name = self._name
        plan.common_inline_user_defined_data_source.python_data_source.CopyFrom(
            self._data_source.to_plan(session)
        )
        return plan

    def to_data_source_proto(
        self, session: "SparkConnectClient"
    ) -> "proto.CommonInlineUserDefinedDataSource":
        plan = proto.CommonInlineUserDefinedDataSource()
        plan.name = self._name
        plan.python_data_source.CopyFrom(self._data_source.to_plan(session))
        return plan


class CachedRelation(LogicalPlan):
    def __init__(self, plan: proto.Relation) -> None:
        super(CachedRelation, self).__init__(None)
        self._plan = plan
        # Update the plan ID based on the incremented counter.
        self._plan.common.plan_id = self._plan_id

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        return self._plan
