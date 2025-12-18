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
import abc
import dataclasses
from typing import Optional, List, Tuple, Dict, Any, Union, TYPE_CHECKING, Sequence

from pyspark.errors import PySparkValueError

if TYPE_CHECKING:
    try:
        import graphviz  # type: ignore
    except ImportError:
        pass


class ObservedMetrics(abc.ABC):
    @property
    @abc.abstractmethod
    def name(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def pairs(self) -> Dict[str, Any]:
        ...

    @property
    @abc.abstractmethod
    def keys(self) -> List[str]:
        ...


class MetricValue:
    """The metric values is the Python representation of a plan metric value from the JVM.
    However, it does not have any reference to the original value."""

    def __init__(self, name: str, value: Union[int, float], type: str):
        self._name = name
        self._type = type
        self._value = value

    def __repr__(self) -> str:
        return f"<{self._name}={self._value} ({self._type})>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> Union[int, float]:
        return self._value

    @property
    def metric_type(self) -> str:
        return self._type

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable dictionary representation of this metric value.

        Returns
        -------
        dict
            A dictionary with keys 'name', 'value', and 'type'.
        """
        return {
            "name": self._name,
            "value": self._value,
            "type": self._type,
        }


class PlanMetrics:
    """Represents a particular plan node and the associated metrics of this node."""

    def __init__(self, name: str, id: int, parent: int, metrics: List[MetricValue]):
        self._name = name
        self._id = id
        self._parent_id = parent
        self._metrics = metrics

    def __repr__(self) -> str:
        return f"Plan({self._name}: {self._id}->{self._parent_id})={self._metrics}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def plan_id(self) -> int:
        return self._id

    @property
    def parent_plan_id(self) -> int:
        return self._parent_id

    @property
    def metrics(self) -> List[MetricValue]:
        return self._metrics

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable dictionary representation of this plan metrics.

        Returns
        -------
        dict
            A dictionary with keys 'name', 'plan_id', 'parent_plan_id', and 'metrics'.
        """
        return {
            "name": self._name,
            "plan_id": self._id,
            "parent_plan_id": self._parent_id,
            "metrics": [m.to_dict() for m in self._metrics],
        }


class CollectedMetrics:
    @dataclasses.dataclass
    class Node:
        id: int
        name: str = dataclasses.field(default="")
        metrics: List[MetricValue] = dataclasses.field(default_factory=list)
        children: List[int] = dataclasses.field(default_factory=list)

    def text(self, current: "Node", graph: Dict[int, "Node"], prefix: str = "") -> str:
        """
        Converts the current node and its children into a textual representation. This is used
        to provide a usable output for the command line or other text-based interfaces. However,
        it is recommended to use the Graphviz representation for a more visual representation.

        Parameters
        ----------
        current: Node
            Current node in the graph.
        graph: dict
            A dictionary representing the full graph mapping from node ID (int) to the node itself.
            The node is an instance of :class:`CollectedMetrics:Node`.
        prefix: str
            String prefix used for generating the output buffer.

        Returns
        -------
        The full string representation of the current node as root.
        """
        base_metrics = set(["numPartitions", "peakMemory", "numOutputRows", "spillSize"])

        # Format the metrics of this node:
        metric_buffer = []
        for m in current.metrics:
            if m.name in base_metrics:
                metric_buffer.append(f"{m.name}: {m.value} ({m.metric_type})")

        buffer = f"{prefix}+- {current.name}({','.join(metric_buffer)})\n"
        for i, child in enumerate(current.children):
            c = graph[child]
            new_prefix = prefix + "  " if i == len(c.children) - 1 else prefix
            if current.id != c.id:
                buffer += self.text(c, graph, new_prefix)
        return buffer

    def __init__(self, metrics: List[PlanMetrics]):
        # Sort the input list
        self._metrics = sorted(metrics, key=lambda x: x._parent_id, reverse=False)

    def extract_graph(self) -> Tuple[int, Dict[int, "CollectedMetrics.Node"]]:
        """
        Builds the graph of the query plan. The graph is represented as a dictionary where the key
        is the node ID and the value is the node itself. The root node is the node that has no
        parent.

        Returns
        -------
        The root node ID and the graph of all nodes.
        """
        all_nodes: Dict[int, CollectedMetrics.Node] = {}

        for m in self._metrics:
            # Add yourself to the list if you have to.
            if m.plan_id not in all_nodes:
                all_nodes[m.plan_id] = CollectedMetrics.Node(m.plan_id, m.name, m.metrics)
            else:
                all_nodes[m.plan_id].name = m.name
                all_nodes[m.plan_id].metrics = m.metrics

            # Now check for the parent of this node if it's in
            if m.parent_plan_id not in all_nodes:
                all_nodes[m.parent_plan_id] = CollectedMetrics.Node(m.parent_plan_id)

            all_nodes[m.parent_plan_id].children.append(m.plan_id)

        # Next step is to find all the root nodes. Root nodes are never used in children.
        # So we start with all node ids as candidates.
        candidates = set(all_nodes.keys())
        for k, v in all_nodes.items():
            for c in v.children:
                if c in candidates and c != k:
                    candidates.remove(c)

        assert len(candidates) == 1, f"Expected 1 root node, found {len(candidates)}"
        return candidates.pop(), all_nodes

    def toText(self) -> str:
        """
        Converts the execution graph from a graph into a textual representation
        that can be read at the command line for example.

        Returns
        -------
        A string representation of the collected metrics.
        """
        root, graph = self.extract_graph()
        return self.text(graph[root], graph)

    def toDot(self, filename: Optional[str] = None, out_format: str = "png") -> "graphviz.Digraph":
        """
        Converts the collected metrics into a dot representation. Since the graphviz Digraph
        implementation provides the ability to render the result graph directory in a
        notebook, we return the graph object directly.

        If the graphviz package is not available, a PACKAGE_NOT_INSTALLED error is raised.

        Parameters
        ----------
        filename : str, optional
            The filename to save the graph to given an output format. The path can be
            relative or absolute.

        out_format : str
            The output format of the graph. The default is 'png'.

        Returns
        -------
        An instance of the graphviz.Digraph object.
        """
        try:
            import graphviz

            dot = graphviz.Digraph(
                comment="Query Plan",
                node_attr={
                    "shape": "box",
                    "font-size": "10pt",
                },
            )

            root, graph = self.extract_graph()
            for k, v in graph.items():
                # Build table rows for the metrics
                rows = "\n".join(
                    [
                        (
                            f'<TR><TD><FONT POINT-SIZE="8">{x.name}</FONT></TD><TD>'
                            f'<FONT POINT-SIZE="8">{x.value} ({x.metric_type})</FONT></TD></TR>'
                        )
                        for x in v.metrics
                    ]
                )

                dot.node(
                    str(k),
                    """<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
                    <TR>
                        <TD COLSPAN="2" BGCOLOR="lightgrey">
                            <FONT POINT-SIZE=\"10\">{}</FONT>
                        </TD>
                    </TR>
                    <TR><TD COLSPAN="2"><FONT POINT-SIZE=\"10\">Metrics</FONT></TD></TR>
                    {}
                    </TABLE>>""".format(
                        v.name, rows
                    ),
                )
                for c in v.children:
                    dot.edge(str(k), str(c))

            if filename:
                dot.render(filename, format=out_format, cleanup=True)
            return dot

        except ImportError:
            raise PySparkValueError(
                errorClass="PACKAGE_NOT_INSTALLED",
                messageParameters={"package_name": "graphviz", "minimum_version": "0.20"},
            )


class ExecutionInfo:
    """The ExecutionInfo class allows users to inspect the query execution of this particular
    data frame. This value is only set in the data frame if it was executed."""

    def __init__(
        self, metrics: Optional[list[PlanMetrics]], obs: Optional[Sequence[ObservedMetrics]]
    ):
        self._metrics = CollectedMetrics(metrics) if metrics else None
        self._observations = obs if obs else []

    @property
    def metrics(self) -> Optional[CollectedMetrics]:
        return self._metrics

    @property
    def flows(self) -> List[Tuple[str, Dict[str, Any]]]:
        return [(f.name, f.pairs) for f in self._observations]
