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

from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.pipelines.output import Output
from pyspark.pipelines.flow import Flow
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator, Optional

from pyspark.errors import PySparkRuntimeError


class GraphElementRegistry(ABC):
    """
    Abstract base class for graph element registries. This class is used to register datasets and
    flows. The concrete implementations of this class should provide the actual storage and
    retrieval mechanisms for the datasets and flows.
    """

    @abstractmethod
    def register_output(self, output: Output) -> None:
        """Add the given dataset to the registry."""

    @abstractmethod
    def register_flow(self, flow: Flow) -> None:
        """Add the given flow to the registry."""

    @abstractmethod
    def register_sql(self, sql_text: str, file_path: Path) -> None:
        """Register a string containing SQL statements the dataflow graph.

        :param sql: The SQL text, containing one or more statements that define graph elements, to
            register.
        :param file_path: The path to the file that the SQL txt came from.
        """


_graph_element_registry_context_var: ContextVar[Optional[GraphElementRegistry]] = ContextVar(
    "graph_element_registry_context", default=None
)


@contextmanager
def graph_element_registration_context(
    registry: GraphElementRegistry,
) -> Generator[None, None, None]:
    """
    Context manager that sets the active graph element registry, in a thread-local variable, for the
    duration of the context.
    """
    token = _graph_element_registry_context_var.set(registry)
    try:
        yield
    finally:
        _graph_element_registry_context_var.reset(token)


def get_active_graph_element_registry() -> GraphElementRegistry:
    graph = _graph_element_registry_context_var.get()
    if graph is None:
        raise PySparkRuntimeError(
            errorClass="GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE",
            messageParameters={},
        )

    return graph
