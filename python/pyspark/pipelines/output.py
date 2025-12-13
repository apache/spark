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
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Union

from pyspark.pipelines.source_code_location import SourceCodeLocation
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class Output:
    """Base class for definitions of outputs in a pipeline dataflow graph.

    :param name: The name of the outputs. May be a multi-part name, such as "db.table".
    :param comment: Optional comment for the output.
    :param source_code_location: The location of the source code that created this output.
        This is used for debugging and tracing purposes.
    """

    name: str
    comment: Optional[str]
    source_code_location: SourceCodeLocation


@dataclass(frozen=True)
class Table(Output):
    """
    Definition of a table in a pipeline dataflow graph, i.e. a catalog object backed by data in
    physical storage.

    :param table_properties: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param cluster_by: A list containing the column names of the cluster columns.
    :param schema Explicit Spark SQL schema to materialize this table with. Supports either a
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    :param format: The format of the table, e.g. "parquet".
    """

    table_properties: Mapping[str, str]
    partition_cols: Optional[Sequence[str]]
    cluster_by: Optional[Sequence[str]]
    schema: Optional[Union[StructType, str]]
    format: Optional[str]


@dataclass(frozen=True)
class MaterializedView(Table):
    """Definition of a materialized view in a pipeline dataflow graph. A materialized view is a
    table whose contents are defined to be the result of a query."""


@dataclass(frozen=True)
class StreamingTable(Table):
    """Definition of a streaming table in a pipeline dataflow graph. A streaming table is a
    table whose contents are produced by one or more streaming flows."""


@dataclass(frozen=True)
class TemporaryView(Output):
    """Definition of a temporary view in a pipeline dataflow graph. Temporary views can be
    referenced by flows within the dataflow graph, but are not visible outside of the graph."""

    pass


@dataclass(frozen=True)
class Sink(Output):
    """Definition of an external sink in a pipeline dataflow graph. An external sink's
    contents are written to an external system rather than managed by the pipeline itself."""

    format: str
    options: Mapping[str, str]
