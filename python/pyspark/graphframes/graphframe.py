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

from __future__ import annotations

from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


class GraphFrame:
    """A graph whose vertices and edges are represented by Spark DataFrames.

    The vertex DataFrame must contain a unique ``id`` column. The edge DataFrame must contain
    ``src`` and ``dst`` columns identifying its source and destination vertices. All additional
    columns are retained as graph attributes.

    The initial in-tree API consists entirely of DataFrame operations and therefore supports both
    classic Spark and Spark Connect.
    """

    ID = "id"
    SRC = "src"
    DST = "dst"
    EDGE = "edge"

    def __init__(self, vertices: DataFrame, edges: DataFrame) -> None:
        self._require_column(vertices, self.ID, "Vertex ID")
        self._require_column(edges, self.SRC, "Source vertex ID")
        self._require_column(edges, self.DST, "Destination vertex ID")
        self._vertices = vertices
        self._edges = edges

    @property
    def vertices(self) -> DataFrame:
        """The graph's vertex DataFrame."""
        return self._vertices

    @property
    def nodes(self) -> DataFrame:
        """An alias for :attr:`vertices`."""
        return self.vertices

    @property
    def edges(self) -> DataFrame:
        """The graph's edge DataFrame."""
        return self._edges

    @property
    def triplets(self) -> DataFrame:
        """Return ``(source vertex)-[edge]->(destination vertex)`` triplets."""
        source_vertices = self.vertices.select(
            self.vertices[self.ID].alias("__graphframes_src_id"),
            self._nested(self.vertices, self.SRC),
        )
        graph_edges = self.edges.select(
            self.edges[self.SRC].alias("__graphframes_edge_src"),
            self.edges[self.DST].alias("__graphframes_edge_dst"),
            self._nested(self.edges, self.EDGE),
        )
        destination_vertices = self.vertices.select(
            self.vertices[self.ID].alias("__graphframes_dst_id"),
            self._nested(self.vertices, self.DST),
        )
        return (
            source_vertices.join(
                graph_edges,
                F.col("__graphframes_src_id") == F.col("__graphframes_edge_src"),
            )
            .join(
                destination_vertices,
                F.col("__graphframes_dst_id") == F.col("__graphframes_edge_dst"),
            )
            .select(self.SRC, self.EDGE, self.DST)
        )

    @property
    def outDegrees(self) -> DataFrame:
        """Return the out-degree of vertices having at least one outgoing edge."""
        return self.edges.groupBy(self.edges[self.SRC].alias(self.ID)).agg(
            F.count("*").cast("int").alias("outDegree")
        )

    @property
    def inDegrees(self) -> DataFrame:
        """Return the in-degree of vertices having at least one incoming edge."""
        return self.edges.groupBy(self.edges[self.DST].alias(self.ID)).agg(
            F.count("*").cast("int").alias("inDegree")
        )

    @property
    def degrees(self) -> DataFrame:
        """Return the total degree of vertices incident to at least one edge."""
        return (
            self.edges.select(
                F.explode(F.array(self.edges[self.SRC], self.edges[self.DST])).alias(self.ID)
            )
            .groupBy(self.ID)
            .agg(F.count("*").cast("int").alias("degree"))
        )

    def cache(self) -> "GraphFrame":
        """Persist the vertex and edge DataFrames with their default storage level."""
        self.vertices.cache()
        self.edges.cache()
        return self

    def persist(
        self, storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER
    ) -> "GraphFrame":
        """Persist the vertex and edge DataFrames with ``storage_level``."""
        self.vertices.persist(storage_level)
        self.edges.persist(storage_level)
        return self

    def unpersist(self, blocking: bool = False) -> "GraphFrame":
        """Remove the vertex and edge DataFrames from the cache."""
        self.vertices.unpersist(blocking)
        self.edges.unpersist(blocking)
        return self

    def filterVertices(self, condition: Union[Column, str]) -> "GraphFrame":
        """Filter vertices and remove edges incident to any removed vertex."""
        filtered_vertices = self.vertices.filter(condition)
        vertex_ids = filtered_vertices.select(filtered_vertices[self.ID])
        filtered_edges = self.edges.join(
            vertex_ids,
            self.edges[self.SRC] == vertex_ids[self.ID],
            "left_semi",
        ).join(
            vertex_ids,
            self.edges[self.DST] == vertex_ids[self.ID],
            "left_semi",
        )
        return GraphFrame(filtered_vertices, filtered_edges)

    def filterEdges(self, condition: Union[Column, str]) -> "GraphFrame":
        """Filter edges while keeping all vertices."""
        return GraphFrame(self.vertices, self.edges.filter(condition))

    def dropIsolatedVertices(self) -> "GraphFrame":
        """Return a graph without vertices that are not incident to an edge."""
        incident_ids = self.edges.select(
            F.explode(F.array(self.edges[self.SRC], self.edges[self.DST])).alias(self.ID)
        )
        return GraphFrame(self.vertices.join(incident_ids, self.ID, "left_semi"), self.edges)

    def as_reversed(self) -> "GraphFrame":
        """Return a graph with the direction of every edge reversed."""
        attributes = [
            self.edges[name] for name in self.edges.columns if name not in {self.SRC, self.DST}
        ]
        reversed_edges = self.edges.select(
            self.edges[self.DST].alias(self.SRC),
            self.edges[self.SRC].alias(self.DST),
            *attributes,
        )
        return GraphFrame(self.vertices, reversed_edges)

    def as_undirected(self) -> "GraphFrame":
        """Return an undirected graph by adding a reversed copy of every edge."""
        return GraphFrame(self.vertices, self.edges.unionByName(self.as_reversed().edges))

    def validate(self) -> None:
        """Run jobs that validate vertex uniqueness and edge endpoint integrity."""
        vertex_count = self.vertices.count()
        distinct_vertex_count = self.vertices.select(self.ID).distinct().count()
        if vertex_count != distinct_vertex_count:
            raise ValueError(
                f"Graph contains {vertex_count - distinct_vertex_count} duplicate vertices"
            )

        endpoints = (
            self.edges.select(self.edges[self.SRC].alias(self.ID))
            .union(self.edges.select(self.edges[self.DST].alias(self.ID)))
            .distinct()
        )
        missing_endpoint_count = endpoints.join(self.vertices, self.ID, "left_anti").count()
        if missing_endpoint_count:
            raise ValueError(
                f"Graph contains {missing_endpoint_count} edge endpoints without matching vertices"
            )

    @classmethod
    def from_edges(
        cls,
        edges: DataFrame,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> "GraphFrame":
        """Create a graph by deriving and persisting distinct vertices from ``edges``."""
        cls._require_column(edges, cls.SRC, "Source vertex ID")
        cls._require_column(edges, cls.DST, "Destination vertex ID")
        vertices = (
            edges.select(edges[cls.SRC].alias(cls.ID))
            .union(edges.select(edges[cls.DST].alias(cls.ID)))
            .distinct()
            .persist(storage_level)
        )
        return cls(vertices, edges)

    def __repr__(self) -> str:
        vertex_columns = [self.ID] + [name for name in self.vertices.columns if name != self.ID]
        edge_columns = [self.SRC, self.DST] + [
            name for name in self.edges.columns if name not in {self.SRC, self.DST}
        ]
        return (
            f"GraphFrame(v:{self.vertices.select(*vertex_columns)!r}, "
            f"e:{self.edges.select(*edge_columns)!r})"
        )

    @staticmethod
    def _nested(dataframe: DataFrame, name: str) -> Column:
        return F.struct(*[dataframe[column] for column in dataframe.columns]).alias(name)

    @staticmethod
    def _require_column(dataframe: DataFrame, column: str, label: str) -> None:
        if column not in dataframe.columns:
            available = ", ".join(dataframe.columns)
            raise ValueError(
                f"{label} column '{column}' is missing; available columns: {available}"
            )
