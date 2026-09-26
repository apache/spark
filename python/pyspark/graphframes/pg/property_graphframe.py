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

"""PropertyGraphFrame implementation for PySpark."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from pyspark.graphframes.pg.property_groups import EdgePropertyGroup, VertexPropertyGroup
from pyspark.sql.functions import col, lit

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

from pyspark.graphframes import GraphFrame


class PropertyGraphFrame:
    """
    A high-level abstraction for working with property graphs in PySpark.

    PropertyGraphFrame serves as a logical structure that manages collections of vertex and edge
    property groups, providing a user-friendly API for graph operations. It handles various
    internal complexities such as:

    - ID conversion and collision prevention
    - Management of directed/undirected graph representations
    - Handling of weighted/unweighted edges
    - Data consistency across different property groups

    The class maintains separate collections for vertex and edge properties, allowing for flexible
    graph construction while ensuring data integrity.

    Example:
        >>> from pyspark.graphframes.pg import VertexPropertyGroup, EdgePropertyGroup, PropertyGraphFrame
        >>> from pyspark.graphframes import GraphFrame
        >>>
        >>> # Create vertex groups
        >>> people_data = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> people_group = VertexPropertyGroup("people", people_data, "id")
        >>>
        >>> movies_data = spark.createDataFrame([(1, "Matrix"), (2, "Inception")], ["id", "title"])
        >>> movies_group = VertexPropertyGroup("movies", movies_data, "id")
        >>>
        >>> # Create edge group
        >>> likes_data = spark.createDataFrame([(1, 1, 1.0)], ["src", "dst", "weight"])
        >>> likes_group = EdgePropertyGroup(
        ...     "likes", likes_data, people_group, movies_group,
        ...     is_directed=False, src_column_name="src", dst_column_name="dst",
        ...     weight_column_name="weight"
        ... )
        >>>
        >>> # Create property graph
        >>> pg = PropertyGraphFrame([people_group, movies_group], [likes_group])

    :param vertex_property_groups: Sequence of vertex property groups
    :param edges_property_groups: Sequence of edge property groups
    """

    PROPERTY_GROUP_COL_NAME = "property_group"
    EXTERNAL_ID = "external_id"

    def __init__(
        self,
        vertex_property_groups: Sequence,
        edges_property_groups: Sequence,
    ) -> None:
        """
        Initialize a PropertyGraphFrame.

        :param vertex_property_groups: Sequence of vertex property groups
        :param edges_property_groups: Sequence of edge property groups
        """

        # Validate input types
        for group in vertex_property_groups:
            if not isinstance(group, VertexPropertyGroup):
                raise TypeError(
                    f"All vertex_property_groups must be VertexPropertyGroup instances, "
                    f"got {type(group)}"
                )

        for group in edges_property_groups:
            if not isinstance(group, EdgePropertyGroup):
                raise TypeError(
                    f"All edges_property_groups must be EdgePropertyGroup instances, "
                    f"got {type(group)}"
                )

        self._vertex_property_groups = list(vertex_property_groups)
        self._edges_property_groups = list(edges_property_groups)

        # Create lookup maps
        self._vertex_groups: dict[str, VertexPropertyGroup] = {
            group.name: group for group in self._vertex_property_groups
        }
        self._edge_groups: dict[str, EdgePropertyGroup] = {
            group.name: group for group in self._edges_property_groups
        }

    @property
    def vertex_property_groups(self) -> list[VertexPropertyGroup]:
        """Return the list of vertex property groups."""

        return self._vertex_property_groups

    @property
    def edges_property_groups(self) -> list[EdgePropertyGroup]:
        """Return the list of edge property groups."""

        return self._edges_property_groups

    def to_graphframe(
        self,
        vertex_property_groups: Sequence[str],
        edge_property_groups: Sequence[str],
        edge_group_filters: dict[str, Column] | None = None,
        vertex_group_filters: dict[str, Column] | None = None,
    ) -> GraphFrame:
        """
        Convert the property graph to a unified GraphFrame representation.

        This method transforms a property graph that may contain multiple vertex types and both
        directed and undirected edges into a single GraphFrame object where all vertices and edges
        share the same schema. The conversion process handles:

        - Internal ID generation and collision prevention by hashing vertex/edge IDs with their
          group names
        - Merging of different vertex types into a unified vertex DataFrame
        - Conversion of directed/undirected edge relationships into a consistent edge DataFrame
        - Filtering of vertices and edges based on provided predicates

        :param vertex_property_groups: Sequence of vertex property group names to include
        :param edge_property_groups: Sequence of edge property group names to include
        :param edge_group_filters: Optional dict mapping edge group names to filter predicates
        :param vertex_group_filters: Optional dict mapping vertex group names to filter predicates
        :return: A GraphFrame containing the unified representation
        :raises ValueError: If a specified group name does not exist

        Example:
            >>> from pyspark.sql.functions import lit
            >>> graph = pg.to_graph_frame(
            ...     vertex_property_groups=["people", "movies"],
            ...     edge_property_groups=["likes", "messages"],
            ...     edge_group_filters={"likes": lit(True), "messages": lit(True)},
            ...     vertex_group_filters={"people": lit(True), "movies": lit(True)}
            ... )
        """
        # Set default filters if not provided
        if edge_group_filters is None:
            edge_group_filters = {}
        if vertex_group_filters is None:
            vertex_group_filters = {}

        # Validate group names
        for name in vertex_property_groups:
            if name not in self._vertex_groups:
                raise ValueError(f"Vertex property group '{name}' does not exist")

        for name in edge_property_groups:
            if name not in self._edge_groups:
                raise ValueError(f"Edge property group '{name}' does not exist")

        # Combine vertices from all specified groups
        if not vertex_property_groups:
            raise ValueError("At least one vertex property group must be specified")

        vertices_list = []
        for name in vertex_property_groups:
            filter_col = vertex_group_filters.get(name, lit(True))
            group_data = self._vertex_groups[name].get_data(filter_col)
            vertices_list.append(group_data)

        vertices = vertices_list[0]
        for v in vertices_list[1:]:
            vertices = vertices.union(v)

        # Combine edges from all specified groups
        if not edge_property_groups:
            raise ValueError("At least one edge property group must be specified")

        edges_list = []
        for name in edge_property_groups:
            filter_col = edge_group_filters.get(name, lit(True))
            group_data = self._edge_groups[name].get_data(filter_col)
            edges_list.append(group_data)

        edges = edges_list[0]
        for e in edges_list[1:]:
            edges = edges.union(e)

        return GraphFrame(vertices, edges)

    def projection_by(
        self,
        left_bi_graph_part: str,
        right_bi_graph_part: str,
        edge_group: str,
        new_edge_weight: Callable[[Column, Column], Column] | None = None,
    ) -> "PropertyGraphFrame":
        """
        Project a bipartite graph onto one of its parts.

        Creates edges between vertices that share neighbors in the other part. Drops the property
        group used for projection and returns a new property graph.

        :param left_bi_graph_part: Name of the vertex property group to project onto
        :param right_bi_graph_part: Name of the vertex property group to project through
        :param edge_group: Name of the edge property group connecting the two parts
        :param new_edge_weight: Optional function that takes two weight columns and returns
                               a new weight column. If None, uses weight 1.0 for all edges.
        :return: A new PropertyGraphFrame containing the projected graph
        :raises ValueError: If group names are invalid or edge group doesn't connect the parts

        Example:
            >>> # Project people through movies they both like
            >>> projected = pg.projection_by("people", "movies", "likes")
            >>> # Custom weight function
            >>> from pyspark.sql.functions import col
            >>> projected = pg.projection_by(
            ...     "people", "movies", "likes",
            ...     new_edge_weight=lambda w1, w2: w1 + w2
            ... )
        """
        # Validate inputs
        if edge_group not in self._edge_groups:
            raise ValueError(f"Edge property group '{edge_group}' does not exist")

        if left_bi_graph_part not in self._vertex_groups:
            raise ValueError(f"Vertex property group '{left_bi_graph_part}' does not exist")

        if right_bi_graph_part not in self._vertex_groups:
            raise ValueError(f"Vertex property group '{right_bi_graph_part}' does not exist")

        old_group = self._edge_groups[edge_group]

        # Validate edge group connects the specified parts
        if old_group.src_property_group.name != left_bi_graph_part:
            raise ValueError(
                f"Edge property group should have '{left_bi_graph_part}' as source "
                f"but has '{old_group.src_property_group.name}'"
            )

        if old_group.dst_property_group.name != right_bi_graph_part:
            raise ValueError(
                f"Edge property group should have '{right_bi_graph_part}' as destination "
                f"but has '{old_group.dst_property_group.name}'"
            )

        # Get vertex groups to keep
        kept_v_property_groups = [
            g for g in self._vertex_property_groups if g.name != right_bi_graph_part
        ]

        # Get edge groups to keep (excluding the one being projected)
        kept_e_property_groups = [g for g in self._edges_property_groups if g.name != edge_group]

        # Create projected edges by joining edges through common neighbors
        old_edges_data = old_group.data

        e1 = old_edges_data.alias("e1")
        e2 = old_edges_data.alias("e2")

        # Join edges on common destination (the right part)
        joined = e1.join(
            e2, col("e1." + old_group.dst_column_name) == col("e2." + old_group.dst_column_name)
        )

        # Filter to avoid duplicates (e1.src < e2.src)
        joined = joined.filter(
            col("e1." + old_group.src_column_name) < col("e2." + old_group.src_column_name)
        )

        # Add weight column
        if new_edge_weight is not None:
            w1 = col(f"e1.{old_group.weight_column_name}")
            w2 = col(f"e2.{old_group.weight_column_name}")
            weight_col = new_edge_weight(w1, w2)
        else:
            weight_col = lit(1.0)

        # Select source and destination for new edges
        projected_edges = joined.select(
            col("e1." + old_group.src_column_name).alias(GraphFrame.SRC),
            col("e2." + old_group.src_column_name).alias(GraphFrame.DST),
            weight_col.alias(GraphFrame.WEIGHT),
        )

        # Create new edge property group
        left_group = self._vertex_groups[left_bi_graph_part]

        new_edge_group = EdgePropertyGroup(
            name=f"projected_{edge_group}",
            data=projected_edges,
            src_property_group=left_group,
            dst_property_group=left_group,
            is_directed=False,
            src_column_name=GraphFrame.SRC,
            dst_column_name=GraphFrame.DST,
            weight_column_name=GraphFrame.WEIGHT,
        )

        return PropertyGraphFrame(kept_v_property_groups, kept_e_property_groups + [new_edge_group])

    def join_vertices(
        self,
        vertices_data: DataFrame,
        vertex_groups: Sequence[str],
    ) -> DataFrame:
        """
        Join algorithm results back to the original vertex data.

        Joins the vertices data (typically output from graph algorithms) with the specified
        vertex property groups to produce a unified DataFrame with original vertex attributes.

        :param vertices_data: DataFrame containing vertex algorithm results (from to_graph_frame)
        :param vertex_groups: Sequence of vertex group names to join
        :return: A DataFrame with joined vertex data
        :raises ValueError: If a specified group name does not exist

        Example:
            >>> # Run connected components and join results back
            >>> graph = pg.to_graph_frame(["people"], ["messages"], {}, {})
            >>> components = graph.connectedComponents()
            >>> joined = pg.join_vertices(components, ["people"])
        """
        # Validate group names
        for name in vertex_groups:
            if name not in self._vertex_groups:
                raise ValueError(f"Vertex property group '{name}' does not exist")

        if not vertex_groups:
            raise ValueError("At least one vertex group must be specified")

        # Join each group separately
        result_dfs = []

        for vg_name in vertex_groups:
            group = self._vertex_groups[vg_name]

            # Filter vertices data for this group
            filtered = vertices_data.filter(
                col(PropertyGraphFrame.PROPERTY_GROUP_COL_NAME) == lit(vg_name)
            )

            if group.apply_mask_on_id:
                # Use internal ID mapping to join back to original data
                id_mapping = group._get_internal_id_mapping()
                joined = id_mapping.join(filtered, [GraphFrame.ID], "left").drop(GraphFrame.ID)
            else:
                # Direct join on ID
                joined = (
                    group.get_data()
                    .join(filtered, GraphFrame.ID, "left")
                    .withColumnRenamed(GraphFrame.ID, PropertyGraphFrame.EXTERNAL_ID)
                )

            result_dfs.append(joined)

        # Union all results
        result = result_dfs[0]
        for df in result_dfs[1:]:
            result = result.union(df)

        return result
