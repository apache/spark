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

import warnings
from typing import TYPE_CHECKING, Any, cast, final

from typing_extensions import override

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


class AggregateNeighbors:
    """Helper class for referencing attributes in AggregateNeighbors expressions.

    Use these static methods in accumulator update expressions, stopping conditions,
    and target conditions to reference vertex and edge attributes.

    **Example:**

    >>> result = g.aggregate_neighbors(
    ...     starting_vertices=F.col("id") == 1,
    ...     max_hops=5,
    ...     accumulator_names=["sum_values"],
    ...     accumulator_inits=[F.lit(0)],
    ...     accumulator_updates=[
    ...         F.col("sum_values") + AggregateNeighbors.dst_attr("value")
    ...     ],
    ...     target_condition=AggregateNeighbors.dst_attr("id") == 10
    ... )
    """

    @staticmethod
    def src_attr(colName: str) -> Column:
        """Reference a source vertex attribute.

        :param colName: Name of the source vertex attribute
        :return: Column expression referencing the attribute
        """
        return F.col("src_attributes").getField(colName)

    @staticmethod
    def dst_attr(colName: str) -> Column:
        """Reference a destination vertex attribute.

        :param colName: Name of the destination vertex attribute
        :return: Column expression referencing the attribute
        """
        return F.col("dst_attributes").getField(colName)

    @staticmethod
    def edge_attr(colName: str) -> Column:
        """Reference an edge attribute.

        :param colName: Name of the edge attribute
        :return: Column expression referencing the attribute
        """
        return F.col("edge_attributes").getField(colName)


from pyspark.graphframes.internal.utils import (
    _HASH2VEC_DECAY_FUNCTIONS,
    _RandomWalksEmbeddingsParameters,
)
from pyspark.sql.utils import is_remote

if TYPE_CHECKING:
    from pyspark.graphframes.lib import Pregel
    from pyspark.sql import Column, DataFrame

"""Constant for the vertices ID column name."""
ID = "id"

"""Constant for the edge src column name."""
SRC = "src"

"""Constant for the edge dst column name."""
DST = "dst"

"""Constant for the edge column name."""
EDGE = "edge"

"""Constant for the weight column name."""
WEIGHT = "weight"


class GraphFrame:
    """
    Represents a graph with vertices and edges stored as DataFrames.

    :param v:  :class:`DataFrame` holding vertex information.
               Must contain a column named "id" that stores unique
               vertex IDs.
    :param e:  :class:`DataFrame` holding edge information.
               Must contain two columns "src" and "dst" storing source
               vertex IDs and destination vertex IDs of edges, respectively.

    >>> localVertices = [(1,"A"), (2,"B"), (3, "C")]
    >>> localEdges = [(1,2,"love"), (2,1,"hate"), (2,3,"follow")]
    >>> v = spark.createDataFrame(localVertices, ["id", "name"])
    >>> e = spark.createDataFrame(localEdges, ["src", "dst", "action"])
    >>> g = GraphFrame(v, e)
    """

    ID: str = ID
    SRC: str = SRC
    DST: str = DST
    EDGE: str = EDGE
    WEIGHT: str = WEIGHT

    @staticmethod
    def _from_impl(impl: Any) -> "GraphFrame":
        return GraphFrame(impl._vertices, impl._edges)

    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        """
        Initialize a GraphFrame from vertex DataFrame and edges DataFrame.

        :param v: :class:`DataFrame` holding vertex information.
                Must contain a column named "id" that stores unique
                vertex IDs.
        :param e: :class:`DataFrame` holding edge information.
                Must contain two columns "src" and "dst" storing source
                vertex IDs and destination vertex IDs of edges, respectively.
        """
        self._impl: Any
        if self.ID not in v.columns:
            raise ValueError(
                "Vertex ID column {} missing from vertex DataFrame, which has columns: {}".format(
                    self.ID, ",".join(v.columns)
                )
            )
        if self.SRC not in e.columns:
            raise ValueError(
                "Source vertex ID column {} missing from edge DataFrame, which has columns: {}".format(
                    self.SRC, ",".join(e.columns)
                )
            )
        if self.DST not in e.columns:
            raise ValueError(
                "Destination vertex ID column {} missing from edge DataFrame, which has columns: {}".format(
                    self.DST, ",".join(e.columns)
                )
            )
        if is_remote():
            from pyspark.graphframes.connect.graphframes_client import GraphFrameConnect

            self._impl = GraphFrameConnect(cast(Any, v), cast(Any, e))  # ty: ignore[invalid-argument-type]
        else:
            from pyspark.graphframes.classic.graphframe import GraphFrame as GraphFrameClassic

            self._impl = GraphFrameClassic(cast(Any, v), cast(Any, e))  # ty: ignore[invalid-argument-type]

    @property
    def vertices(self) -> DataFrame:
        """
        :class:`DataFrame` holding vertex information, with unique column "id"
        for vertex IDs.
        """
        return self._impl._vertices

    @property
    def edges(self) -> DataFrame:
        """
        :class:`DataFrame` holding edge information, with unique columns "src" and
        "dst" storing source vertex IDs and destination vertex IDs of edges,
        respectively.
        """
        return self._impl._edges

    @property
    def nodes(self) -> DataFrame:
        """Alias to vertices."""
        return self.vertices

    @override
    def __repr__(self) -> str:
        # Exactly like in the scala core
        v_cols = [self.ID] + [col for col in self._impl._vertices.columns if col != self.ID]
        e_cols = [self.SRC, self.DST] + [
            col for col in self._impl._edges.columns if col not in {self.SRC, self.DST}
        ]
        v = self._impl._vertices.select(*v_cols).__repr__()
        e = self._impl._edges.select(*e_cols).__repr__()

        return f"GraphFrame(v:{v}, e:{e})"

    def cache(self) -> "GraphFrame":
        """Persist the dataframe representation of vertices and edges of the graph with the default
        storage level.
        """
        new_vertices = self._impl._vertices.cache()
        new_edges = self._impl._edges.cache()
        return GraphFrame(new_vertices, new_edges)

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "GraphFrame":
        """Persist the dataframe representation of vertices and edges of the graph with the given
        storage level.
        """
        new_vertices = self._impl._vertices.persist(storageLevel=storageLevel)
        new_edges = self._impl._edges.persist(storageLevel=storageLevel)
        return GraphFrame(new_vertices, new_edges)

    def unpersist(self, blocking: bool = False) -> "GraphFrame":
        """Mark the dataframe representation of vertices and edges of the graph as non-persistent,
        and remove all blocks for it from memory and disk.
        """
        new_vertices = self._impl._vertices.unpersist(blocking=blocking)
        new_edges = self._impl._edges.unpersist(blocking=blocking)
        return GraphFrame(new_vertices, new_edges)

    @property
    def outDegrees(self) -> DataFrame:
        """
        The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - "outDegree" (integer) storing the out-degree of the vertex

        Note that vertices with 0 out-edges are not returned in the result.

        :return:  DataFrame with new vertices column "outDegree"
        """
        return self._impl._edges.groupBy(F.col(self.SRC).alias(self.ID)).agg(
            F.count("*").alias("outDegree")
        )

    @property
    def inDegrees(self) -> DataFrame:
        """
        The in-degree of each vertex in the graph, returned as a DataFame with two columns:
         - "id": the ID of the vertex
         - "inDegree" (int) storing the in-degree of the vertex

        Note that vertices with 0 in-edges are not returned in the result.

        :return:  DataFrame with new vertices column "inDegree"
        """
        return self._impl._edges.groupBy(F.col(self.DST).alias(self.ID)).agg(
            F.count("*").alias("inDegree")
        )

    @property
    def degrees(self) -> DataFrame:
        """
        The degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - 'degree' (integer) the degree of the vertex

        Note that vertices with 0 edges are not returned in the result.

        :return:  DataFrame with new vertices column "degree"
        """
        return (
            self._impl._edges.select(
                F.explode(F.array(F.col(self.SRC), F.col(self.DST))).alias(self.ID)
            )
            .groupBy(self.ID)
            .agg(F.count("*").alias("degree"))
        )

    def type_out_degree(self, edge_type_col: str, edge_types: list[Any] | None = None) -> DataFrame:
        """
        The out-degree of each vertex per edge type, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - "outDegrees": a struct with a field for each edge type, storing the out-degree count

        :param edge_type_col: Name of the column in edges DataFrame that contains edge types
        :param edge_types: Optional list of edge type values. If None, edge types will be discovered automatically.
        :return: DataFrame with columns "id" and "outDegrees" (struct type)
        """
        if edge_types is not None:
            pivot_df = self._impl._edges.groupBy(F.col(self.SRC).alias(self.ID)).pivot(
                edge_type_col, edge_types
            )
        else:
            pivot_df = self._impl._edges.groupBy(F.col(self.SRC).alias(self.ID)).pivot(
                edge_type_col
            )

        count_df = pivot_df.agg(F.count(F.lit(1))).na.fill(0)
        struct_cols = [
            F.col(col_name).cast("int").alias(col_name)
            for col_name in count_df.columns
            if col_name != self.ID
        ]

        return count_df.select(F.col(self.ID), F.struct(*struct_cols).alias("outDegrees"))

    def type_in_degree(self, edge_type_col: str, edge_types: list[Any] | None = None) -> DataFrame:
        """
        The in-degree of each vertex per edge type, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - "inDegrees": a struct with a field for each edge type, storing the in-degree count

        :param edge_type_col: Name of the column in edges DataFrame that contains edge types
        :param edge_types: Optional list of edge type values. If None, edge types will be discovered automatically.
        :return: DataFrame with columns "id" and "inDegrees" (struct type)
        """
        if edge_types is not None:
            pivot_df = self._impl._edges.groupBy(F.col(self.DST).alias(self.ID)).pivot(
                edge_type_col, edge_types
            )
        else:
            pivot_df = self._impl._edges.groupBy(F.col(self.DST).alias(self.ID)).pivot(
                edge_type_col
            )

        count_df = pivot_df.agg(F.count(F.lit(1))).na.fill(0)
        struct_cols = [
            F.col(col_name).cast("int").alias(col_name)
            for col_name in count_df.columns
            if col_name != self.ID
        ]
        return count_df.select(F.col(self.ID), F.struct(*struct_cols).alias("inDegrees"))

    def type_degree(self, edge_type_col: str, edge_types: list[Any] | None = None) -> DataFrame:
        """
        The total degree of each vertex per edge type (both in and out), returned as a DataFrame
        with two columns:

         - "id": the ID of the vertex
         - "degrees": a struct with a field for each edge type, storing the total degree count

        :param edge_type_col: Name of the column in edges DataFrame that contains edge types
        :param edge_types: Optional list of edge type values. If None, edge types will be discovered automatically.
        :return: DataFrame with columns "id" and "degrees" (struct type)
        """
        exploded_edges = self._impl._edges.select(
            F.explode(F.array(F.col(self.SRC), F.col(self.DST))).alias(self.ID),
            F.col(edge_type_col),
        )

        if edge_types is not None:
            pivot_df = exploded_edges.groupBy(self.ID).pivot(edge_type_col, edge_types)
        else:
            pivot_df = exploded_edges.groupBy(self.ID).pivot(edge_type_col)

        count_df = pivot_df.agg(F.count(F.lit(1))).na.fill(0)
        struct_cols = [
            F.col(col_name).cast("int").alias(col_name)
            for col_name in count_df.columns
            if col_name != self.ID
        ]

        return count_df.select(F.col(self.ID), F.struct(*struct_cols).alias("degrees"))

    @property
    def triplets(self) -> DataFrame:
        """
        The triplets (source vertex)-[edge]->(destination vertex) for all edges in the graph.

        Returned as a :class:`DataFrame` with three columns:
         - "src": source vertex with schema matching 'vertices'
         - "edge": edge with schema matching 'edges'
         - 'dst': destination vertex with schema matching 'vertices'

        :return:  DataFrame with columns 'src', 'edge', and 'dst'
        """
        return self._impl.triplets

    @property
    def pregel(self) -> Pregel:
        """
        Get the :class:`graphframes.classic.pregel.Pregel`
        or :class`graphframes.connect.graphframes_client.Pregel`
        object for running pregel.

        See :class:`graphframes.lib.Pregel` for more details.
        """
        return self._impl.pregel

    def find(self, pattern: str) -> DataFrame:
        """
        Motif finding: searching the graph for structural patterns.

        Motif finding uses a simple Domain-Specific Language (DSL) for expressing structural
        queries. For example, ``graph.find("(a)-[e1]->(b); (b)-[e2]->(a)")`` will search for
        pairs of vertices ``a``, ``b`` connected by edges in both directions. It returns a
        :class:`DataFrame` of all such structures, with columns for each named element (vertex
        or edge) in the motif.

        **Performance tip:** Motif finding translates patterns into a series of joins. Enabling
        Spark's Cost-Based Optimizer (CBO) and join reordering can significantly improve
        performance::

            spark.conf.set("spark.sql.cbo.enabled", "true")
            spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

        The join reorder algorithm is bounded by ``spark.sql.cbo.joinReorder.dp.threshold``
        (default: ``12``). If the estimated number of joins in your motif exceeds this threshold,
        increase it accordingly::

            spark.conf.set("spark.sql.cbo.joinReorder.dp.threshold", "20")

        CBO relies on table statistics, so run ``ANALYZE TABLE <tableName> COMPUTE STATISTICS`` on
        the vertices and edges tables (or temp views) to ensure accurate statistics are available.

        :param pattern: String describing the motif to search for.
        :return: DataFrame with one Row for each instance of the motif found.
        """
        return self._impl.find(pattern=pattern)

    def filterVertices(self, condition: str | Column) -> "GraphFrame":
        """
        Filters the vertices based on expression, remove edges containing any dropped vertices.

        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered vertices and edges.
        """
        return GraphFrame._from_impl(self._impl.filterVertices(condition=condition))

    def filterEdges(self, condition: str | Column) -> "GraphFrame":
        """
        Filters the edges based on expression, keep all vertices.

        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered edges.
        """

        return GraphFrame._from_impl(self._impl.filterEdges(condition=condition))

    def dropIsolatedVertices(self) -> "GraphFrame":
        """
        Drops isolated vertices, vertices are not contained in any edges.

        :return: GraphFrame with filtered vertices.
        """
        return GraphFrame._from_impl(self._impl.dropIsolatedVertices())

    def detectingCycles(
        self,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """Find all cycles in the graph.

        An implementation of the Rocha-Thatte cycle detection algorithm.
        Rocha, Rodrigo Caetano, and Bhalchandra D. Thatte. "Distributed cycle detection in
        large-scale sparse graphs." Proceedings of Simpósio Brasileiro de Pesquisa Operacional
        (SBPO'15) (2015): 1-11.

        Returns a DataFrame with unique cycles.

        :param checkpoint_interval: Pregel checkpoint interval, default is 2
        :param use_local_checkpoints: should local checkpoints be used instead of checkpointDir
        :storage_level: the level of storage for both intermediate results and an output DataFrame

        :return: Persisted DataFrame with all the cycles
        """
        return self._impl.detectingCycles(checkpoint_interval, use_local_checkpoints, storage_level)

    def bfs(
        self,
        fromExpr: str,
        toExpr: str,
        edgeFilter: str | None = None,
        maxPathLength: int = 10,
    ) -> DataFrame:
        """
        Breadth-first search (BFS).

        See Scala documentation for more details.

        :return: DataFrame with one Row for each shortest path between matching vertices.
        """
        return self._impl.bfs(
            fromExpr=fromExpr,
            toExpr=toExpr,
            edgeFilter=edgeFilter,
            maxPathLength=maxPathLength,
        )

    def all_paths(
        self,
        from_expr: Column | str,
        to_expr: Column | str,
        edge_filter: Column | str | None = None,
        max_path_length: int = 5,
        is_directed: bool = True,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Computes all simple paths between source and destination vertices.

        This algorithm enumerates paths up to ``max_path_length`` hops. It supports directed
        and undirected traversal as well as optional edge filtering. It returns all simple
        paths between source and destination vertices. Here the term "simple" means no
        repeated vertices. For example, if there are paths A-B-C, A-D-C and the edge B-A,
        and the user asked to find all the paths between "A" and "C", only A-B-C and A-D-C
        will be returned, but not A-B-A-D-C.

        The default value of ``max_path_length`` is 5. Keep in mind that requesting
        ``max_path_length`` on the scale of the graph diameter may cause the algorithm to
        try to return (almost) all simple paths in the graph, which can create huge
        performance degradation or even OOM-like errors.

        **Returned DataFrame schema:**

         - ``path``: array of vertex ids in traversal order
         - ``len``: number of edges in the path (Long)

        .. note::
            In the case of an undirected graph, the algorithm runs on an internal graph
            made by union of edges and reversed edges. It is assumed that the graph does
            not have multi-edges. Results may be unstable and unpredictable for graphs
            with multi-edges.

        **Example:**

        >>> paths = g.all_paths(
        ...     from_expr="name = 'A'",
        ...     to_expr="name = 'C'",
        ...     max_path_length=3,
        ... )
        >>> paths.show()

        :param from_expr: Column expression or SQL expression string identifying the
            source (starting) vertices.
        :param to_expr: Column expression or SQL expression string identifying the
            destination (target) vertices.
        :param edge_filter: Optional Column expression or SQL expression string applied
            to edges during traversal. Only edges satisfying this condition are considered.
            If not provided, all edges are considered.
        :param max_path_length: Maximum number of edges in a path; must be greater than 0.
            Default is 5. Setting a large value (e.g., on the scale of the graph diameter)
            may cause severe performance degradation or out-of-memory errors.
        :param is_directed: Whether to use directed traversal. If False, the graph is
            treated as undirected by internally unioning edges with reversed edges.
            Default is True.
        :param checkpoint_interval: Checkpoint every N iterations, 0 = disabled (default: 0)
        :param use_local_checkpoints: Use local checkpoints (faster but less reliable)
        :param storage_level: Storage level for intermediate results

        :return: DataFrame with columns ``path`` (array of vertex ids) and ``len``
            (number of edges in the path).
        """
        return self._impl.all_paths(
            from_expr=from_expr,
            to_expr=to_expr,
            edge_filter=edge_filter,
            max_path_length=max_path_length,
            is_directed=is_directed,
            checkpoint_interval=checkpoint_interval,
            use_local_checkpoints=use_local_checkpoints,
            storage_level=storage_level,
        )

    def aggregateMessages(
        self,
        aggCol: list[Column | str] | Column,
        sendToSrc: list[Column | str] | Column | str | None = None,
        sendToDst: list[Column | str] | Column | str | None = None,
        intermediate_storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Aggregates messages from the neighbours.

        When specifying the messages and aggregation function, the user may reference columns using
        the static methods in :class:`graphframes.lib.AggregateMessages`.

        See Scala documentation for more details.

        Warning! The result of this method is persisted DataFrame object! Users should handle unpersist
        to avoid possible memory leaks!

        :param aggCol: the requested aggregation output either as a collection of
            :class:`pyspark.sql.Column` or SQL expression string
        :param sendToSrc: message sent to the source vertex of each triplet either as
            a collection of :class:`pyspark.sql.Column` or SQL expression string (default: None)
        :param sendToDst: message sent to the destination vertex of each triplet either as
            collection of :class:`pyspark.sql.Column` or SQL expression string (default: None)
        :param intermediate_storage_level: the level of intermediate storage that will be used
            for both intermediate result and the output.

        :return: Persisted DataFrame with columns for the vertex ID and the resulting aggregated message.
            The name of the resulted message column is based on the alias of the provided aggCol!
        """

        if sendToDst is None:
            sendToDst = []
        if sendToSrc is None:
            sendToSrc = []

        # Back-compatibility workaround
        if not isinstance(aggCol, list):
            warnings.warn(
                "Passing single column to aggCol is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                [aggCol], sendToSrc, sendToDst, intermediate_storage_level
            )
        if not isinstance(sendToSrc, list):
            warnings.warn(
                "Passing single column to sendToSrc is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                aggCol, [sendToSrc], sendToDst, intermediate_storage_level
            )
        if not isinstance(sendToDst, list):
            warnings.warn(
                "Passing single column to sendToDst is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                aggCol, sendToSrc, [sendToDst], intermediate_storage_level
            )

        if len(aggCol) == 0:
            raise TypeError("At least one aggregation column should be provided!")

        if (len(sendToSrc) == 0) and (len(sendToDst) == 0):
            raise ValueError("Either `sendToSrc`, `sendToDst`, or both have to be provided")
        return self._impl.aggregateMessages(
            aggCol=aggCol,
            sendToSrc=sendToSrc,
            sendToDst=sendToDst,
            intermediate_storage_level=intermediate_storage_level,
        )

    # Standard algorithms

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
        useLabelsAsComponents: bool = False,
        use_local_checkpoints: bool = False,
        max_iter: int = (1 << 31) - 2,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Computes the connected components of the graph.

        See Scala documentation for more details.

        :param algorithm: connected components algorithm to use (default: "graphframes")
                          Supported algorithms are "two_phase", "randomized_contraction",
                          "graphframes" (deprecated alias for "two_phase") and "graphx".
        :param checkpointInterval: checkpoint interval in terms of number of iterations (default: 2)
        :param broadcastThreshold: broadcast threshold in propagating component assignments
                                   (default: 1000000). Passing -1 disable manual broadcasting and
                                   allows AQE to handle skewed joins. This mode is much faster
                                   and is recommended to use. Default value may be changed to -1
                                   in the future versions of GraphFrames.
        :param useLabelsAsComponents: if True, uses the vertex labels as components, otherwise will
                                      use longs
        :param use_local_checkpoints: should local checkpoints be used, default false;
                                    local checkpoints are faster and does not require to set
                                    a persistent checkpointDir; from the other side, local
                                    checkpoints are less reliable and require executors to have
                                    big enough local disks.
        :param storage_level: storage level for both intermediate and final dataframes.

        :return: DataFrame with new vertices column "component"
        """
        return self._impl.connectedComponents(
            algorithm=algorithm,
            checkpointInterval=checkpointInterval,
            broadcastThreshold=broadcastThreshold,
            useLabelsAsComponents=useLabelsAsComponents,
            use_local_checkpoints=use_local_checkpoints,
            max_iter=max_iter,
            storage_level=storage_level,
        )

    def maximal_independent_set(
        self,
        seed: int = 42,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        This method implements a distributed algorithm for finding a Maximal Independent Set (MIS)
        in a graph.

        An MIS is a set of vertices such that no two vertices in the set are adjacent (i.e., there
        is no edge between any two vertices in the set), and the set is maximal, meaning that adding
        any other vertex to the set would violate the independence property. Note that this
        implementation finds a maximal (but not necessarily maximum) independent set; that is, it
        ensures no more vertices can be added to the set, but does not guarantee that the set has
        the largest possible number of vertices among all possible independent sets in the graph.

        The algorithm implemented here is based on the paper: Ghaffari, Mohsen. "An improved
        distributed algorithm for maximal independent set." Proceedings of the twenty-seventh annual
        ACM-SIAM symposium on Discrete algorithms. Society for Industrial and Applied Mathematics,
        2016.

        Note: This is a randomized, non-deterministic algorithm. The result may vary between runs
        even if a fixed random seed is provided because of how Apache Spark works.

        :param seed: random seed used for tie-breaking in the algorithm (default: 42)
        :param checkpoint_interval: checkpoint interval in terms of number of iterations (default: 2)
        :param use_local_checkpoints: whether to use local checkpoints (default: False);
                                      local checkpoints are faster and do not require setting
                                      a persistent checkpoint directory; however, they are less
                                      reliable and require executors to have sufficient local disk space.
        :param storage_level: storage level for both intermediate and final DataFrames
                              (default: MEMORY_AND_DISK_DESER)

        :return: DataFrame containing the ``id`` of each vertex in the Maximal Independent Set
        """
        return self._impl.maximal_independent_set(
            checkpoint_interval, storage_level, use_local_checkpoints, seed
        )

    def k_core(
        self,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        The k-core is the maximal subgraph such that every vertex has at least degree k.
        The k-core metric is a measure of the centrality of a node in a network, based on its
        degree and the degrees of its neighbors. Nodes with higher k-core values are considered
        to be more central and influential within the network.

        This implementation is based on the algorithm described in:
        Mandal, Aritra, and Mohammad Al Hasan. "A distributed k-core decomposition algorithm
        on spark." 2017 IEEE International Conference on Big Data (Big Data). IEEE, 2017.

        :param checkpoint_interval: Pregel checkpoint interval, default is 2
        :param use_local_checkpoints: should local checkpoints be used instead of checkpointDir
        :param storage_level: the level of storage for both intermediate results and an output DataFrame

        :return: Persisted DataFrame with ID and k-core values (column "kcore")
        """
        return self._impl.k_core(checkpoint_interval, use_local_checkpoints, storage_level)

    def hyper_anf(
        self,
        n_hops: int = 3,
        lg_nom_entries: int = 12,
        edge_filter: Column | str | None = None,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """HyperANF-style approximation of the neighbourhood function.

        This implementation is inspired by
        `HyperANF: Approximating the Neighbourhood Function of Very Large Graphs on a Budget
        <https://arxiv.org/pdf/1011.5599>`_
        (Vigna, Boldi, Rosa; 2010).

        The input graph is treated as directed: for each vertex, reachability is computed by
        following outgoing edges from ``src`` to ``dst``.

        Compared with the cumulative neighbourhood-function presentation in the paper, this
        implementation returns one column per hop: ``hop_0``, ``hop_1``, ``hop_2``, …, ``hop_N``.
        The ``hop_0`` column contains a HyperLogLog sketch of the source vertex itself, and each
        ``hop_k`` column for ``k >= 1`` contains a HyperLogLog sketch of the set of vertices
        reachable in exactly ``k`` hops.  To derive the cumulative approximate neighbourhood
        function for distances up to some hop ``k``, combine ``hop_0`` through ``hop_k`` with
        ``hll_union`` and then apply ``hll_sketch_estimate`` to the merged sketch.

        The computation can also be restricted to a subgraph by supplying an edge filter
        expression via ``edge_filter``.  A common use case is to filter on ``src``, for example
        ``"src IN (1, 2, 3)"``, to obtain sketches only for a selected set of starting vertices.

        **Example:**

        >>> result = g.hyper_anf(n_hops=2)
        >>> result.columns
        ['id', 'hop_0', 'hop_1', 'hop_2']

        :param n_hops: Maximum hop distance to compute (positive integer). The result will
            contain columns ``hop_0`` through ``hop_N`` where ``N = n_hops``.
        :param lg_nom_entries: Log2 of nominal entries used by HLL sketch aggregations.
            Must be between 4 and 21 (inclusive). Higher values increase accuracy at the
            cost of memory. Default is 12.
        :param edge_filter: Optional column expression or SQL expression string applied to
            edges before computation. Only edges satisfying this predicate participate in
            the directed reachability expansion.
        :param checkpoint_interval: Checkpoint interval in terms of number of iterations
            (default: 2). Use 0 to disable checkpointing.
        :param use_local_checkpoints: Whether to use local checkpoints instead of a
            persistent checkpoint directory. Local checkpoints are faster but less reliable.
        :param storage_level: Storage level for intermediate and final DataFrames.

        :return: Persisted DataFrame with vertex ``id`` and one sketch column per hop
            (``hop_0``, ``hop_1``, …, ``hop_N``).
        """
        if n_hops <= 0:
            raise ValueError("n_hops must be a positive integer")
        if not (4 <= lg_nom_entries <= 21):
            raise ValueError("lg_nom_entries must be between 4 and 21 (inclusive)")

        return self._impl.hyper_anf(
            n_hops=n_hops,
            lg_nom_entries=lg_nom_entries,
            edge_filter=edge_filter,
            checkpoint_interval=checkpoint_interval,
            use_local_checkpoints=use_local_checkpoints,
            storage_level=storage_level,
        )

    def labelPropagation(
        self,
        maxIter: int,
        algorithm: str = "graphx",
        use_local_checkpoints: bool = False,
        checkpoint_interval: int = 2,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Runs static label propagation for detecting communities in networks.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to be performed
        :param algorithm: implementation to use, possible values are "graphframes" and "graphx";
                          "graphx" is faster for small-medium sized graphs,
                          "graphframes" requires less amount of memory
        :param use_local_checkpoints: should local checkpoints be used, default false;
                                      local checkpoints are faster and does not require to set
                                      a persistent checkpointDir; from the other side, local
                                      checkpoints are less reliable and require executors to have
                                      big enough local disks.
        :checkpoint_interval: How often should the intermediate result be checkpointed;
                              Using big value here may tend to huge logical plan growth due
                              to the iterative nature of the algorithm.
        :param storage_level: storage level for both intermediate and final dataframes.

        :return: Persisted DataFrame with new vertices column "label"
        """
        return self._impl.labelPropagation(
            maxIter=maxIter,
            algorithm=algorithm,
            use_local_checkpoints=use_local_checkpoints,
            checkpoint_interval=checkpoint_interval,
            storage_level=storage_level,
        )

    def neighborhood_aware_cdlp(
        self,
        max_iter: int,
        structural_similarity_multiplier: float = 0.5,
        ignore_direct_links: bool = False,
        initial_label_col: str | None = None,
        is_directed: bool = True,
        lg_nom_entries: int = 12,
        use_local_checkpoints: bool = False,
        checkpoint_interval: int = 2,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Neighborhood-aware community detection via weighted label propagation.

        This algorithm is a Label Propagation variant where each incoming label vote is weighted
        by a combination of:
        - optional direct-link baseline strength (enabled unless
        ``ignore_direct_links = True``), and
        - neighborhood-overlap strength
        (``structural_similarity_multiplier * common_neighbors``).

        Intuitively, labels from neighbors that are structurally similar to the destination
        (many common neighbors) can be amplified instead of treating all edges equally.

        At each iteration, every vertex aggregates weighted incoming votes by label and picks
        the label with maximum total weight.

        Edge-weight regimes:
        - ``ignore_direct_links = False``:
        ``edge_weight = 1 + structural_similarity_multiplier * common_neighbors``
        - ``ignore_direct_links = True``:
        ``edge_weight = structural_similarity_multiplier * common_neighbors``

        :param max_iter: maximum number of propagation rounds.
        :param structural_similarity_multiplier: scales neighborhood-overlap contribution.
               Must be non-negative.
        :param ignore_direct_links: whether to drop direct-link baseline vote mass.
        :param initial_label_col: optional vertex column used to initialize labels.
        :param is_directed: whether to treat edges as directed.
        :param lg_nom_entries: log2 nominal entries used by Theta sketch aggregations.
        :param use_local_checkpoints: whether to use local checkpoints.
        :param checkpoint_interval: checkpoint interval in iterations.
        :param storage_level: storage level for intermediate datasets.

        :return: Persisted DataFrame with new vertex column ``label``.
        """
        if structural_similarity_multiplier < 0.0:
            raise ValueError("structural_similarity_multiplier must be >= 0")

        if ignore_direct_links and structural_similarity_multiplier == 0.0:
            raise ValueError(
                "structural_similarity_multiplier must be > 0 when ignore_direct_links is True"
            )

        return self._impl.neighborhood_aware_cdlp(
            max_iter=max_iter,
            structural_similarity_multiplier=structural_similarity_multiplier,
            ignore_direct_links=ignore_direct_links,
            use_local_checkpoints=use_local_checkpoints,
            checkpoint_interval=checkpoint_interval,
            storage_level=storage_level,
            is_directed=is_directed,
            lg_nom_entries=lg_nom_entries,
            initial_label_col=initial_label_col,
        )

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: Any | None = None,
        maxIter: int | None = None,
        tol: float | None = None,
    ) -> "GraphFrame":
        """
        Runs the PageRank algorithm on the graph.
        Note: Exactly one of fixed_num_iter or tolerance must be set.

        See Scala documentation for more details.

        :param resetProbability: Probability of resetting to a random vertex.
        :param sourceId: (optional) the source vertex for a personalized PageRank.
        :param maxIter: If set, the algorithm is run for a fixed number
               of iterations. This may not be set if the `tol` parameter is set.
        :param tol: If set, the algorithm is run until the given tolerance.
               This may not be set if the `numIter` parameter is set.
        :return:  GraphFrame with new vertices column "pagerank" and new edges column "weight"
        """
        return GraphFrame._from_impl(
            self._impl.pageRank(
                resetProbability=resetProbability,
                sourceId=sourceId,
                maxIter=maxIter,
                tol=tol,
            )
        )

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: list[Any] | None = None,
        maxIter: int | None = None,
    ) -> "GraphFrame":
        """
        Run the personalized PageRank algorithm on the graph,
        from the provided list of sources in parallel for a fixed number of iterations.

        See Scala documentation for more details.

        :param resetProbability: Probability of resetting to a random vertex
        :param sourceIds: the source vertices for a personalized PageRank
        :param maxIter: the fixed number of iterations this algorithm runs
        :return:  GraphFrame with new vertices column "pageranks" and new edges column "weight"
        """
        return GraphFrame._from_impl(
            self._impl.parallelPersonalizedPageRank(
                resetProbability=resetProbability, sourceIds=sourceIds, maxIter=maxIter
            )
        )

    def shortestPaths(
        self,
        landmarks: list[str | int],
        algorithm: str = "graphx",
        use_local_checkpoints: bool = False,
        checkpoint_interval: int = 2,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
        is_directed: bool = True,
    ) -> DataFrame:
        """
        Runs the shortest path algorithm from a set of landmark vertices in the graph.

        See Scala documentation for more details.

        :param landmarks: a set of one or more landmarks
        :param algorithm: implementation to use, possible values are "graphframes" and "graphx";
                          "graphx" is faster for small-medium sized graphs,
                          "graphframes" requires less amount of memory
        :param use_local_checkpoints: should local checkpoints be used, default false;
                                      local checkpoints are faster and does not require to set
                                      a persistent checkpointDir; from the other side, local
                                      checkpoints are less reliable and require executors to have
                                      big enough local disks.
        :param checkpoint_interval: How often should the intermediate result be checkpointed;
                              Using big value here may tend to huge logical plan growth due
                              to the iterative nature of the algorithm.
        :param storage_level: storage level for both intermediate and final dataframes.
        :param is_directed: should algorithm find directed paths or any paths.

        :return: persistent DataFrame with new vertices column "distances"
        """
        return self._impl.shortestPaths(
            landmarks=landmarks,
            algorithm=algorithm,
            use_local_checkpoints=use_local_checkpoints,
            checkpoint_interval=checkpoint_interval,
            storage_level=storage_level,
            is_directed=is_directed,
        )

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        """
        Runs the strongly connected components algorithm on this graph.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to run
        :return: DataFrame with new vertex column "component"
        """
        return self._impl.stronglyConnectedComponents(maxIter=maxIter)

    def svdPlusPlus(
        self,
        rank: int = 10,
        maxIter: int = 2,
        minValue: float = 0.0,
        maxValue: float = 5.0,
        gamma1: float = 0.007,
        gamma2: float = 0.007,
        gamma6: float = 0.005,
        gamma7: float = 0.015,
    ) -> tuple[DataFrame, float]:
        """Runs the SVD++ algorithm for Collaborative Filtering.

        Based on the paper "Factorization Meets the Neighborhood: a Multifaceted Collaborative
        Filtering Model" by Yehuda Koren (2008).

        **Algorithm Description**
        SVD++ improves upon standard Matrix Factorization by incorporating implicit feedback
        (the history of items a user has interacted with) alongside explicit ratings.
        The prediction rule is:
        ``r_ui = µ + b_u + b_i + q_i^T * (p_u + |N(u)|^-0.5 * sum(y_j for j in N(u)))``

        **Input Requirements**
        The input graph must be a **Directed Bipartite Graph**:
        - **Vertices**: A mix of Users and Items.
        - **Edges**: Directed strictly from **User (src) -> Item (dst)**.
        - **Edge Attribute**: Represents the rating (weight).

        :param rank: The number of latent factors (embedding size).
        :param maxIter: The maximum number of iterations.
        :param minValue: The minimum possible rating value (used for clipping predictions).
        :param maxValue: The maximum possible rating value (used for clipping predictions).
        :param gamma1: Learning rate for bias parameters (`b_u`, `b_i`).
        :param gamma2: Learning rate for factor parameters (`p_u`, `q_i`, `y_j`).
        :param gamma6: Regularization coefficient for bias parameters.
        :param gamma7: Regularization coefficient for factor parameters.
        :return: A tuple ``(v, loss)`` where:
            - ``v`` is a DataFrame of vertices containing the trained model parameters (embeddings).
            - ``loss`` is the final training loss (double).

        **Output DataFrame Columns**
        The returned DataFrame ``v`` contains the following new columns containing the model parameters:

        - **column1** (Array[Double]): Primary Latent Factors (Explicit Embedding).
            - For Users: Preferences vector (`p_u`).
            - For Items: Characteristics vector (`q_i`).
        - **column2** (Array[Double]): Implicit Factors (Implicit Embedding).
            - For Items: Influence vector (`y_i`).
            - For Users: Unused/Zero (users aggregate `y` from neighbors).
        - **column3** (Double): Bias term.
            - For Users: User bias (`b_u`).
            - For Items: Item bias (`b_i`).
        - **column4** (Double): Implicit Normalization term.
            - For Users: Precomputed ``|N(u)|^-0.5``.
            - For Items: Unused.
        """
        return self._impl.svdPlusPlus(
            rank=rank,
            maxIter=maxIter,
            minValue=minValue,
            maxValue=maxValue,
            gamma1=gamma1,
            gamma2=gamma2,
            gamma6=gamma6,
            gamma7=gamma7,
        )

    def triangleCount(
        self, storage_level: StorageLevel, algorithm: str = "exact", lg_nom_entries: int = 12
    ) -> DataFrame:
        """
        Computes the number of triangles passing through each vertex.
        This algorithm identifies sets of three vertices where each pair is connected by an edge.

        The implementation provides two algorithms:

        - "exact": Computes the exact triangle count using set intersection of neighbor lists.

        Note: This method can fail or encounter OOM errors on power-law graphs or graphs with
        very high-degree nodes, as it requires collecting and intersecting the full neighbor
        lists for the source and destination vertices of every edge.

        - "approx": Uses DataSketches (Theta sketches) to estimate the triangle count. This trades off perfect accuracy for significantly improved performance and lower memory overhead, making it suitable for large-scale or dense graphs.

        :param storage_level: Storage level for caching intermediate DataFrames.
        :param algorithm: The triangle counting algorithm to use, "exact" or "approx" (default: "exact").
        :param lg_nom_entries: The log2 of the nominal entries for the Theta sketch (only used
                               if algorithm="approx"). Higher values increase accuracy at the
                               cost of memory. (default: 12).
        :return: A DataFrame containing the vertex "id" and the triangle "count".
        """
        spark_version = self._impl._spark.version
        if (spark_version[:3] < "4.1") and (algorithm == "approx"):
            err_msg = "approximate algorithm requires Spark 4.1+"
            err_msg += f" version {spark_version[:3]} is not supported"
            raise ValueError(err_msg)
        return self._impl.triangleCount(
            storage_level=storage_level, algorithm=algorithm, log_nom_entries=lg_nom_entries
        )

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: str | None = None
    ) -> DataFrame:
        """
        Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by Lin and Cohen. From the abstract: PIC finds a very low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise similarity matrix of the data.

        :param k: the numbers of clusters to create
        :param maxIter: param for maximum number of iterations (>= 0)
        :param weightCol: optional name of weight column, 1.0 is used if not provided

        :return: DataFrame with new column "cluster"
        """
        return self._impl.powerIterationClustering(k, maxIter, weightCol)

    def validate(
        self,
        check_vertices: bool = True,
        intermediate_storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> None:
        """
        Validates the consistency and integrity of a graph by performing checks on the vertices and
        edges.

        :param check_vertices: a flag to indicate whether additional vertex consistency checks
            should be performed. If true, the method will verify that all vertices in the vertex
            DataFrame are represented in the edge DataFrame and vice versa. It is slow on big graphs.
        :param intermediate_storage_level: the storage level to be used when persisting
            intermediate DataFrame computations during the validation process.
        :return: Unit, as the method performs validation checks and throws an exception if
            validation fails.
        :raises ValueError: if there are any inconsistencies in the graph, such as duplicate
            vertices, mismatched vertices between edges and vertex DataFrames or missing
            connections.
        """
        persisted_vertices = self.vertices.persist(intermediate_storage_level)
        row = persisted_vertices.select(F.count_distinct(F.col(ID))).first()
        assert row is not None  # for type checker
        count_distinct_vertices = row[0]
        assert isinstance(count_distinct_vertices, int)  # for type checker
        total_count_vertices = persisted_vertices.count()
        if count_distinct_vertices != total_count_vertices:
            _msg = "Graph contains ({}) duplicate vertices."

            raise ValueError(_msg.format(total_count_vertices - count_distinct_vertices))
        if check_vertices:
            vertices_set_from_edges = (
                self.edges.select(F.col(SRC).alias(ID))
                .union(self.edges.select(F.col(DST).alias(ID)))
                .distinct()
                .persist(intermediate_storage_level)
            )
            count_vertices_from_edges = vertices_set_from_edges.count()
            if count_vertices_from_edges > count_distinct_vertices:
                _msg = "Graph is inconsistent: edges has {} "
                _msg += "vertices, but vertices has {} vertices."
                raise ValueError(_msg.format(count_vertices_from_edges, count_distinct_vertices))

            combined = vertices_set_from_edges.join(self.vertices, ID, "left_anti")
            count_of_bad_vertices = combined.count()
            if count_of_bad_vertices > 0:
                _msg = "Vertices DataFrame does not contain all edges src/dst. "
                _msg += "Found {} edges src/dst that are not in the vertices DataFrame."
                raise ValueError(_msg.format(count_of_bad_vertices))
            _ = vertices_set_from_edges.unpersist()
        _ = persisted_vertices.unpersist()

    def as_undirected(self) -> "GraphFrame":
        """
        Converts the directed graph into an undirected graph by ensuring that all directed edges are
        bidirectional. For every directed edge (src, dst), a corresponding edge (dst, src) is added.

        :return: A new GraphFrame representing the undirected graph.
        """

        edge_attr_columns = [c for c in self.edges.columns if c not in [SRC, DST]]

        # Create the undirected edges by duplicating each edge in both directions

        # 3.5.x problem: selecting empty struct fails on spark connect
        # TODO: remove after removing 3.5.x

        if edge_attr_columns:
            forward_edges = self.edges.select(
                F.col(SRC), F.col(DST), F.struct(*edge_attr_columns).alias(EDGE)
            )
            backward_edges = self.edges.select(
                F.col(DST).alias(SRC),
                F.col(SRC).alias(DST),
                F.struct(*edge_attr_columns).alias(EDGE),
            )
            new_edges = forward_edges.union(backward_edges).select(SRC, DST, EDGE)
        else:
            forward_edges = self.edges.select(F.col(SRC), F.col(DST))
            backward_edges = self.edges.select(F.col(DST).alias(SRC), F.col(SRC).alias(DST))
            new_edges = forward_edges.union(backward_edges).select(SRC, DST)

        # Preserve additional edge attributes
        edge_columns = [F.col(EDGE).getField(c).alias(c) for c in edge_attr_columns]

        # Select all columns including the new edge attributes
        selected_columns = [F.col(SRC), F.col(DST)] + edge_columns
        new_edges = new_edges.select(*selected_columns)

        return GraphFrame(self.vertices, new_edges)

    def as_reversed(self) -> "GraphFrame":
        """
        Reverses the direction of all edges in the graph. For every directed edge (src, dst),
        the resulting graph will contain an edge (dst, src) with the same attributes.

        :return: A new GraphFrame with all edge directions reversed.
        """
        edge_attr_columns = [c for c in self.edges.columns if c not in [SRC, DST]]

        if edge_attr_columns:
            reversed_edges = self.edges.select(
                F.col(DST).alias(SRC),
                F.col(SRC).alias(DST),
                F.struct(*edge_attr_columns).alias(EDGE),
            )
            reversed_edges = reversed_edges.select(SRC, DST, EDGE)
        else:
            reversed_edges = self.edges.select(F.col(DST).alias(SRC), F.col(SRC).alias(DST))

        edge_columns = [F.col(EDGE).getField(c).alias(c) for c in edge_attr_columns]
        selected_columns = [F.col(SRC), F.col(DST)] + edge_columns
        reversed_edges = reversed_edges.select(*selected_columns)

        return GraphFrame(self.vertices, reversed_edges)

    def aggregate_neighbors(
        self,
        starting_vertices: Column | str,
        accumulator_names: list[str],
        accumulator_inits: list[Column | str],
        accumulator_updates: list[Column | str],
        max_hops: int = 3,
        stopping_condition: Column | str | None = None,
        target_condition: Column | str | None = None,
        required_vertex_attributes: list[str] | None = None,
        required_edge_attributes: list[str] | None = None,
        edge_filter: Column | str | None = None,
        remove_loops: bool = False,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """Multi-hop neighbor aggregation with customizable accumulators.

        AggregateNeighbors allows you to explore the graph up to a specified number of hops,
        accumulating values along paths using customizable accumulator expressions. It supports
        both stopping conditions (when to stop exploring) and target conditions (when to
        collect a result).

        **Basic Example:**

        >>> from pyspark.sql import functions as F
        >>> g = GraphFrame(vertices, edges)
        >>> result = g.aggregate_neighbors(
        ...     starting_vertices=F.col("id") == 1,
        ...     max_hops=3,
        ...     accumulator_names=["path_length"],
        ...     accumulator_inits=[F.lit(0)],
        ...     accumulator_updates=[F.col("path_length") + 1],
        ...     target_condition=AggregateNeighbors.dst_attr("id") == 4
        ... )

        **Using Accumulators:**

        Accumulators track values as the algorithm traverses the graph. Each accumulator has:
        - A name (becomes a column in the result)
        - An initial value expression (evaluated on starting vertices)
        - An update expression (evaluated when traversing each edge)

        In update expressions, you can reference:
        - Source vertex attributes via ``src_attr("attrName")``
        - Destination vertex attributes via ``dst_attr("attrName")``
        - Edge attributes via ``edge_attr("attrName")``

        **Example with Multiple Accumulators:**

        >>> result = g.aggregate_neighbors(
        ...     starting_vertices=F.col("id") == 1,
        ...     max_hops=5,
        ...     accumulator_names=["sum_values", "product_weights"],
        ...     accumulator_inits=[F.lit(0), F.lit(1.0)],
        ...     accumulator_updates=[
        ...         F.col("sum_values") + AggregateNeighbors.dst_attr("value"),
        ...         F.col("product_weights") * AggregateNeighbors.edge_attr("weight")
        ...     ],
        ...     target_condition=F.col("dst.id") == 10
        ... )

        **Stopping vs Target Conditions:**

        - ``stopping_condition``: Stops traversal along a path when true. Useful for avoiding
          cycles or limiting search depth based on custom criteria.
        - ``target_condition``: Marks vertices as results when true. Only accumulators reaching
          target vertices are returned.
        - If both are provided, only accumulators that reach ``target_condition`` are saved.
        - At least one must be provided.

        **Performance Considerations:**

        - Use ``required_vertex_attributes`` and ``required_edge_attributes`` to limit the
          columns carried through traversal, reducing memory usage.
        - Use ``edge_filter`` to limit traversable edges.
        - Set ``remove_loops=True`` to exclude self-loop edges.
        - Use ``checkpoint_interval`` to prevent logical plan growth on deep traversals.
        - Be cautious with high ``max_hops`` on dense graphs (risk of OOM).

        **Warning:** The result is a persisted DataFrame. Call ``.unpersist()`` when done
        to release memory.

        :param starting_vertices: Column expression selecting seed vertices (e.g., ``F.col("id") == 1``)
        :param max_hops: Maximum number of hops to explore (positive integer)
        :param accumulator_names: List of names for accumulators (become result columns)
        :param accumulator_inits: List of initial value expressions for accumulators
        :param accumulator_updates: List of update expressions for accumulators
        :param stopping_condition: Optional condition to stop traversal along a path
        :param target_condition: Optional condition to mark vertices as results
        :param required_vertex_attributes: Vertex columns to carry (None = all columns)
        :param required_edge_attributes: Edge columns to carry (None = all columns)
        :param edge_filter: Optional condition to filter traversable edges
        :param remove_loops: If True, exclude self-loop edges (default: False)
        :param checkpoint_interval: Checkpoint every N iterations, 0 = disabled (default: 0)
        :param use_local_checkpoints: Use local checkpoints (faster but less reliable)
        :param storage_level: Storage level for intermediate results
        :return: DataFrame with columns: ``id`` (target vertex), ``hop`` (path length), and
                 one column per accumulator with its final value
        """
        return self._impl.aggregate_neighbors(
            starting_vertices=starting_vertices,
            max_hops=max_hops,
            accumulator_names=accumulator_names,
            accumulator_inits=accumulator_inits,
            accumulator_updates=accumulator_updates,
            stopping_condition=stopping_condition,
            target_condition=target_condition,
            required_vertex_attributes=required_vertex_attributes,
            required_edge_attributes=required_edge_attributes,
            edge_filter=edge_filter,
            remove_loops=remove_loops,
            checkpoint_interval=checkpoint_interval,
            use_local_checkpoints=use_local_checkpoints,
            storage_level=storage_level,
        )


@final
class RandomWalkEmbeddings:
    def __init__(self, graph: GraphFrame) -> None:
        self._graph: GraphFrame = graph
        self._params: _RandomWalksEmbeddingsParameters = _RandomWalksEmbeddingsParameters()

    def use_cached_random_walks(self, cached_walks_path: str) -> None:
        if cached_walks_path == "":
            raise ValueError("cached walks path cannot be empty")
        self._params.rw_cached_walks = cached_walks_path

    def set_rw_model(
        self,
        temporary_prefix: str,
        use_edge_direction: bool = False,
        max_neighbors_per_vertex: int = 50,
        num_walks_per_node: int = 5,
        num_batches: int = 5,
        walks_per_batch: int = 10,
        restart_probability: float = 0.1,
        seed: int = 42,
    ) -> None:
        self._params.rw_model = "rw_with_restart"
        self._params.rw_temporary_prefix = temporary_prefix
        self._params.use_edge_direction = use_edge_direction
        self._params.rw_max_nbrs = max_neighbors_per_vertex
        self._params.rw_num_walks_per_node = num_walks_per_node
        self._params.rw_num_batches = num_batches
        self._params.rw_batch_size = walks_per_batch
        self._params.rw_restart_probability = restart_probability
        self._params.rw_seed = seed

    def set_hash2vec(
        self,
        context_size: int = 5,
        num_partitions: int = 5,
        embeddings_dim: int = 512,
        decay_function: str = "gaussian",
        gaussian_sigma: float = 1.0,
        hashing_seed: int = 42,
        sign_seed: int = 18,
        l2_norm: bool = True,
        save_norm: bool = True,
    ) -> None:
        if decay_function not in _HASH2VEC_DECAY_FUNCTIONS:
            raise ValueError(f"supported decay functions are {str(_HASH2VEC_DECAY_FUNCTIONS)}")

        self._params.sequence_model = "hash2vec"
        self._params.hash2vec_context_size = context_size
        self._params.hash2vec_num_partitions = num_partitions
        self._params.hash2vec_embeddings_dim = embeddings_dim
        self._params.hash2vec_decay_function = decay_function
        self._params.hash2vec_gaussian_sigma = gaussian_sigma
        self._params.hash2vec_hashing_seed = hashing_seed
        self._params.hash2vec_sign_seed = sign_seed
        self._params.hash2vec_do_l2_norm = l2_norm
        self._params.hash2vec_safe_l2 = save_norm

    def set_word2vec(
        self,
        max_iter: int = 1,
        embeddings_dim: int = 100,
        window_size: int = 5,
        num_partitions: int = 1,
        min_count: int = 5,
        max_sentence_length: int = 1000,
        seed: int = 42,
        step_size: float = 0.025,
    ) -> None:
        self._params.sequence_model = "word2vec"
        self._params.word2vec_max_iter = max_iter
        self._params.word2vec_embeddings_dim = embeddings_dim
        self._params.word2vec_window_size = window_size
        self._params.word2vec_num_partitions = num_partitions
        self._params.word2vec_min_count = min_count
        self._params.word2vec_max_sentence_length = max_sentence_length
        self._params.word2vec_seed = seed
        self._params.word2vec_step_size = step_size

    def unset_neighbors_aggregation(self) -> None:
        self._params.aggregate_neighbors = False

    def set_neighbors_aggregation(self, max_neighbors: int = 50, seed: int = 42) -> None:
        self._params.aggregate_neighbors = True
        self._params.aggregate_neighbors_max_nbrs = max_neighbors
        self._params.aggregate_neighbors_seed = seed

    def set_clean_up_after_run(self, clean_up: bool = True) -> None:
        self._params.clean_up_after_run = clean_up

    def run(self) -> DataFrame:
        if self._params.rw_temporary_prefix == "":
            if self._params.rw_cached_walks == "":
                raise ValueError("TMP path or cached walks path should be provided!")
        return self._graph._impl.rw_embeddings(self._params)
