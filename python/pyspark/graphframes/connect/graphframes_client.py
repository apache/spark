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

from typing import cast, final

from typing_extensions import Self, override

from pyspark.graphframes.internal.utils import _RandomWalksEmbeddingsParameters
from pyspark.sql.connect import functions as F
from pyspark.sql.connect import proto
from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.connect.proto import graphframes_pb2 as pb
from pyspark.sql.connect.session import SparkSession
from pyspark.storagelevel import StorageLevel

from .utils import (
    dataframe_to_proto,
    make_column_or_expr,
    make_str_or_long_id,
    storage_level_to_proto,
)


def _dataframe_from_plan(plan: LogicalPlan, session: SparkSession) -> DataFrame:
    return DataFrame(plan, session)


@final
class PregelConnect:
    def __init__(self, graph: "GraphFrameConnect") -> None:
        self.graph = graph
        self._max_iter = 10
        self._checkpoint_interval = 2
        self._col_name: str | None = None
        self._initial_expr: Column | str | None = None
        self._update_after_agg_msgs_expr: Column | str | None = None
        self._send_msg_to_src: list[Column | str] = []
        self._send_msg_to_dst: list[Column | str] = []
        self._agg_msg: Column | None = None
        self._early_stopping = False
        self._use_local_checkpoints = False
        self._storage_level = StorageLevel.MEMORY_AND_DISK_DESER
        self._initial_active_expr: Column | str | None = None
        self._update_active_expr: Column | str | None = None
        self._stop_if_all_non_active = False
        self._skip_messages_from_non_active = False
        self._required_src_columns: list[str] = []
        self._required_dst_columns: list[str] = []
        self._required_edge_columns: list[str] = []

    def setMaxIter(self, value: int) -> Self:
        self._max_iter = value
        return self

    def setCheckpointInterval(self, value: int) -> Self:
        self._checkpoint_interval = value
        return self

    def setEarlyStopping(self, value: bool) -> Self:
        self._early_stopping = value
        return self

    def withVertexColumn(
        self,
        colName: str,
        initialExpr: Column | str,
        updateAfterAggMsgsExpr: Column | str,
    ) -> Self:
        self._col_name = colName
        self._initial_expr = initialExpr
        self._update_after_agg_msgs_expr = updateAfterAggMsgsExpr
        return self

    def sendMsgToSrc(self, msgExpr: Column | str) -> Self:
        self._send_msg_to_src.append(msgExpr)
        return self

    def sendMsgToDst(self, msgExpr: Column | str) -> Self:
        self._send_msg_to_dst.append(msgExpr)
        return self

    def aggMsgs(self, aggExpr: Column) -> Self:
        self._agg_msg = aggExpr
        return self

    def setStopIfAllNonActiveVertices(self, value: bool) -> Self:
        self._stop_if_all_non_active = value
        return self

    def setInitialActiveVertexExpression(self, value: Column | str) -> Self:
        self._initial_active_expr = value
        return self

    def setUpdateActiveVertexExpression(self, value: Column | str) -> Self:
        self._update_active_expr = value
        return self

    def setSkipMessagesFromNonActiveVertices(self, value: bool) -> Self:
        self._skip_messages_from_non_active = value
        return self

    def setUseLocalCheckpoints(self, value: bool) -> Self:
        self._use_local_checkpoints = value
        return self

    def setIntermediateStorageLevel(self, storage_level: StorageLevel) -> Self:
        self._storage_level = storage_level
        return self

    def required_src_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which source vertex columns are required when constructing triplets.

        By default, all source vertex columns are included in triplets, which can create large
        intermediate datasets for algorithms with significant state. Use this method to reduce
        memory usage by specifying only the columns that are actually needed.

        :param col_name: the first required source vertex column name
        :param col_names: additional required source vertex column names
        """
        self._required_src_columns = [col_name] + list(col_names)
        return self

    def required_dst_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which destination vertex columns are required when constructing triplets.

        By default, all destination vertex columns are included in triplets, which can create large
        intermediate datasets for algorithms with significant state. Use this method to reduce
        memory usage by specifying only the columns that are actually needed.

        :param col_name: the first required destination vertex column name
        :param col_names: additional required destination vertex column names
        """
        self._required_dst_columns = [col_name] + list(col_names)
        return self

    def required_edge_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which edge columns are required when constructing triplets.

        By default, only src and dst columns are included. Use this method to specify
        additional edge columns that are needed by the sendMsgToSrc and sendMsgToDst
        expressions.

        :param col_name: the first required edge column name
        :param col_names: additional required edge column names
        """
        self._required_edge_columns = [col_name] + list(col_names)
        return self

    def run(self) -> DataFrame:
        @final
        class Pregel(LogicalPlan):
            def __init__(
                self,
                max_iter: int,
                checkpoint_interval: int,
                early_stopping: bool,
                vertex_col_name: str,
                agg_msg: Column | str,
                send2dst: list[Column | str],
                send2src: list[Column | str],
                vertex_col_init: Column | str,
                vertex_col_upd: Column | str,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
                initial_active_col: Column | str | None,
                update_active_col: Column | str | None,
                stop_if_all_non_active: bool,
                skip_message_from_non_active: bool,
                required_src_columns: list[str],
                required_dst_columns: list[str],
                required_edge_columns: list[str],
                vertices: DataFrame,
                edges: DataFrame,
            ) -> None:
                super().__init__(None)
                self.max_iter = max_iter
                self.checkpoint_interval = checkpoint_interval
                self.early_stopping = early_stopping
                self.vertex_col_name = vertex_col_name
                self.agg_msg = agg_msg
                self.send2dst = send2dst
                self.send2src = send2src
                self.vertex_col_init = vertex_col_init
                self.vertex_col_upd = vertex_col_upd
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level
                self.initial_active_expr = initial_active_col
                self.update_active_expr = update_active_col
                self.stop_if_all_non_active = stop_if_all_non_active
                self.skip_message_from_non_active = skip_message_from_non_active
                self.required_src_columns = required_src_columns
                self.required_dst_columns = required_dst_columns
                self.required_edge_columns = required_edge_columns
                self.vertices = vertices
                self.edges = edges

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                pregel = pb.Pregel(
                    agg_msgs=make_column_or_expr(self.agg_msg, session),
                    send_msg_to_dst=[
                        make_column_or_expr(c_or_e, session) for c_or_e in self.send2dst
                    ],
                    send_msg_to_src=[
                        make_column_or_expr(c_or_e, session) for c_or_e in self.send2src
                    ],
                    checkpoint_interval=self.checkpoint_interval,
                    max_iter=self.max_iter,
                    additional_col_name=self.vertex_col_name,
                    additional_col_initial=make_column_or_expr(self.vertex_col_init, session),
                    additional_col_upd=make_column_or_expr(self.vertex_col_upd, session),
                    early_stopping=self.early_stopping,
                    use_local_checkpoints=self.use_local_checkpoints,
                    storage_level=storage_level_to_proto(self.storage_level),
                    stop_if_all_non_active=self.stop_if_all_non_active,
                    skip_messages_from_non_active=self.skip_message_from_non_active,
                    initial_active_expr=make_column_or_expr(self.initial_active_expr, session)
                    if self.initial_active_expr is not None
                    else None,
                    update_active_expr=make_column_or_expr(self.update_active_expr, session)
                    if self.update_active_expr is not None
                    else None,
                    required_src_columns=",".join(self.required_src_columns)
                    if self.required_src_columns
                    else None,
                    required_dst_columns=",".join(self.required_dst_columns)
                    if self.required_dst_columns
                    else None,
                    required_edge_columns=",".join(self.required_edge_columns)
                    if self.required_edge_columns
                    else None,
                )
                pb_message = pb.GraphFramesAPI(
                    vertices=dataframe_to_proto(self.vertices, session),
                    edges=dataframe_to_proto(self.edges, session),
                )
                pb_message.pregel.CopyFrom(pregel)
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(pb_message)
                return plan

        if (
            (self._col_name is None)
            or (self._initial_expr is None)
            or (self._update_after_agg_msgs_expr is None)
        ):
            raise ValueError("Initial vertex column is not initialized!")

        if self._agg_msg is None:
            raise ValueError("AggMsg is not initialized!")

        return _dataframe_from_plan(
            Pregel(
                max_iter=self._max_iter,
                checkpoint_interval=self._checkpoint_interval,
                vertex_col_name=self._col_name,
                vertex_col_init=self._initial_expr,
                vertex_col_upd=self._update_after_agg_msgs_expr,
                agg_msg=self._agg_msg,
                send2dst=self._send_msg_to_dst,
                send2src=self._send_msg_to_src,
                early_stopping=self._early_stopping,
                use_local_checkpoints=self._use_local_checkpoints,
                initial_active_col=self._initial_active_expr,
                update_active_col=self._update_active_expr,
                stop_if_all_non_active=self._stop_if_all_non_active,
                skip_message_from_non_active=self._skip_messages_from_non_active,
                required_src_columns=self._required_src_columns,
                required_dst_columns=self._required_dst_columns,
                required_edge_columns=self._required_edge_columns,
                storage_level=self._storage_level,
                vertices=self.graph._vertices,
                edges=self.graph._edges,
            ),
            session=self.graph._spark,
        )

    @staticmethod
    def msg() -> Column:
        return cast(Column, F.col("_pregel_msg_"))

    @staticmethod
    def src(colName: str) -> Column:
        return cast(Column, F.col("src." + colName))

    @staticmethod
    def dst(colName: str) -> Column:
        return cast(Column, F.col("dst." + colName))

    @staticmethod
    def edge(colName: str) -> Column:
        return cast(Column, F.col("edge." + colName))


@final
class GraphFrameConnect:
    _ID: str = "id"
    _SRC: str = "src"
    _DST: str = "dst"
    _EDGE: str = "edge"

    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        self._vertices = v
        self._edges = e
        self._spark = v.sparkSession

    @staticmethod
    def _get_pb_api_message(
        vertices: DataFrame, edges: DataFrame, client: SparkConnectClient
    ) -> pb.GraphFramesAPI:
        return pb.GraphFramesAPI(
            vertices=dataframe_to_proto(vertices, client),
            edges=dataframe_to_proto(edges, client),
        )

    @property
    def triplets(self) -> DataFrame:
        @final
        class Triplets(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                super().__init__(None)
                self.v = v
                self.e = e

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triplets.CopyFrom(pb.Triplets())
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(Triplets(self._vertices, self._edges), self._spark)

    @property
    def pregel(self) -> PregelConnect:
        return PregelConnect(self)

    def find(self, pattern: str) -> DataFrame:
        @final
        class Find(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, pattern: str) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.p = pattern

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.find.CopyFrom(pb.Find(pattern=self.p))
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(Find(self._vertices, self._edges, pattern), self._spark)

    def filterVertices(self, condition: str | Column) -> "GraphFrameConnect":
        @final
        class FilterVertices(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, condition: str | Column) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.c = condition

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_vertices.CopyFrom(
                    pb.FilterVertices(condition=col_or_expr)
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        new_vertices = _dataframe_from_plan(
            FilterVertices(self._vertices, self._edges, condition), self._spark
        )
        # Exactly like in the scala-core
        new_edges = self._edges.join(
            new_vertices.withColumn(self._SRC, F.col(self._ID)),
            on=[self._SRC],
            how="left_semi",
        ).join(
            new_vertices.withColumn(self._DST, F.col(self._ID)),
            on=[self._DST],
            how="left_semi",
        )
        return GraphFrameConnect(new_vertices, cast(DataFrame, new_edges))

    def filterEdges(self, condition: str | Column) -> "GraphFrameConnect":
        @final
        class FilterEdges(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, condition: str | Column) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.c = condition

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_edges.CopyFrom(pb.FilterEdges(condition=col_or_expr))
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        new_edges = _dataframe_from_plan(
            FilterEdges(self._vertices, self._edges, condition), self._spark
        )
        return GraphFrameConnect(self._vertices, new_edges)

    def detectingCycles(
        self,
        checkpoint_interval: int,
        use_local_checkpoints: bool,
        intermediate_storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class DetectingCycles(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                checkpoint_interval: int,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.checkpoint_interval = checkpoint_interval
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.detecting_cycles.CopyFrom(
                    pb.DetectingCycles(
                        use_local_checkpoints=self.use_local_checkpoints,
                        checkpoint_interval=self.checkpoint_interval,
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            DetectingCycles(
                self._vertices,
                self._edges,
                checkpoint_interval,
                use_local_checkpoints,
                intermediate_storage_level,
            ),
            self._spark,
        )

    def dropIsolatedVertices(self) -> "GraphFrameConnect":
        @final
        class DropIsolatedVertices(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                super().__init__(None)
                self.v = v
                self.e = e

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.drop_isolated_vertices.CopyFrom(pb.DropIsolatedVertices())
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        new_vertices = _dataframe_from_plan(
            DropIsolatedVertices(self._vertices, self._edges), self._spark
        )
        return GraphFrameConnect(new_vertices, self._edges)

    def bfs(
        self,
        fromExpr: Column | str,
        toExpr: Column | str,
        edgeFilter: Column | str | None = None,
        maxPathLength: int = 10,
    ) -> DataFrame:
        @final
        class BFS(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                from_expr: Column | str,
                to_expr: Column | str,
                edge_filter: Column | str,
                max_path_len: int,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.from_expr = from_expr
                self.to_expr = to_expr
                self.edge_filter = edge_filter
                self.max_path_len = max_path_len

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.bfs.CopyFrom(
                    pb.BFS(
                        from_expr=make_column_or_expr(self.from_expr, session),
                        to_expr=make_column_or_expr(self.to_expr, session),
                        edge_filter=make_column_or_expr(self.edge_filter, session),
                        max_path_length=self.max_path_len,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        actual_edge_filter: Column | str = (
            cast(Column, F.lit(True)) if edgeFilter is None else edgeFilter
        )

        return _dataframe_from_plan(
            BFS(
                v=self._vertices,
                e=self._edges,
                from_expr=fromExpr,
                to_expr=toExpr,
                edge_filter=actual_edge_filter,
                max_path_len=maxPathLength,
            ),
            self._spark,
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
        @final
        class AllPaths(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                from_expr: Column | str,
                to_expr: Column | str,
                edge_filter: Column | str,
                max_path_length: int,
                is_directed: bool,
                checkpoint_interval: int,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.from_expr = from_expr
                self.to_expr = to_expr
                self.edge_filter = edge_filter
                self.max_path_length = max_path_length
                self.is_directed = is_directed
                self.checkpoint_interval = checkpoint_interval
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.all_paths.CopyFrom(
                    pb.AllPaths(
                        from_expr=make_column_or_expr(self.from_expr, session),
                        to_expr=make_column_or_expr(self.to_expr, session),
                        edge_filter=make_column_or_expr(self.edge_filter, session),
                        max_path_length=self.max_path_length,
                        is_directed=self.is_directed,
                        checkpoint_interval=self.checkpoint_interval,
                        use_local_checkpoints=self.use_local_checkpoints,
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        actual_edge_filter: Column | str = (
            cast(Column, F.lit(True)) if edge_filter is None else edge_filter
        )

        return _dataframe_from_plan(
            AllPaths(
                v=self._vertices,
                e=self._edges,
                from_expr=from_expr,
                to_expr=to_expr,
                edge_filter=actual_edge_filter,
                max_path_length=max_path_length,
                is_directed=is_directed,
                checkpoint_interval=checkpoint_interval,
                use_local_checkpoints=use_local_checkpoints,
                storage_level=storage_level,
            ),
            self._spark,
        )

    def aggregateMessages(
        self,
        aggCol: list[Column | str],
        sendToSrc: list[Column | str],
        sendToDst: list[Column | str],
        intermediate_storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class AggregateMessages(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                agg_col: list[Column | str],
                send2src: list[Column | str],
                send2dst: list[Column | str],
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.agg_col = agg_col
                self.send2src = send2src
                self.send2dst = send2dst
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.aggregate_messages.CopyFrom(
                    pb.AggregateMessages(
                        agg_col=[make_column_or_expr(x, session) for x in self.agg_col],
                        send_to_src=[make_column_or_expr(x, session) for x in self.send2src],
                        send_to_dst=[make_column_or_expr(x, session) for x in self.send2dst],
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        if (len(sendToSrc) == 0) and (len(sendToDst) == 0):
            raise ValueError("Either `sendToSrc`, `sendToDst`, or both have to be provided")

        return _dataframe_from_plan(
            AggregateMessages(
                self._vertices,
                self._edges,
                aggCol,
                sendToSrc,
                sendToDst,
                intermediate_storage_level,
            ),
            self._spark,
        )

    def connectedComponents(
        self,
        algorithm: str,
        checkpointInterval: int,
        broadcastThreshold: int,
        useLabelsAsComponents: bool,
        use_local_checkpoints: bool,
        max_iter: int,
        storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class ConnectedComponents(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                algorithm: str,
                checkpoint_interval: int,
                broadcast_threshold: int,
                use_labels_as_components: bool,
                use_local_checkpoints: bool,
                max_iter: int,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.algorithm = algorithm
                self.checkpoint_interval = checkpoint_interval
                self.broadcast_threshold = broadcast_threshold
                self.use_labels_as_components = use_labels_as_components
                self.use_local_checkpoints = use_local_checkpoints
                self.max_iter = max_iter
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.connected_components.CopyFrom(
                    pb.ConnectedComponents(
                        algorithm=self.algorithm,
                        checkpoint_interval=self.checkpoint_interval,
                        broadcast_threshold=self.broadcast_threshold,
                        use_labels_as_components=self.use_labels_as_components,
                        use_local_checkpoints=self.use_local_checkpoints,
                        max_iter=self.max_iter,
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            ConnectedComponents(
                self._vertices,
                self._edges,
                algorithm,
                checkpointInterval,
                broadcastThreshold,
                useLabelsAsComponents,
                use_local_checkpoints,
                max_iter,
                storage_level,
            ),
            self._spark,
        )

    def labelPropagation(
        self,
        maxIter: int,
        algorithm: str,
        use_local_checkpoints: bool,
        checkpoint_interval: int,
        storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class LabelPropagation(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                max_iter: int,
                algorithm: str,
                use_local_checkpoints: bool,
                checkpoint_interval: int,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.max_iter = max_iter
                self.algorithm = algorithm
                self.use_local_checkpoints = use_local_checkpoints
                self.checkpoint_interval = checkpoint_interval
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.label_propagation.CopyFrom(
                    pb.LabelPropagation(
                        algorithm=self.algorithm,
                        max_iter=self.max_iter,
                        use_local_checkpoints=self.use_local_checkpoints,
                        checkpoint_interval=self.checkpoint_interval,
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            LabelPropagation(
                self._vertices,
                self._edges,
                maxIter,
                algorithm,
                use_local_checkpoints,
                checkpoint_interval,
                storage_level,
            ),
            self._spark,
        )

    def neighborhood_aware_cdlp(
        self,
        max_iter: int,
        structural_similarity_multiplier: float,
        ignore_direct_links: bool,
        use_local_checkpoints: bool,
        checkpoint_interval: int,
        storage_level: StorageLevel,
        is_directed: bool,
        lg_nom_entries: int,
        initial_label_col: str | None,
    ) -> DataFrame:
        @final
        class NeighborhoodAwareCDLP(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                max_iter: int,
                structural_similarity_multiplier: float,
                ignore_direct_links: bool,
                use_local_checkpoints: bool,
                checkpoint_interval: int,
                storage_level: StorageLevel,
                is_directed: bool,
                lg_nom_entries: int,
                initial_label_col: str | None,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.max_iter = max_iter
                self.structural_similarity_multiplier = structural_similarity_multiplier
                self.ignore_direct_links = ignore_direct_links
                self.use_local_checkpoints = use_local_checkpoints
                self.checkpoint_interval = checkpoint_interval
                self.storage_level = storage_level
                self.is_directed = is_directed
                self.lg_nom_entries = lg_nom_entries
                self.initial_label_col = initial_label_col

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.neighborhood_aware_cdlp.CopyFrom(
                    pb.NeighborhoodAwareCDLP(
                        max_iter=self.max_iter,
                        structural_similarity_multiplier=self.structural_similarity_multiplier,
                        ignore_direct_links=self.ignore_direct_links,
                        use_local_checkpoints=self.use_local_checkpoints,
                        checkpoint_interval=self.checkpoint_interval,
                        storage_level=storage_level_to_proto(self.storage_level),
                        is_directed=self.is_directed,
                        lg_nom_entries=self.lg_nom_entries,
                        initial_label_col=self.initial_label_col,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            NeighborhoodAwareCDLP(
                self._vertices,
                self._edges,
                max_iter,
                structural_similarity_multiplier,
                ignore_direct_links,
                use_local_checkpoints,
                checkpoint_interval,
                storage_level,
                is_directed,
                lg_nom_entries,
                initial_label_col,
            ),
            self._spark,
        )

    def _update_page_rank_edge_weights(self, new_vertices: DataFrame) -> "GraphFrameConnect":
        cols2select = [col for col in self._edges.columns if col != "weight"] + ["weight"]
        out_degrees = self._edges.groupBy(self._SRC).agg(F.count("*").alias("outDegree"))
        new_edges = (
            self._edges.join(out_degrees, on=[self._SRC], how="inner")
            .withColumn("weight", F.lit(1.0) / F.col("outDegree"))
            .select(*cols2select)
        )
        return GraphFrameConnect(new_vertices, cast(DataFrame, new_edges))

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: str | int | None = None,
        maxIter: int | None = None,
        tol: float | None = None,
    ) -> "GraphFrameConnect":
        @final
        class PageRank(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                reset_prob: float,
                source_id: str | int | None,
                max_iter: int | None,
                tol: float | None,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.reset_prob = reset_prob
                self.source_id = source_id
                self.max_iter = max_iter
                self.tol = tol

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.page_rank.CopyFrom(
                    pb.PageRank(
                        reset_probability=self.reset_prob,
                        source_id=(
                            None if self.source_id is None else make_str_or_long_id(self.source_id)
                        ),
                        max_iter=self.max_iter,
                        tol=self.tol,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        if (maxIter is None) == (tol is None):
            # TODO: in classic it is not an axception but assert;
            # at the same time I think it should be an exception.
            raise ValueError("Exactly one of maxIter or tol should be set.")

        new_vertices = _dataframe_from_plan(
            PageRank(
                self._vertices,
                self._edges,
                reset_prob=resetProbability,
                source_id=sourceId,
                max_iter=maxIter,
                tol=tol,
            ),
            self._spark,
        )
        # TODO: should this part to be optional? Like 'compute_edge_weights'?
        return self._update_page_rank_edge_weights(new_vertices)

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: list[str | int] | None = None,
        maxIter: int | None = None,
    ) -> "GraphFrameConnect":
        @final
        class ParallelPersonalizedPageRank(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                reset_prob: float,
                source_ids: list[str | int],
                max_iter: int,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.reset_prob = reset_prob
                self.source_ids = source_ids
                self.max_iter = max_iter

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.parallel_personalized_page_rank.CopyFrom(
                    pb.ParallelPersonalizedPageRank(
                        reset_probability=self.reset_prob,
                        source_ids=[make_str_or_long_id(raw_id) for raw_id in self.source_ids],
                        max_iter=self.max_iter,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        assert sourceIds is not None and len(sourceIds) > 0, (
            "Source vertices Ids sourceIds must be provided"
        )
        assert maxIter is not None, "Max number of iterations maxIter must be provided"

        new_vertices = _dataframe_from_plan(
            ParallelPersonalizedPageRank(
                self._vertices,
                self._edges,
                reset_prob=resetProbability,
                source_ids=sourceIds,
                max_iter=maxIter,
            ),
            self._spark,
        )
        return self._update_page_rank_edge_weights(new_vertices)

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: str | None = None
    ) -> DataFrame:
        @final
        class PowerIterationClustering(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                k: int,
                max_iter: int,
                weight_col: str | None,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.k = k
                self.max_iter = max_iter
                self.weight_col = weight_col

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.power_iteration_clustering.CopyFrom(
                    pb.PowerIterationClustering(
                        k=self.k,
                        max_iter=self.max_iter,
                        weight_col=self.weight_col,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            PowerIterationClustering(self._vertices, self._edges, k, maxIter, weightCol),
            self._spark,
        )

    def shortestPaths(
        self,
        landmarks: list[str | int],
        algorithm: str,
        use_local_checkpoints: bool,
        checkpoint_interval: int,
        storage_level: StorageLevel,
        is_directed: bool,
    ) -> DataFrame:
        @final
        class ShortestPaths(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                landmarks: list[str | int],
                algorithm: str,
                use_local_checkpoints: bool,
                checkpoint_interval: int,
                storage_level: StorageLevel,
                is_directed: bool,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.landmarks = landmarks
                self.algorithm = algorithm
                self.use_local_checkpoints = use_local_checkpoints
                self.checkpoint_interval = checkpoint_interval
                self.storage_level = storage_level
                self.is_directed = is_directed

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.shortest_paths.CopyFrom(
                    pb.ShortestPaths(
                        landmarks=[make_str_or_long_id(raw_id) for raw_id in self.landmarks],
                        algorithm=self.algorithm,
                        use_local_checkpoints=self.use_local_checkpoints,
                        checkpoint_interval=self.checkpoint_interval,
                        storage_level=storage_level_to_proto(self.storage_level),
                        is_directed=self.is_directed,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            ShortestPaths(
                self._vertices,
                self._edges,
                landmarks,
                algorithm,
                use_local_checkpoints,
                checkpoint_interval,
                storage_level,
                is_directed,
            ),
            self._spark,
        )

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        @final
        class StronglyConnectedComponents(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, max_iter: int) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.max_iter = max_iter

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.strongly_connected_components.CopyFrom(
                    pb.StronglyConnectedComponents(max_iter=self.max_iter)
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            StronglyConnectedComponents(self._vertices, self._edges, maxIter),
            self._spark,
        )

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
        @final
        class SVDPlusPlus(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                rank: int,
                max_iter: int,
                min_value: float,
                max_value: float,
                gamma1: float,
                gamma2: float,
                gamma6: float,
                gamma7: float,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.rank = rank
                self.max_iter = max_iter
                self.min_value = min_value
                self.max_value = max_value
                self.gamma1 = gamma1
                self.gamma2 = gamma2
                self.gamma6 = gamma6
                self.gamma7 = gamma7

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.svd_plus_plus.CopyFrom(
                    pb.SVDPlusPlus(
                        rank=self.rank,
                        max_iter=self.max_iter,
                        min_value=self.min_value,
                        max_value=self.max_value,
                        gamma1=self.gamma1,
                        gamma2=self.gamma2,
                        gamma6=self.gamma6,
                        gamma7=self.gamma7,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        output = _dataframe_from_plan(
            SVDPlusPlus(
                self._vertices,
                self._edges,
                rank=rank,
                max_iter=maxIter,
                min_value=minValue,
                max_value=maxValue,
                gamma1=gamma1,
                gamma2=gamma2,
                gamma6=gamma6,
                gamma7=gamma7,
            ),
            self._spark,
        )

        loss_row = output.select("loss").first()
        assert loss_row is not None
        return (cast(DataFrame, output.drop("loss")), float(loss_row["loss"]))

    def triangleCount(
        self, storage_level: StorageLevel, algorithm: str, log_nom_entries: int
    ) -> DataFrame:
        @final
        class TriangleCount(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, storage_level: StorageLevel) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triangle_count.CopyFrom(
                    pb.TriangleCount(
                        storage_level=storage_level_to_proto(self.storage_level),
                        algorithm=algorithm,
                        lg_nom_entries=log_nom_entries,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            TriangleCount(self._vertices, self._edges, storage_level), self._spark
        )

    def maximal_independent_set(
        self,
        checkpoint_interval: int,
        storage_level: StorageLevel,
        use_local_checkpoints: bool,
        seed: int,
    ) -> DataFrame:
        @final
        class MaximalIndependentSet(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                checkpoint_interval: int,
                storage_level: StorageLevel,
                use_local_checkpoints: bool,
                seed: int,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.checkpoint_interval = checkpoint_interval
                self.storage_level = storage_level
                self.use_local_checkpoints = use_local_checkpoints
                self.seed = seed

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.mis.CopyFrom(
                    pb.MaximalIndependentSet(
                        checkpoint_interval=self.checkpoint_interval,
                        storage_level=storage_level_to_proto(self.storage_level),
                        use_local_checkpoints=self.use_local_checkpoints,
                        seed=self.seed,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            MaximalIndependentSet(
                self._vertices,
                self._edges,
                checkpoint_interval,
                storage_level,
                use_local_checkpoints,
                seed,
            ),
            self._spark,
        )

    def k_core(
        self,
        checkpoint_interval: int,
        use_local_checkpoints: bool,
        storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class KCore(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                checkpoint_interval: int,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.checkpoint_interval = checkpoint_interval
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.kcore.CopyFrom(
                    pb.KCore(
                        checkpoint_interval=self.checkpoint_interval,
                        use_local_checkpoints=self.use_local_checkpoints,
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            KCore(
                self._vertices,
                self._edges,
                checkpoint_interval,
                use_local_checkpoints,
                storage_level,
            ),
            self._spark,
        )

    def hyper_anf(
        self,
        n_hops: int,
        lg_nom_entries: int,
        edge_filter: Column | str | None,
        checkpoint_interval: int,
        use_local_checkpoints: bool,
        storage_level: StorageLevel,
    ) -> DataFrame:
        @final
        class HyperANF(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                n_hops: int,
                lg_nom_entries: int,
                edge_filter: Column | str | None,
                checkpoint_interval: int,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.n_hops = n_hops
                self.lg_nom_entries = lg_nom_entries
                self.edge_filter = edge_filter
                self.checkpoint_interval = checkpoint_interval
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                ha_message = pb.HyperANF(
                    n_hops=self.n_hops,
                    lg_nom_entries=self.lg_nom_entries,
                    checkpoint_interval=self.checkpoint_interval,
                    use_local_checkpoints=self.use_local_checkpoints,
                    storage_level=storage_level_to_proto(self.storage_level),
                )
                if self.edge_filter is not None:
                    ha_message.edges_filter_expression.CopyFrom(
                        make_column_or_expr(self.edge_filter, session)
                    )
                graphframes_api_call.hyper_anf.CopyFrom(ha_message)
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            HyperANF(
                self._vertices,
                self._edges,
                n_hops,
                lg_nom_entries,
                edge_filter,
                checkpoint_interval,
                use_local_checkpoints,
                storage_level,
            ),
            self._spark,
        )

    def aggregate_neighbors(
        self,
        starting_vertices: Column | str,
        max_hops: int,
        accumulator_names: list[str],
        accumulator_inits: list[Column | str],
        accumulator_updates: list[Column | str],
        stopping_condition: Column | str | None = None,
        target_condition: Column | str | None = None,
        required_vertex_attributes: list[str] | None = None,
        required_edge_attributes: list[str] | None = None,
        edge_filter: Column | str | None = None,
        remove_loops: bool = False,
        checkpoint_interval: int = 0,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        @final
        class AggregateNeighbors(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                starting_vertices: Column | str,
                max_hops: int,
                accumulator_names: list[str],
                accumulator_inits: list[Column | str],
                accumulator_updates: list[Column | str],
                stopping_condition: Column | str | None,
                target_condition: Column | str | None,
                required_vertex_attributes: list[str] | None,
                required_edge_attributes: list[str] | None,
                edge_filter: Column | str | None,
                remove_loops: bool,
                checkpoint_interval: int,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.starting_vertices = starting_vertices
                self.max_hops = max_hops
                self.accumulator_names = accumulator_names
                self.accumulator_inits = accumulator_inits
                self.accumulator_updates = accumulator_updates
                self.stopping_condition = stopping_condition
                self.target_condition = target_condition
                self.required_vertex_attributes = required_vertex_attributes or []
                self.required_edge_attributes = required_edge_attributes or []
                self.edge_filter = edge_filter
                self.remove_loops = remove_loops
                self.checkpoint_interval = checkpoint_interval
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                # Build the protobuf message
                an_message = pb.AggregateNeighbors(
                    starting_vertices=make_column_or_expr(self.starting_vertices, session),
                    max_hops=self.max_hops,
                    accumulator_names=self.accumulator_names,
                    accumulator_inits=[
                        make_column_or_expr(init, session) for init in self.accumulator_inits
                    ],
                    accumulator_updates=[
                        make_column_or_expr(update, session) for update in self.accumulator_updates
                    ],
                    required_vertex_attributes=self.required_vertex_attributes,
                    required_edge_attributes=self.required_edge_attributes,
                    remove_loops=self.remove_loops,
                    checkpoint_interval=self.checkpoint_interval,
                    use_local_checkpoints=self.use_local_checkpoints,
                    storage_level=storage_level_to_proto(self.storage_level),
                )

                # Add optional fields if present
                if self.stopping_condition is not None:
                    an_message.stopping_condition.CopyFrom(
                        make_column_or_expr(self.stopping_condition, session)
                    )

                if self.target_condition is not None:
                    an_message.target_condition.CopyFrom(
                        make_column_or_expr(self.target_condition, session)
                    )

                if self.edge_filter is not None:
                    an_message.edge_filter.CopyFrom(make_column_or_expr(self.edge_filter, session))

                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.aggregate_neighbors.CopyFrom(an_message)
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            AggregateNeighbors(
                self._vertices,
                self._edges,
                starting_vertices,
                max_hops,
                accumulator_names,
                accumulator_inits,
                accumulator_updates,
                stopping_condition,
                target_condition,
                required_vertex_attributes,
                required_edge_attributes,
                edge_filter,
                remove_loops,
                checkpoint_interval,
                use_local_checkpoints,
                storage_level,
            ),
            self._spark,
        )

    def rw_embeddings(self, params: _RandomWalksEmbeddingsParameters) -> DataFrame:
        @final
        class RWEmbeddings(LogicalPlan):
            def __init__(
                self, v: DataFrame, e: DataFrame, params: _RandomWalksEmbeddingsParameters
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.params = params

            @override
            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.rw_embeddings.CopyFrom(
                    pb.RandomWalkEmbeddings(
                        use_edge_direction=self.params.use_edge_direction,
                        rw_model=self.params.rw_model,
                        rw_max_nbrs=self.params.rw_max_nbrs,
                        rw_num_walks_per_node=self.params.rw_num_walks_per_node,
                        rw_batch_size=self.params.rw_batch_size,
                        rw_num_batches=self.params.rw_num_batches,
                        rw_seed=self.params.rw_seed,
                        rw_restart_probability=self.params.rw_restart_probability,
                        rw_temporary_prefix=self.params.rw_temporary_prefix,
                        rw_cached_walks=self.params.rw_cached_walks,
                        sequence_model=self.params.sequence_model,
                        hash2vec_context_size=self.params.hash2vec_context_size,
                        hash2vec_num_partitions=self.params.hash2vec_num_partitions,
                        hash2vec_embeddings_dim=self.params.hash2vec_embeddings_dim,
                        hash2vec_decay_function=self.params.hash2vec_decay_function,
                        hash2vec_gaussian_sigma=self.params.hash2vec_gaussian_sigma,
                        hash2vec_hashing_seed=self.params.hash2vec_hashing_seed,
                        hash2vec_sign_seed=self.params.hash2vec_sign_seed,
                        hash2vec_do_l2_norm=self.params.hash2vec_do_l2_norm,
                        hash2vec_safe_l2=self.params.hash2vec_safe_l2,
                        word2vec_max_iter=self.params.word2vec_max_iter,
                        word2vec_embeddings_dim=self.params.word2vec_embeddings_dim,
                        word2vec_window_size=self.params.word2vec_window_size,
                        word2vec_num_partitions=self.params.word2vec_num_partitions,
                        word2vec_min_count=self.params.word2vec_min_count,
                        word2vec_max_sentence_length=self.params.word2vec_max_sentence_length,
                        word2vec_seed=self.params.word2vec_seed,
                        word2vec_step_size=self.params.word2vec_step_size,
                        aggregate_neighbors=self.params.aggregate_neighbors,
                        aggregate_neighbors_max_nbrs=self.params.aggregate_neighbors_max_nbrs,
                        aggregate_neighbors_seed=self.params.aggregate_neighbors_seed,
                        clean_up_after_run=self.params.clean_up_after_run,
                    )
                )
                plan = self._create_proto_relation()
                plan.graph_frames.CopyFrom(graphframes_api_call)
                return plan

        return _dataframe_from_plan(RWEmbeddings(self._vertices, self._edges, params), self._spark)
