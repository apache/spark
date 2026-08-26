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

from py4j.java_gateway import JavaObject

from pyspark import SparkContext
from pyspark.graphframes.classic.utils import storage_level_to_jvm
from pyspark.graphframes.internal.utils import _RandomWalksEmbeddingsParameters
from pyspark.graphframes.lib import Pregel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.classic.column import Column, _to_seq
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel


def _from_java_gf(jgf: JavaObject, spark: SparkSession) -> "GraphFrame":
    """
    (internal) creates a python GraphFrame wrapper from a java GraphFrame.

    :param jgf:
    """
    pv = DataFrame(jgf.vertices(), spark)
    pe = DataFrame(jgf.edges(), spark)
    return GraphFrame(pv, pe)


def _java_api(jsc: SparkContext) -> JavaObject:
    javaClassName = "org.apache.spark.graphframes.GraphFramePythonAPI"
    if jsc._jvm is None:
        raise RuntimeError(
            "Spark Driver's JVM is dead or did not start properly. See driver logs for details."
        )
    return (
        jsc._jvm.Thread.currentThread()
        .getContextClassLoader()
        .loadClass(javaClassName)
        .newInstance()
    )


@final
class GraphFrame:
    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        self._vertices = v
        self._edges = e
        self._spark = v.sparkSession
        self._sc = self._spark._sc
        self._jvm_gf_api = _java_api(self._sc)
        self._jvm = self._spark._jvm

        self._ATTR: str = self._jvm_gf_api.ATTR()

        self._jvm_graph = self._jvm_gf_api.createGraph(v._jdf, e._jdf)

    @property
    def triplets(self) -> DataFrame:
        jdf = self._jvm_graph.triplets()
        return DataFrame(jdf, self._spark)

    @property
    def pregel(self) -> Pregel:
        return Pregel(self)

    def find(self, pattern: str) -> DataFrame:
        jdf = self._jvm_graph.find(pattern)
        return DataFrame(jdf, self._spark)

    def filterVertices(self, condition: str | Column) -> "GraphFrame":
        if isinstance(condition, str):
            jdf = self._jvm_graph.filterVertices(condition)
        else:
            jdf = self._jvm_graph.filterVertices(condition._jc)

        return _from_java_gf(jdf, self._spark)

    def filterEdges(self, condition: str | Column) -> "GraphFrame":
        if isinstance(condition, str):
            jdf = self._jvm_graph.filterEdges(condition)
        else:
            jdf = self._jvm_graph.filterEdges(condition._jc)

        return _from_java_gf(jdf, self._spark)

    def detectingCycles(
        self,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        intermediate_storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        builder = self._jvm_graph.detectingCycles()
        builder.setUseLocalCheckpoints(use_local_checkpoints)
        builder.setCheckpointInterval(checkpoint_interval)
        builder.setIntermediateStorageLevel(
            storage_level_to_jvm(intermediate_storage_level, self._spark)
        )
        jdf = builder.run()

        return DataFrame(jdf, self._spark)

    def dropIsolatedVertices(self) -> "GraphFrame":
        jdf = self._jvm_graph.dropIsolatedVertices()
        return _from_java_gf(jdf, self._spark)

    def bfs(
        self,
        fromExpr: str,
        toExpr: str,
        edgeFilter: str | None = None,
        maxPathLength: int = 10,
    ) -> DataFrame:
        builder = (
            self._jvm_graph.bfs().fromExpr(fromExpr).toExpr(toExpr).maxPathLength(maxPathLength)
        )
        if edgeFilter is not None:
            builder.edgeFilter(edgeFilter)
        jdf = builder.run()
        return DataFrame(jdf, self._spark)

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
        builder = self._jvm_graph.allPaths()
        if isinstance(from_expr, Column):
            builder.fromExpr(from_expr._jc)
        else:
            builder.fromExpr(from_expr)
        if isinstance(to_expr, Column):
            builder.toExpr(to_expr._jc)
        else:
            builder.toExpr(to_expr)
        builder.maxPathLength(max_path_length).setIsDirected(is_directed)
        if edge_filter is not None:
            if isinstance(edge_filter, Column):
                builder.edgeFilter(edge_filter._jc)
            else:
                builder.edgeFilter(edge_filter)

        if checkpoint_interval > 0:
            builder.setCheckpointInterval(checkpoint_interval)

        builder.setUseLocalCheckpoints(use_local_checkpoints)
        builder.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))

        jdf = builder.run()
        return DataFrame(jdf, self._spark)

    def aggregateMessages(
        self,
        aggCol: list[Column | str],
        sendToSrc: list[Column | str],
        sendToDst: list[Column | str],
        intermediate_storage_level: StorageLevel,
    ) -> DataFrame:
        builder = self._jvm_graph.aggregateMessages()
        builder.setIntermediateStorageLevel(
            storage_level_to_jvm(intermediate_storage_level, self._spark)
        )
        if len(sendToSrc) == 1:
            if isinstance(sendToSrc[0], Column):
                builder.sendToSrc(sendToSrc[0]._jc)
            elif isinstance(sendToSrc[0], str):
                builder.sendToSrc(sendToSrc[0])
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        elif len(sendToSrc) > 1:
            if all(isinstance(x, Column) for x in sendToSrc):
                send2src = [x._jc for x in cast(list[Column], sendToSrc)]
                builder.sendToSrc(send2src[0], _to_seq(self._sc, send2src[1:]))
            elif all(isinstance(x, str) for x in sendToSrc):
                builder.sendToSrc(sendToSrc[0], _to_seq(self._sc, sendToSrc[1:]))
            else:
                raise TypeError(
                    "Multiple messages should all be `Column` or `str`, not a mix of them."
                )

        if len(sendToDst) == 1:
            if isinstance(sendToDst[0], Column):
                builder.sendToDst(sendToDst[0]._jc)
            elif isinstance(sendToDst[0], str):
                builder.sendToDst(sendToDst[0])
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        elif len(sendToDst) > 1:
            if all(isinstance(x, Column) for x in sendToDst):
                send2dst = [x._jc for x in cast(list[Column], sendToDst)]
                builder.sendToDst(send2dst[0], _to_seq(self._sc, send2dst[1:]))
            elif all(isinstance(x, str) for x in sendToDst):
                builder.sendToDst(sendToDst[0], _to_seq(self._sc, sendToDst[1:]))
            else:
                raise TypeError(
                    "Multiple messages should all be `Column` or `str`, not a mix of them."
                )

        if len(aggCol) == 1:
            if isinstance(aggCol[0], Column):
                jdf = builder.agg(aggCol[0]._jc)
            elif isinstance(aggCol[0], str):
                jdf = builder.agg(aggCol[0])
        elif len(aggCol) > 1:
            if all(isinstance(x, Column) for x in aggCol):
                jdf = builder.agg(
                    cast(Column, aggCol[0])._jc,
                    _to_seq(self._sc, [x._jc for x in cast(list[Column], aggCol)]),
                )
            elif all(isinstance(x, str) for x in aggCol):
                jdf = builder.agg(aggCol[0], _to_seq(self._sc, aggCol[1:]))
            else:
                raise TypeError(
                    "Multiple agg cols should all be `Column` or `str`, not a mix of them."
                )
        return DataFrame(jdf, self._spark)

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
        java_cc = self._jvm_graph.connectedComponents()
        java_cc.setAlgorithm(algorithm)
        java_cc.setCheckpointInterval(checkpointInterval)
        java_cc.setBroadcastThreshold(broadcastThreshold)
        java_cc.setUseLabelsAsComponents(useLabelsAsComponents)
        java_cc.setUseLocalCheckpoints(use_local_checkpoints)
        java_cc.maxIter(max_iter)
        java_cc.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        jdf = java_cc.run()

        return DataFrame(jdf, self._spark)

    def labelPropagation(
        self,
        maxIter: int,
        algorithm: str,
        use_local_checkpoints: bool,
        checkpoint_interval: int,
        storage_level: StorageLevel,
    ) -> DataFrame:
        java_cdlp = self._jvm_graph.labelPropagation()
        java_cdlp.maxIter(maxIter)
        java_cdlp.setAlgorithm(algorithm)
        java_cdlp.setUseLocalCheckpoints(use_local_checkpoints)
        java_cdlp.setCheckpointInterval(checkpoint_interval)
        java_cdlp.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        jdf = java_cdlp.run()

        return DataFrame(jdf, self._spark)

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
        java_nacdlp = self._jvm_graph.structureAwareLabelPropagation()
        java_nacdlp.maxIter(max_iter)
        java_nacdlp.setStructuralSimilarityMultiplier(structural_similarity_multiplier)
        java_nacdlp.setIgnoreDirectLinks(ignore_direct_links)
        java_nacdlp.setUseLocalCheckpoints(use_local_checkpoints)
        java_nacdlp.setCheckpointInterval(checkpoint_interval)
        java_nacdlp.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        java_nacdlp.setIsDirected(is_directed)
        java_nacdlp.setLgNomEntries(lg_nom_entries)

        if initial_label_col is not None:
            java_nacdlp.setInitialLabelCol(initial_label_col)

        jdf = java_nacdlp.run()
        return DataFrame(jdf, self._spark)

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: str | int | None = None,
        maxIter: int | None = None,
        tol: float | None = None,
    ) -> "GraphFrame":
        builder = self._jvm_graph.pageRank().resetProbability(resetProbability)
        if sourceId is not None:
            builder.sourceId(sourceId)
        if maxIter is not None:
            builder.maxIter(maxIter)
            assert tol is None, "Exactly one of maxIter or tol should be set."
        else:
            assert tol is not None, "Exactly one of maxIter or tol should be set."
            builder.tol(tol)
        jgf = builder.run()
        return _from_java_gf(jgf, self._spark)

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: list[str | int] | None = None,
        maxIter: int | None = None,
    ) -> "GraphFrame":
        assert sourceIds is not None and len(sourceIds) > 0, (
            "Source vertices Ids sourceIds must be provided"
        )
        assert maxIter is not None, "Max number of iterations maxIter must be provided"
        jvm = self._sc._jvm
        assert jvm is not None
        sourceIds = jvm.PythonUtils.toArray(sourceIds)
        builder = self._jvm_graph.parallelPersonalizedPageRank()
        builder.resetProbability(resetProbability)
        builder.sourceIds(sourceIds)
        builder.maxIter(maxIter)
        jgf = builder.run()
        return _from_java_gf(jgf, self._spark)

    def shortestPaths(
        self,
        landmarks: list[str | int],
        algorithm: str,
        use_local_checkpoints: bool,
        checkpoint_interval: int,
        storage_level: StorageLevel,
        is_directed: bool,
    ) -> DataFrame:
        java_sp = self._jvm_graph.shortestPaths()
        java_sp.landmarks(landmarks)
        java_sp.setAlgorithm(algorithm)
        java_sp.setUseLocalCheckpoints(use_local_checkpoints)
        java_sp.setCheckpointInterval(checkpoint_interval)
        java_sp.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        java_sp.setIsDirected(is_directed)
        jdf = java_sp.run()

        return DataFrame(jdf, self._spark)

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        builder = self._jvm_graph.stronglyConnectedComponents()
        builder.maxIter(maxIter)
        jdf = builder.run()
        return DataFrame(jdf, self._spark)

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
        # This call is actually useless, because one needs to build the configuration first...
        builder = self._jvm_graph.svdPlusPlus()
        builder.rank(rank).maxIter(maxIter).minValue(minValue).maxValue(maxValue)
        builder.gamma1(gamma1).gamma2(gamma2).gamma6(gamma6).gamma7(gamma7)
        jdf = builder.run()
        loss = builder.loss()
        v = DataFrame(jdf, self._spark)
        return (v, loss)

    def triangleCount(
        self, storage_level: StorageLevel, algorithm: str, log_nom_entries: int
    ) -> DataFrame:
        builder = self._jvm_graph.triangleCount()
        builder.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        builder.setAlgorithm(algorithm)
        builder.setLgNomEntries(log_nom_entries)
        jdf = builder.run()
        return DataFrame(jdf, self._spark)

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: str | None = None
    ) -> DataFrame:
        jvm = self._spark._jvm
        assert jvm is not None
        if weightCol:
            weightCol = jvm.scala.Option.apply(weightCol)
        else:
            weightCol = jvm.scala.Option.empty()
        jdf = self._jvm_graph.powerIterationClustering(k, maxIter, weightCol)
        return DataFrame(jdf, self._spark)

    def maximal_independent_set(
        self,
        checkpoint_interval: int,
        storage_level: StorageLevel,
        use_local_checkpoints: bool,
        seed: int,
    ) -> DataFrame:
        builder = self._jvm_graph.maximalIndependentSet()
        builder.setCheckpointInterval(checkpoint_interval)
        builder.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        builder.setUseLocalCheckpoints(use_local_checkpoints)

        jdf = builder.run(seed)
        return DataFrame(jdf, self._spark)

    def k_core(
        self,
        checkpoint_interval: int,
        use_local_checkpoints: bool,
        storage_level: StorageLevel,
    ) -> DataFrame:
        java_kcore = self._jvm_graph.kCore()
        java_kcore.setUseLocalCheckpoints(use_local_checkpoints)
        java_kcore.setCheckpointInterval(checkpoint_interval)
        java_kcore.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        jdf = java_kcore.run()

        return DataFrame(jdf, self._spark)

    def hyper_anf(
        self,
        n_hops: int,
        lg_nom_entries: int,
        edge_filter: Column | str | None,
        checkpoint_interval: int,
        use_local_checkpoints: bool,
        storage_level: StorageLevel,
    ) -> DataFrame:
        builder = self._jvm_graph.hyperANF()
        builder.setNHops(n_hops)
        builder.setLgNomEntries(lg_nom_entries)
        if edge_filter is not None:
            if isinstance(edge_filter, Column):
                builder.setEdgesFilterExpression(edge_filter._jc)
            else:
                builder.setEdgesFilterExpression(edge_filter)
        builder.setCheckpointInterval(checkpoint_interval)
        builder.setUseLocalCheckpoints(use_local_checkpoints)
        builder.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))
        jdf = builder.run()

        return DataFrame(jdf, self._spark)

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
        builder = self._jvm_graph.aggregateNeighbors()

        # Set required parameters
        if isinstance(starting_vertices, Column):
            builder.setStartingVertices(starting_vertices._jc)
        else:
            builder.setStartingVertices(starting_vertices)

        builder.setMaxHops(max_hops)

        jvm = self._sc._jvm
        assert jvm is not None
        # Handle accumulators with proper py4j conversion
        if len(accumulator_names) > 0:
            names_seq = jvm.scala.collection.JavaConverters.asScalaBuffer(accumulator_names).toSeq()

            inits_list = []
            for init in accumulator_inits:
                if isinstance(init, Column):
                    inits_list.append(init._jc)
                else:
                    inits_list.append(F.expr(init)._jc)
            inits_seq = jvm.scala.collection.JavaConverters.asScalaBuffer(inits_list).toSeq()

            updates_list = []
            for update in accumulator_updates:
                if isinstance(update, Column):
                    updates_list.append(update._jc)
                else:
                    updates_list.append(F.expr(update)._jc)
            updates_seq = jvm.scala.collection.JavaConverters.asScalaBuffer(updates_list).toSeq()

            builder.setAccumulators(names_seq, inits_seq, updates_seq)

        # Set optional parameters
        if stopping_condition is not None:
            if isinstance(stopping_condition, Column):
                builder.setStoppingCondition(stopping_condition._jc)
            else:
                builder.setStoppingCondition(stopping_condition)

        if target_condition is not None:
            if isinstance(target_condition, Column):
                builder.setTargetCondition(target_condition._jc)
            else:
                builder.setTargetCondition(target_condition)

        if required_vertex_attributes is not None and len(required_vertex_attributes) > 0:
            attrs_seq = jvm.scala.collection.JavaConverters.asScalaBuffer(
                required_vertex_attributes
            ).toSeq()
            builder.setRequiredVertexAttributes(attrs_seq)

        if required_edge_attributes is not None and len(required_edge_attributes) > 0:
            attrs_seq = jvm.scala.collection.JavaConverters.asScalaBuffer(
                required_edge_attributes
            ).toSeq()
            builder.setRequiredEdgeAttributes(attrs_seq)

        if edge_filter is not None:
            if isinstance(edge_filter, Column):
                builder.setEdgeFilter(edge_filter._jc)
            else:
                builder.setEdgeFilter(edge_filter)

        builder.setRemoveLoops(remove_loops)

        if checkpoint_interval > 0:
            builder.setCheckpointInterval(checkpoint_interval)

        builder.setUseLocalCheckpoints(use_local_checkpoints)
        builder.setIntermediateStorageLevel(storage_level_to_jvm(storage_level, self._spark))

        jdf = builder.run()
        assert jdf is not None

        return DataFrame(jdf, self._spark)

    def rw_embeddings(self, params: _RandomWalksEmbeddingsParameters) -> DataFrame:
        assert self._jvm is not None
        j_rw_embeddings = self._jvm.org.apache.spark.graphframes.embeddings.RandomWalkEmbeddings
        assert j_rw_embeddings is not None
        jdf: JavaObject = j_rw_embeddings.pythonAPI(
            self._jvm_graph,
            params.use_edge_direction,
            params.rw_model,
            params.rw_max_nbrs,
            params.rw_num_walks_per_node,
            params.rw_batch_size,
            params.rw_num_batches,
            params.rw_seed,
            params.rw_restart_probability,
            params.rw_temporary_prefix,
            params.rw_cached_walks,
            params.sequence_model,
            params.hash2vec_context_size,
            params.hash2vec_num_partitions,
            params.hash2vec_embeddings_dim,
            params.hash2vec_decay_function,
            params.hash2vec_gaussian_sigma,
            params.hash2vec_hashing_seed,
            params.hash2vec_sign_seed,
            params.hash2vec_do_l2_norm,
            params.hash2vec_safe_l2,
            params.word2vec_max_iter,
            params.word2vec_embeddings_dim,
            params.word2vec_window_size,
            params.word2vec_num_partitions,
            params.word2vec_min_count,
            params.word2vec_max_sentence_length,
            params.word2vec_seed,
            params.word2vec_step_size,
            params.aggregate_neighbors,
            params.aggregate_neighbors_max_nbrs,
            params.aggregate_neighbors_seed,
            params.clean_up_after_run,
        )
        assert jdf is not None

        return DataFrame(jdf, self._spark)
