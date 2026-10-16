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

import gc
import os
import shutil
import tempfile
import uuid
from unittest.mock import patch

from pyspark import SparkConf, StorageLevel
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.tests import test_graphframes as upstream
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import CachedRemoteRelation, RemoveRemoteCachedRelation
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import eventually


def _cached_result(test, session, df_id):
    if test._legacy_sc is None:
        return None
    service = getattr(
        getattr(test._legacy_sc._jvm.org.apache.spark.sql.connect.service, "SparkConnectService$"),
        "MODULE$",
    )
    holder = service.getOrCreateIsolatedSession(
        session.client._user_id or "", session.client._session_id
    )
    return holder.dataFrameCache().getOrDefault(df_id, None)


def _remove_cached_result(result):
    client = result.sparkSession.client
    result.unpersist(blocking=True)
    client.execute_command(RemoveRemoteCachedRelation(result._plan).command(client))


class GraphFramesUpstreamConnectTests(ReusedConnectTestCase):
    _checkpoint_dir = tempfile.mkdtemp(prefix="spark-graphframes-connect-")

    @classmethod
    def conf(cls) -> SparkConf:
        return (
            super()
            .conf()
            .set("spark.driver.memory", "4g")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.checkpoint.dir", cls._checkpoint_dir)
        )

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if cls._legacy_sc is not None:
            cls._legacy_sc.setCheckpointDir(cls._checkpoint_dir)

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        if os.path.exists(cls._checkpoint_dir):
            shutil.rmtree(cls._checkpoint_dir)

    def local_graph(self) -> GraphFrame:
        vertices = self.spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])
        edges = self.spark.createDataFrame(
            [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")],
            ["src", "dst", "action"],
        )
        return GraphFrame(vertices, edges)

    def test_construction(self) -> None:
        upstream.test_construction(self.spark, self.local_graph())

    def test_page_rank(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                upstream.test_page_rank(self.spark, args)

    def test_pregel_early_stopping(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                upstream.test_pregel_early_stopping(self.spark, args)

    def test_connected_components(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            for cc_args in [(-1, True), (10000, True), (-1, False), (10000, False)]:
                with self.subTest(args=args, cc_args=cc_args):
                    upstream.test_connected_components(self.spark, args, cc_args)

    def test_connected_components2(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            for cc_args in [(-1, True), (10000, True), (-1, False), (10000, False)]:
                with self.subTest(args=args, cc_args=cc_args):
                    upstream.test_connected_components2(self.spark, args, cc_args)

    def test_connected_components_cached_result(self) -> None:
        cache_manager = None
        if self._legacy_sc is not None:
            session = self._legacy_sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
            cache_manager = session.sharedState().cacheManager()
        ids = set()
        with self.sql_conf({"spark.connect.session.planCache.enabled": "false"}):
            for algorithm in ["two_phase", "randomized_contraction", "graphx"]:
                for id_type in [int, str]:
                    for local in [False, True]:
                        with self.subTest(algorithm=algorithm, id_type=id_type, local=local):
                            self._check_connected_components_result(
                                algorithm, id_type, local, ids, cache_manager
                            )

    def _check_connected_components_result(
        self, algorithm, id_type, local, ids, cache_manager
    ) -> None:
        vertices = self.spark.createDataFrame(
            [(id_type(i), f"vertex-{i}") for i in range(1, 7)], ["id", "name"]
        )
        edges = self.spark.createDataFrame(
            [(id_type(a), id_type(b)) for a, b in [(1, 2), (2, 3), (4, 5)]], ["src", "dst"]
        )
        graph = GraphFrame(vertices, edges)
        before = cache_manager.cachedData().size() if cache_manager is not None else None
        client = self.spark.client
        with patch.object(client, "execute_command", wraps=client.execute_command) as execute:
            result = graph.connectedComponents(
                algorithm=algorithm,
                checkpointInterval=2,
                use_local_checkpoints=local,
                useLabelsAsComponents=True,
            )
        command = execute.call_args.args[0]
        self.assertEqual(command.WhichOneof("command_type"), "graph_frames_command")
        df_id = command.graph_frames_command.df_id
        try:
            self.assertRegex(df_id, r"^_graphframes_cc_[0-9a-f]{32}$")
            self.assertEqual(uuid.UUID(hex=df_id.removeprefix("_graphframes_cc_")).version, 4)
            self.assertNotIn(df_id, ids)
            ids.add(df_id)
            root = result._plan.to_proto(client).root
            self.assertEqual(root.WhichOneof("rel_type"), "cached_remote_relation")
            self.assertEqual(root.cached_remote_relation.relation_id, df_id)
            self.assertEqual(result.storageLevel, StorageLevel.NONE)
            if cache_manager is not None:
                self.assertEqual(cache_manager.cachedData().size(), before)
            rows = result.orderBy("id").collect()
            groups = {}
            for row in rows:
                groups.setdefault(row.component, set()).add(row.id)
            self.assertEqual(
                {frozenset(group) for group in groups.values()},
                {
                    frozenset(id_type(i) for i in [1, 2, 3]),
                    frozenset(id_type(i) for i in [4, 5]),
                    frozenset([id_type(6)]),
                },
            )
            self.assertEqual([row.id for row in rows], [id_type(i) for i in range(1, 7)])
            if algorithm != "randomized_contraction":
                self.assertEqual([row.name for row in rows], [f"vertex-{i}" for i in range(1, 7)])
            self.assertEqual(result.orderBy("id").collect(), rows)
            self.assertEqual(result.cache().count(), 6)
            result.unpersist(blocking=True)
            self.assertEqual(result.orderBy("id").collect(), rows)
            # The client-generated ID makes retries reuse the exact same checkpointed DataFrame.
            cached = _cached_result(self, self.spark, df_id)
            if self._legacy_sc is not None:
                self.assertIsNotNone(cached)
            client.execute_command(command)
            if cached is not None:
                self.assertTrue(cached.equals(_cached_result(self, self.spark, df_id)))
            self.assertEqual(result.orderBy("id").collect(), rows)
        finally:
            _remove_cached_result(result)
        if self._legacy_sc is not None:
            self.assertIsNone(_cached_result(self, self.spark, df_id))
        if cache_manager is not None:
            self.assertEqual(cache_manager.cachedData().size(), before)

    def test_shortest_paths(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                upstream.test_shortest_paths(self.spark, args)

    def test_triangle_counts(self) -> None:
        for storage_level in upstream.STORAGE_LEVELS:
            with self.subTest(storage_level=storage_level):
                upstream.test_triangle_counts(self.spark, storage_level)

    def test_cycles_finding(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                upstream.test_cycles_finding(self.spark, args)

    def test_mis(self) -> None:
        for storage_level in upstream.STORAGE_LEVELS:
            with self.subTest(storage_level=storage_level):
                upstream.test_mis(self.spark, storage_level)

    def test_kcore(self) -> None:
        for args in upstream.PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                upstream.test_kcore(self.spark, args)


class GraphFramesLocalCheckpointConnectTests(ReusedConnectTestCase):
    def _graph(self, spark):
        vertices = spark.createDataFrame([("a",), ("b",), ("c",)], ["id"])
        edges = spark.createDataFrame([("a", "b")], ["src", "dst"])
        return GraphFrame(vertices, edges)

    def _persistent_rdd_ids(self):
        if self._legacy_sc is None:
            return None
        entries = self._legacy_sc._jsc.sc().getPersistentRDDs().keysIterator()
        ids = set()
        while entries.hasNext():
            ids.add(entries.next())
        return ids

    def test_local_checkpoint_without_directory(self) -> None:
        before = self._persistent_rdd_ids()
        with self.sql_conf({"spark.connect.session.planCache.enabled": "false"}):
            result = self._graph(self.spark).connectedComponents(
                algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=False
            )
            try:
                expected = result.orderBy("id").collect()
                self.assertEqual(len(expected), 3)
                self.assertIsInstance(result._plan, CachedRemoteRelation)
                self.assertEqual(result.storageLevel, StorageLevel.NONE)
                result.unpersist(blocking=True)
                self.assertEqual(result.orderBy("id").collect(), expected)
                if before is not None:
                    self.assertGreater(len(self._persistent_rdd_ids() - before), 0)
            finally:
                _remove_cached_result(result)

    def test_result_relation_is_session_local(self) -> None:
        other = ConnectSparkSession(connection=self.spark.client._builder)
        try:
            result = self._graph(other).connectedComponents(
                algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=True
            )
            try:
                self.assertEqual(result.count(), 3)
                unrelated = DataFrame(
                    CachedRemoteRelation(result._plan._relation_id, self.spark), self.spark
                )
                with self.assertRaisesRegex(Exception, "DATAFRAME_NOT_FOUND"):
                    unrelated.count()
            finally:
                _remove_cached_result(result)
        finally:
            other.client.release_session()
            other.client.close()

    def test_invalid_result_id_keeps_existing_result(self) -> None:
        client = self.spark.client
        with patch.object(client, "execute_command", wraps=client.execute_command) as execute:
            result = self._graph(self.spark).connectedComponents(
                algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=True
            )
        command = execute.call_args.args[0]
        try:
            expected = result.orderBy("id").collect()
            for invalid_id in ["", str(uuid.uuid4()), "_graphframes_cc_invalid"]:
                command.graph_frames_command.df_id = invalid_id
                with self.assertRaisesRegex(Exception, "UUID|result ID"):
                    client.execute_command(command)
            self.assertEqual(result.orderBy("id").collect(), expected)
        finally:
            _remove_cached_result(result)

    def test_result_relation_cleanup(self) -> None:
        if self._legacy_sc is None:
            self.skipTest("Requires access to the local Connect server")
        result = self._graph(self.spark).connectedComponents(
            algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=True
        )
        df_id = result._plan._relation_id
        self.assertIsNotNone(_cached_result(self, self.spark, df_id))
        derived = result.filter("id = 'a'")
        del result
        gc.collect()
        self.assertIsNotNone(_cached_result(self, self.spark, df_id))
        self.assertEqual(derived.count(), 1)
        del derived
        gc.collect()

        def removed():
            self.assertIsNone(_cached_result(self, self.spark, df_id))

        eventually(catch_assertions=True)(removed)()


def _spark_test(function):
    def run(self) -> None:
        function(self.spark)

    run.__name__ = function.__name__
    return run


def _local_graph_test(function):
    def run(self) -> None:
        function(self.local_graph())

    run.__name__ = function.__name__
    return run


for _test_function in [
    upstream.test_validate,
    upstream.test_as_undirected,
    upstream.test_as_reversed,
    upstream.test_power_iteration_clustering,
    upstream.test_graphframes_pagerank,
    upstream.test_pregel_required_edge_columns,
    upstream.test_connected_components_example,
    upstream.test_shortest_paths2,
    upstream.test_neighborhood_aware_cdlp_api_defaults,
    upstream.test_neighborhood_aware_cdlp_api_with_all_args,
    upstream.test_neighborhood_aware_cdlp_api_rejects_invalid_multiplier_combination,
    upstream.test_strongly_connected_components,
    upstream.test_approx_triangle_counts,
    upstream.test_aggregate_neighbors_basic,
    upstream.test_aggregate_neighbors_with_edge_filter,
    upstream.test_aggregate_neighbors_multiple_accumulators,
    upstream.test_hyper_anf_basic,
    upstream.test_hyper_anf_args_passed,
    upstream.test_hyper_anf_invalid_args,
]:
    setattr(GraphFramesUpstreamConnectTests, _test_function.__name__, _spark_test(_test_function))


for _test_function in [
    upstream.test_cache,
    upstream.test_degrees,
    upstream.test_type_degrees,
    upstream.test_type_degrees_with_explicit_types,
    upstream.test_motif_finding,
    upstream.test_filterVertices,
    upstream.test_filterEdges,
    upstream.test_dropIsolatedVertices,
    upstream.test_bfs,
    upstream.test_all_paths,
    upstream.test_random_walk_embeddings_api,
    upstream.test_random_walk_embeddings_invalid_args,
]:
    setattr(
        GraphFramesUpstreamConnectTests,
        _test_function.__name__,
        _local_graph_test(_test_function),
    )


if __name__ == "__main__":
    from pyspark.testing.unittestutils import main

    main()
