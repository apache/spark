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

import os
import shutil
import tempfile
from unittest.mock import patch

from pyspark import SparkConf
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.tests import test_graphframes as upstream
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
from pyspark.testing.connectutils import ReusedConnectTestCase


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

    def test_connected_components_view_result(self) -> None:
        cache_manager = None
        if self._legacy_sc is not None:
            session = self._legacy_sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
            cache_manager = session.sharedState().cacheManager()
        names = set()
        with self.sql_conf({"spark.connect.session.planCache.enabled": "false"}):
            for algorithm in ["two_phase", "randomized_contraction", "graphx"]:
                for id_type in [int, str]:
                    for local in [False, True]:
                        with self.subTest(algorithm=algorithm, id_type=id_type, local=local):
                            self._check_connected_components_view(
                                algorithm, id_type, local, names, cache_manager
                            )

    def _check_connected_components_view(
        self, algorithm, id_type, local, names, cache_manager
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
        name = command.graph_frames_command.view_name
        try:
            self.assertRegex(name, r"^_graphframes_cc_[0-9a-f]{32}$")
            self.assertNotIn(name, names)
            names.add(name)
            root = result._plan.to_proto(client).root
            self.assertEqual(root.WhichOneof("rel_type"), "read")
            self.assertEqual(root.read.named_table.unparsed_identifier, name)
            self.assertFalse(self.spark.catalog.isCached(name))
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
            result.unpersist(blocking=True)
            self.assertFalse(self.spark.catalog.isCached(name))
            self.assertEqual(result.orderBy("id").collect(), rows)
            # Retrying the command retains the same checkpoint, even without SQL caching.
            client.execute_command(command)
            self.assertEqual(result.orderBy("id").collect(), rows)
            if cache_manager is not None:
                self.assertEqual(cache_manager.cachedData().size(), before)
            self.assertEqual(result.cache().count(), 6)
        finally:
            self.assertTrue(self.spark.catalog.dropTempView(name))
        self.assertFalse(self.spark.catalog.tableExists(name))
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
            name = result._plan.to_proto(
                self.spark.client
            ).root.read.named_table.unparsed_identifier
            try:
                expected = result.orderBy("id").collect()
                self.assertEqual(len(expected), 3)
                result.unpersist(blocking=True)
                self.assertEqual(result.orderBy("id").collect(), expected)
                self.assertEqual(self.spark.table(name).count(), 3)
                self.assertFalse(self.spark.catalog.isCached(name))
                if before is not None:
                    self.assertGreater(len(self._persistent_rdd_ids() - before), 0)
            finally:
                self.spark.catalog.dropTempView(name)

    def test_result_view_is_session_local(self) -> None:
        other = ConnectSparkSession(connection=self.spark.client._builder)
        try:
            result = self._graph(other).connectedComponents(
                algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=True
            )
            name = result._plan.to_proto(other.client).root.read.named_table.unparsed_identifier
            self.assertEqual(result.count(), 3)
            self.assertTrue(other.catalog.tableExists(name))
            self.assertFalse(self.spark.catalog.tableExists(name))
            other.catalog.dropTempView(name)
        finally:
            other.client.release_session()
            other.client.close()

    def test_conflicting_retry_keeps_result(self) -> None:
        client = self.spark.client
        with patch.object(client, "execute_command", wraps=client.execute_command) as execute:
            result = self._graph(self.spark).connectedComponents(
                algorithm="two_phase", checkpointInterval=0, use_local_checkpoints=True
            )
        command = execute.call_args.args[0]
        name = command.graph_frames_command.view_name
        try:
            expected = result.orderBy("id").collect()
            command.graph_frames_command.api.connected_components.max_iter = 5
            with self.assertRaisesRegex(Exception, "belongs to another request"):
                client.execute_command(command)
            self.assertEqual(result.orderBy("id").collect(), expected)
        finally:
            self.spark.catalog.dropTempView(name)


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
