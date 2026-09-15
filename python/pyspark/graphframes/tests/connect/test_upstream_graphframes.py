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

from pyspark import SparkConf
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.tests import test_graphframes as upstream
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
