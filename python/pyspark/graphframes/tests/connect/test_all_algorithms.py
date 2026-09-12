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

from pyspark.graphframes import GraphFrame
from pyspark.graphframes.graphframe import AggregateNeighbors, RandomWalkEmbeddings
from pyspark.graphframes.lib import AggregateMessages, Pregel
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.testing.connectutils import ReusedConnectTestCase


class GraphFrameConnectAlgorithmTests(ReusedConnectTestCase):
    def setUp(self) -> None:
        super().setUp()
        vertices = self.spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "c", 30), (4, "isolated", 40)],
            ["id", "name", "age"],
        )
        edges = self.spark.createDataFrame(
            [
                (1, 2, "friend", 1.0),
                (2, 3, "follow", 2.0),
                (3, 1, "friend", 3.0),
                (2, 1, "friend", 4.0),
            ],
            ["src", "dst", "relationship", "weight"],
        )
        self.graph = GraphFrame(vertices, edges)

    def test_traversal_algorithms(self) -> None:
        self.assertEqual(self.graph.find("(a)-[e]->(b)").count(), 4)
        self.assertEqual(self.graph.bfs("id = 1", "id = 3").count(), 1)

        paths = self.graph.all_paths(
            "id = 1",
            "id = 3",
            max_path_length=3,
            use_local_checkpoints=True,
        )
        self.assertEqual(paths.count(), 1)
        self.assertEqual(paths.first()["len"], 2)

        cycles = self.graph.detectingCycles(use_local_checkpoints=True)
        self.assertGreater(cycles.count(), 0)

    def test_message_aggregation_and_pregel(self) -> None:
        messages = self.graph.aggregateMessages(
            [F.sum(AggregateMessages.msg).alias("ageSum")],
            sendToDst=[AggregateMessages.src["age"]],
        )
        self.assertEqual(messages.count(), 3)
        messages.unpersist()

        pregel = self.graph.pregel
        result = (
            pregel.setMaxIter(2)
            .setUseLocalCheckpoints(True)
            .withVertexColumn(
                "value",
                F.lit(0),
                F.coalesce(pregel.msg(), F.lit(0)),
            )
            .sendMsgToDst(F.lit(1))
            .aggMsgs(F.sum(pregel.msg()))
            .run()
        )
        self.assertEqual(result.count(), 4)
        self.assertIn("value", result.columns)
        result.unpersist()

    def test_direct_pregel_construction(self) -> None:
        pregel = Pregel(self.graph)
        self.assertIs(pregel.setMaxIter(1), pregel)
        self.assertIsInstance(pregel, Pregel)

    def test_component_and_community_algorithms(self) -> None:
        components = self.graph.connectedComponents(
            algorithm="two_phase",
            use_local_checkpoints=True,
            max_iter=10,
        )
        self.assertEqual(components.count(), 4)
        self.assertIn("component", components.columns)
        components.unpersist()

        labels = self.graph.labelPropagation(
            maxIter=2,
            algorithm="graphframes",
            use_local_checkpoints=True,
        )
        self.assertEqual(labels.count(), 4)
        self.assertIn("label", labels.columns)
        labels.unpersist()

        structure_labels = self.graph.neighborhood_aware_cdlp(
            max_iter=2,
            use_local_checkpoints=True,
        )
        self.assertEqual(structure_labels.count(), 4)
        self.assertIn("label", structure_labels.columns)
        structure_labels.unpersist()

    def test_ranking_and_path_algorithms(self) -> None:
        ranked = self.graph.pageRank(maxIter=2)
        self.assertEqual(ranked.vertices.count(), 4)
        self.assertEqual(ranked.edges.count(), 4)
        self.assertIn("pagerank", ranked.vertices.columns)
        self.assertIn("weight", ranked.edges.columns)

        personalized = self.graph.parallelPersonalizedPageRank(sourceIds=[1, 2], maxIter=2)
        self.assertEqual(personalized.vertices.count(), 4)
        self.assertIn("pageranks", personalized.vertices.columns)

        shortest = self.graph.shortestPaths(
            landmarks=[1, 3],
            algorithm="graphframes",
            use_local_checkpoints=True,
        )
        self.assertEqual(shortest.count(), 4)
        self.assertIn("distances", shortest.columns)
        shortest.unpersist()

        strongly_connected = self.graph.stronglyConnectedComponents(maxIter=5)
        self.assertEqual(strongly_connected.count(), 4)
        self.assertIn("component", strongly_connected.columns)

    def test_structural_algorithms(self) -> None:
        triangles = self.graph.triangleCount(StorageLevel.MEMORY_AND_DISK_DESER)
        self.assertEqual(triangles.count(), 4)
        self.assertIn("count", triangles.columns)
        triangles.unpersist()

        independent_set = self.graph.maximal_independent_set(
            use_local_checkpoints=True,
            seed=7,
        )
        self.assertGreater(independent_set.count(), 0)
        self.assertEqual(independent_set.columns, ["id"])
        independent_set.unpersist()

        cores = self.graph.k_core(use_local_checkpoints=True)
        self.assertEqual(cores.count(), 4)
        self.assertIn("kcore", cores.columns)
        cores.unpersist()

        neighborhood = self.graph.hyper_anf(n_hops=2, use_local_checkpoints=True)
        self.assertEqual(neighborhood.count(), 3)
        self.assertEqual(
            neighborhood.columns,
            ["id", "hop_0", "hop_1", "hop_2"],
        )
        neighborhood.unpersist()

    def test_power_iteration_clustering(self) -> None:
        clusters = self.graph.powerIterationClustering(
            k=2,
            maxIter=5,
            weightCol="weight",
        )
        self.assertEqual(clusters.count(), 3)
        self.assertIn("cluster", clusters.columns)

    def test_svd_plus_plus(self) -> None:
        vertices = self.spark.createDataFrame([(1,), (2,), (3,), (4,)], ["id"])
        ratings = self.spark.createDataFrame(
            [(1, 3, 4.0), (1, 4, 3.0), (2, 3, 2.0), (2, 4, 5.0)],
            ["src", "dst", "weight"],
        )
        model, loss = GraphFrame(vertices, ratings).svdPlusPlus(rank=2, maxIter=1)
        self.assertEqual(model.count(), 4)
        self.assertGreaterEqual(loss, 0.0)

    def test_aggregate_neighbors(self) -> None:
        result = self.graph.aggregate_neighbors(
            starting_vertices=F.col("id") == 1,
            accumulator_names=["path_length"],
            accumulator_inits=[F.lit(0)],
            accumulator_updates=[F.col("path_length") + 1],
            max_hops=3,
            target_condition=AggregateNeighbors.dst_attr("id") == 3,
            required_vertex_attributes=["id"],
            use_local_checkpoints=True,
        )
        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path_length"], 2)
        result.unpersist()

    def test_random_walk_embeddings(self) -> None:
        embeddings = RandomWalkEmbeddings(self.graph)
        embeddings.set_rw_model(
            "/tmp/spark-graphframes-connect-rw-test",
            num_walks_per_node=1,
            num_batches=1,
            walks_per_batch=1,
        )
        embeddings.set_hash2vec(
            context_size=2,
            num_partitions=1,
            embeddings_dim=8,
        )
        embeddings.unset_neighbors_aggregation()
        embeddings.set_clean_up_after_run()
        result = embeddings.run()
        self.assertEqual(result.count(), 3)
        self.assertIn("embedding", result.columns)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
