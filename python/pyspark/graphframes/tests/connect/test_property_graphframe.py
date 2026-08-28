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
from pyspark.graphframes.tests.pg import test_property_graphframe as upstream
from pyspark.testing.connectutils import ReusedConnectTestCase


class PropertyGraphFrameConnectTests(ReusedConnectTestCase):
    _checkpoint_dir = tempfile.mkdtemp(prefix="spark-property-graphframe-connect-")

    @classmethod
    def conf(cls) -> SparkConf:
        return (
            super()
            .conf()
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

    def groups(self):
        people = upstream.people_group(self.spark)
        movies = upstream.movies_group(self.spark)
        likes = upstream.likes_group(self.spark, people, movies)
        messages = upstream.messages_group(self.spark, people)
        graph = upstream.people_movies_graph(people, movies, likes, messages)
        return people, movies, likes, messages, graph

    def test_property_graph_frame_constructor(self) -> None:
        *_, graph = self.groups()
        upstream.test_property_graph_frame_constructor(graph)

    def test_vertex_property_group_creation(self) -> None:
        people, *_ = self.groups()
        upstream.test_vertex_property_group_creation(people)

    def test_edge_property_group_creation(self) -> None:
        _, _, likes, _, _ = self.groups()
        upstream.test_edge_property_group_creation(likes)

    def test_projection_by_movies(self) -> None:
        *_, graph = self.groups()
        upstream.test_projection_by_movies(graph)

    def test_projection_with_custom_weight(self) -> None:
        *_, graph = self.groups()
        upstream.test_projection_with_custom_weight(graph)

    def test_to_graph_frame_messages_only(self) -> None:
        *_, graph = self.groups()
        upstream.test_to_graph_frame_messages_only(graph)

    def test_to_graph_frame_all_groups(self) -> None:
        *_, graph = self.groups()
        upstream.test_to_graph_frame_all_groups(graph)

    def test_to_graph_frame_unmasked_ids(self) -> None:
        people, _, likes, messages, _ = self.groups()
        upstream.test_to_graph_frame_unmasked_ids(self.spark, people, likes, messages)

    def test_join_vertices_with_connected_components(self) -> None:
        *_, graph = self.groups()
        upstream.test_join_vertices_with_connected_components(graph)

    def test_vertex_property_group_validation(self) -> None:
        people, *_ = self.groups()
        upstream.test_vertex_property_group_validation(people)

    def test_edge_property_group_validation(self) -> None:
        people, movies, likes, _, _ = self.groups()
        upstream.test_edge_property_group_validation(people, movies, likes)

    def test_to_graph_frame_invalid_group(self) -> None:
        *_, graph = self.groups()
        upstream.test_to_graph_frame_invalid_group(graph)

    def test_projection_by_invalid_group(self) -> None:
        *_, graph = self.groups()
        upstream.test_projection_by_invalid_group(graph)

    def test_property_graph_frame_to_graph_frame_conversion(self) -> None:
        *_, graph = self.groups()
        upstream.test_property_graph_frame_to_graph_frame_conversion(graph)


if __name__ == "__main__":
    from pyspark.testing.unittestutils import main

    main()
