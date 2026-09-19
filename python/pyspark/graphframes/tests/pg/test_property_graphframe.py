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

import hashlib
import tempfile

from pyspark.graphframes import GraphFrame
from pyspark.graphframes.pg import EdgePropertyGroup, PropertyGraphFrame, VertexPropertyGroup
from pyspark.graphframes.tests._upstream_test_utils import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.testing.sqlutils import ReusedSQLTestCase


def sha256_hash(id_val, group_name):
    """Helper to compute SHA256 hash like Scala does."""
    hash_val = hashlib.sha256(str(id_val).encode("utf-8")).hexdigest()
    return f"{group_name}{hash_val}"


@pytest.fixture(scope="module")
def people_group(spark: SparkSession):
    people_data = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")],
        ["id", "name"],
    )
    return VertexPropertyGroup("people", people_data, "id")


@pytest.fixture(scope="module")
def movies_group(spark: SparkSession):
    movies_data = spark.createDataFrame(
        [(1, "Matrix"), (2, "Inception"), (3, "Interstellar")],
        ["id", "title"],
    )
    return VertexPropertyGroup("movies", movies_data, "id")


@pytest.fixture(scope="module")
def likes_group(
    spark: SparkSession, people_group: VertexPropertyGroup, movies_group: VertexPropertyGroup
):
    likes_data = spark.createDataFrame(
        [(1, 1), (1, 2), (2, 1), (3, 2), (4, 3), (5, 2)],
        ["src", "dst"],
    )
    likes_data_with_weight = likes_data.withColumn("weight", lit(1.0))
    return EdgePropertyGroup(
        "likes",
        likes_data_with_weight,
        people_group,
        movies_group,
        is_directed=False,
        src_column_name="src",
        dst_column_name="dst",
        weight_column_name="weight",
    )


@pytest.fixture(scope="module")
def messages_group(spark: SparkSession, people_group: VertexPropertyGroup):
    messages_data = spark.createDataFrame(
        [(1, 2, 5.0), (2, 3, 8.0), (3, 4, 3.0), (4, 5, 6.0), (5, 1, 9.0)],
        ["src", "dst", "weight"],
    )
    return EdgePropertyGroup(
        "messages",
        messages_data,
        people_group,
        people_group,
        is_directed=True,
        src_column_name="src",
        dst_column_name="dst",
        weight_column_name="weight",
    )


@pytest.fixture(scope="module")
def people_movies_graph(
    people_group: VertexPropertyGroup,
    movies_group: VertexPropertyGroup,
    likes_group: EdgePropertyGroup,
    messages_group: EdgePropertyGroup,
):
    return PropertyGraphFrame(
        [people_group, movies_group],
        [likes_group, messages_group],
    )


def test_property_graph_frame_constructor(people_movies_graph: PropertyGraphFrame) -> None:
    assert len(people_movies_graph.vertex_property_groups) == 2
    assert len(people_movies_graph.edges_property_groups) == 2


def test_vertex_property_group_creation(people_group: VertexPropertyGroup) -> None:
    assert people_group.name == "people"
    assert people_group.primary_key_column == "id"
    assert people_group.apply_mask_on_id


def test_edge_property_group_creation(
    likes_group: EdgePropertyGroup,
) -> None:
    assert likes_group.name == "likes"
    assert likes_group.src_property_group.name == "people"
    assert likes_group.dst_property_group.name == "movies"
    assert not likes_group.is_directed


def test_projection_by_movies(people_movies_graph: PropertyGraphFrame) -> None:
    projected_graph = people_movies_graph.projection_by("people", "movies", "likes")

    assert len(projected_graph.vertex_property_groups) == 1
    assert projected_graph.vertex_property_groups[0].name == "people"

    assert len(projected_graph.edges_property_groups) == 2
    assert any(group.name == "messages" for group in projected_graph.edges_property_groups)

    projected_edges_group = next(
        (
            group
            for group in projected_graph.edges_property_groups
            if group.name == "projected_likes"
        ),
        None,
    )
    assert projected_edges_group is not None
    assert projected_edges_group.src_column_name == GraphFrame.SRC
    assert projected_edges_group.dst_column_name == GraphFrame.DST
    assert projected_edges_group.weight_column_name == GraphFrame.WEIGHT
    assert not projected_edges_group.is_directed

    projected_edges = projected_edges_group.data.collect()
    edge_pairs = {(row.src, row.dst) for row in projected_edges}

    expected_edges = {
        (1, 2),  # Alice and Bob both like Matrix
        (1, 3),  # Alice and Charlie both like Inception
        (1, 5),  # Alice and Eve both like Inception
        (3, 5),  # Charlie and Eve both like Inception
    }
    assert edge_pairs == expected_edges


def test_projection_with_custom_weight(people_movies_graph: PropertyGraphFrame) -> None:
    projected_graph = people_movies_graph.projection_by(
        "people", "movies", "likes", new_edge_weight=lambda w1, w2: w1 + w2
    )

    projected_edges_group = next(
        (
            group
            for group in projected_graph.edges_property_groups
            if group.name == "projected_likes"
        ),
        None,
    )
    assert projected_edges_group is not None

    projected_edges = projected_edges_group.data.collect()
    edge_triples = {(row.src, row.dst, row.weight) for row in projected_edges}

    expected_edges = {
        (1, 2, 2.0),
        (1, 3, 2.0),
        (1, 5, 2.0),
        (3, 5, 2.0),
    }
    assert edge_triples == expected_edges


def test_to_graph_frame_messages_only(people_movies_graph: PropertyGraphFrame) -> None:
    graph = people_movies_graph.to_graphframe(
        vertex_property_groups=["people"],
        edge_property_groups=["messages"],
        edge_group_filters={"messages": lit(True)},
        vertex_group_filters={"people": lit(True)},
    )

    vertices = {row.id for row in graph.vertices.collect()}
    edges = {(row.src, row.dst, row.weight) for row in graph.edges.collect()}

    expected_vertices = {sha256_hash(i, "people") for i in range(1, 6)}
    assert vertices == expected_vertices

    expected_edges = {
        (sha256_hash(1, "people"), sha256_hash(2, "people"), 5.0),
        (sha256_hash(2, "people"), sha256_hash(3, "people"), 8.0),
        (sha256_hash(3, "people"), sha256_hash(4, "people"), 3.0),
        (sha256_hash(4, "people"), sha256_hash(5, "people"), 6.0),
        (sha256_hash(5, "people"), sha256_hash(1, "people"), 9.0),
    }
    assert edges == expected_edges


def test_to_graph_frame_all_groups(people_movies_graph: PropertyGraphFrame) -> None:
    graph = people_movies_graph.to_graphframe(
        vertex_property_groups=["people", "movies"],
        edge_property_groups=["messages", "likes"],
        edge_group_filters={"messages": lit(True), "likes": lit(True)},
        vertex_group_filters={"people": lit(True), "movies": lit(True)},
    )

    vertices = graph.vertices.collect()
    edges = graph.edges.collect()

    assert len(vertices) == 8  # 5 people + 3 movies

    vertex_ids = {row.id for row in vertices}
    assert sha256_hash(1, "movies") in vertex_ids
    assert sha256_hash(1, "people") in vertex_ids

    message_edges = [e for e in edges if e.weight != 1.0]
    like_edges = [e for e in edges if e.weight == 1.0]

    assert len(message_edges) == 5  # Directed messages
    assert len(like_edges) == 12  # 6 undirected edges * 2


def test_to_graph_frame_unmasked_ids(
    spark: SparkSession,
    people_group: VertexPropertyGroup,
    likes_group: EdgePropertyGroup,
    messages_group: EdgePropertyGroup,
) -> None:
    movies_data = spark.createDataFrame(
        [(1, "Matrix"), (2, "Inception"), (3, "Interstellar")],
        ["id", "title"],
    )
    unmasked_movies_group = VertexPropertyGroup("movies", movies_data, "id", apply_mask_on_id=False)

    new_likes_group = EdgePropertyGroup(
        "likes",
        likes_group.data,
        likes_group.src_property_group,
        unmasked_movies_group,
        likes_group.is_directed,
        likes_group.src_column_name,
        likes_group.dst_column_name,
        likes_group.weight_column_name,
    )

    modified_graph = PropertyGraphFrame(
        [people_group, unmasked_movies_group],
        [new_likes_group, messages_group],
    )

    graph = modified_graph.to_graphframe(
        vertex_property_groups=["people", "movies"],
        edge_property_groups=["messages", "likes"],
        edge_group_filters={"messages": lit(True), "likes": lit(True)},
        vertex_group_filters={"people": lit(True), "movies": lit(True)},
    )

    vertices = {row.id for row in graph.vertices.collect()}
    edges = graph.edges.collect()

    assert "1" in vertices
    assert "2" in vertices
    assert "3" in vertices
    assert sha256_hash(1, "people") in vertices

    likes_edges = [e for e in edges if e.weight == 1.0]
    assert any(e.src == sha256_hash(1, "people") and e.dst == "1" for e in likes_edges)
    assert any(e.src == "1" and e.dst == sha256_hash(1, "people") for e in likes_edges)


def test_join_vertices_with_connected_components(
    people_movies_graph: PropertyGraphFrame,
) -> None:
    graph = people_movies_graph.to_graphframe(
        vertex_property_groups=["people", "movies"],
        edge_property_groups=["messages", "likes"],
        edge_group_filters={"messages": lit(True), "likes": lit(True)},
        vertex_group_filters={"people": lit(True), "movies": lit(True)},
    )

    components = graph.connectedComponents()

    joined_back = people_movies_graph.join_vertices(components, vertex_groups=["people", "movies"])

    joined_data = joined_back.collect()

    by_group = {}
    for row in joined_data:
        group = row.property_group
        if group not in by_group:
            by_group[group] = []
        by_group[group].append(row)

    assert "movies" in by_group
    assert "people" in by_group
    assert len(by_group["movies"]) == 3
    assert len(by_group["people"]) == 5


def test_vertex_property_group_validation(people_group: VertexPropertyGroup) -> None:
    from pyspark.graphframes.pg.property_groups import InvalidPropertyGroupException

    with pytest.raises(InvalidPropertyGroupException):
        VertexPropertyGroup("test", people_group.data, "nonexistent_column")


def test_edge_property_group_validation(
    people_group: VertexPropertyGroup,
    movies_group: VertexPropertyGroup,
    likes_group: EdgePropertyGroup,
) -> None:
    from pyspark.graphframes.pg.property_groups import InvalidPropertyGroupException

    with pytest.raises(InvalidPropertyGroupException):
        EdgePropertyGroup(
            "test",
            likes_group.data,
            people_group,
            movies_group,
            is_directed=True,
            src_column_name="nonexistent",
            dst_column_name="dst",
            weight_column_name="weight",
        )

    with pytest.raises(InvalidPropertyGroupException):
        EdgePropertyGroup(
            "test",
            likes_group.data,
            people_group,
            movies_group,
            is_directed=True,
            src_column_name="src",
            dst_column_name="nonexistent",
            weight_column_name="weight",
        )

    with pytest.raises(InvalidPropertyGroupException):
        EdgePropertyGroup(
            "test",
            likes_group.data,
            people_group,
            movies_group,
            is_directed=True,
            src_column_name="src",
            dst_column_name="dst",
            weight_column_name="nonexistent",
        )


def test_to_graph_frame_invalid_group(people_movies_graph: PropertyGraphFrame) -> None:
    with pytest.raises(ValueError):
        people_movies_graph.to_graphframe(
            vertex_property_groups=["nonexistent"],
            edge_property_groups=["likes"],
        )

    with pytest.raises(ValueError):
        people_movies_graph.to_graphframe(
            vertex_property_groups=["people"],
            edge_property_groups=["nonexistent"],
        )


def test_projection_by_invalid_group(people_movies_graph: PropertyGraphFrame) -> None:
    with pytest.raises(ValueError):
        people_movies_graph.projection_by("nonexistent", "movies", "likes")

    with pytest.raises(ValueError):
        people_movies_graph.projection_by("people", "nonexistent", "likes")

    with pytest.raises(ValueError):
        people_movies_graph.projection_by("people", "movies", "nonexistent")


def test_property_graph_frame_to_graph_frame_conversion(
    people_movies_graph: PropertyGraphFrame,
) -> None:
    graph = people_movies_graph.to_graphframe(
        vertex_property_groups=["people"],
        edge_property_groups=["messages"],
    )

    assert isinstance(graph, GraphFrame)
    assert GraphFrame.ID in graph.vertices.columns
    assert GraphFrame.SRC in graph.edges.columns
    assert GraphFrame.DST in graph.edges.columns
    assert GraphFrame.WEIGHT in graph.edges.columns


class PropertyGraphFrameTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._checkpoint_dir = tempfile.TemporaryDirectory()
        cls.spark.sparkContext.setCheckpointDir(cls._checkpoint_dir.name)
        cls.spark.conf.set("spark.sql.shuffle.partitions", "4")

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls._checkpoint_dir.cleanup()

    def groups(self):
        people = people_group(self.spark)
        movies = movies_group(self.spark)
        likes = likes_group(self.spark, people, movies)
        messages = messages_group(self.spark, people)
        graph = people_movies_graph(people, movies, likes, messages)
        return people, movies, likes, messages, graph

    def test_property_graph_frame_constructor(self) -> None:
        *_, graph = self.groups()
        test_property_graph_frame_constructor(graph)

    def test_vertex_property_group_creation(self) -> None:
        people, *_ = self.groups()
        test_vertex_property_group_creation(people)

    def test_edge_property_group_creation(self) -> None:
        _, _, likes, _, _ = self.groups()
        test_edge_property_group_creation(likes)

    def test_projection_by_movies(self) -> None:
        *_, graph = self.groups()
        test_projection_by_movies(graph)

    def test_projection_with_custom_weight(self) -> None:
        *_, graph = self.groups()
        test_projection_with_custom_weight(graph)

    def test_to_graph_frame_messages_only(self) -> None:
        *_, graph = self.groups()
        test_to_graph_frame_messages_only(graph)

    def test_to_graph_frame_all_groups(self) -> None:
        *_, graph = self.groups()
        test_to_graph_frame_all_groups(graph)

    def test_to_graph_frame_unmasked_ids(self) -> None:
        people, _, likes, messages, _ = self.groups()
        test_to_graph_frame_unmasked_ids(self.spark, people, likes, messages)

    def test_join_vertices_with_connected_components(self) -> None:
        *_, graph = self.groups()
        test_join_vertices_with_connected_components(graph)

    def test_vertex_property_group_validation(self) -> None:
        people, *_ = self.groups()
        test_vertex_property_group_validation(people)

    def test_edge_property_group_validation(self) -> None:
        people, movies, likes, _, _ = self.groups()
        test_edge_property_group_validation(people, movies, likes)

    def test_to_graph_frame_invalid_group(self) -> None:
        *_, graph = self.groups()
        test_to_graph_frame_invalid_group(graph)

    def test_projection_by_invalid_group(self) -> None:
        *_, graph = self.groups()
        test_projection_by_invalid_group(graph)

    def test_property_graph_frame_to_graph_frame_conversion(self) -> None:
        *_, graph = self.groups()
        test_property_graph_frame_to_graph_frame_conversion(graph)


if __name__ == "__main__":
    from pyspark.testing.unittestutils import main

    main()
