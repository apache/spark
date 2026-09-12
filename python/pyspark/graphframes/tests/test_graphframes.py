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


import tempfile
from dataclasses import dataclass

from pyspark import SparkConf
from pyspark.graphframes.graphframe import AggregateNeighbors, GraphFrame, RandomWalkEmbeddings
from pyspark.graphframes.tests._upstream_test_utils import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sqlfunctions
from pyspark.sql.utils import is_remote
from pyspark.storagelevel import StorageLevel
from pyspark.testing.sqlutils import ReusedSQLTestCase


@dataclass
class PregelArguments:
    algorithm: str
    use_local_checkpoints: bool
    checkpoint_interval: int
    storage_level: StorageLevel


PREGEL_ARGUMENTS = [
    PregelArguments("graphframes", True, 5, StorageLevel.MEMORY_AND_DISK),
    PregelArguments("graphx", False, 3, StorageLevel.DISK_ONLY),
    PregelArguments("graphframes", False, 7, StorageLevel.MEMORY_ONLY),
    PregelArguments("graphframes", True, 1, StorageLevel.DISK_ONLY_3),
]
PREGEL_IDS: list[str] = [
    "graphframes,local,5,MEMORY_AND_DISK",
    "graphx,global,3,DISK_ONLY",
    "graphframes,global,7,MEMORY_ONLY",
    "graphframes,local,1,DISK_ONLY_3",
]
STORAGE_LEVELS = [
    StorageLevel.MEMORY_AND_DISK_2,
    StorageLevel.DISK_ONLY,
    StorageLevel.MEMORY_ONLY,
]
STORAGE_LEVELS_IDS = [
    "MEMORY_AND_DISK_2",
    "DISK_ONLY",
    "MEMORY_ONLY",
]


def test_construction(spark: SparkSession, local_g: GraphFrame) -> None:
    vertexIDs = [row[0] for row in local_g.vertices.select("id").collect()]
    assert sorted(vertexIDs) == [1, 2, 3]

    edgeActions = [row[0] for row in local_g.edges.select("action").collect()]
    assert sorted(edgeActions) == ["follow", "hate", "love"]
    tripletsFirst = list(
        map(
            lambda x: (x[0][1], x[1][1], x[2][2]),
            local_g.triplets.sort("src.id").select("src", "dst", "edge").take(1),
        )
    )
    assert tripletsFirst == [("A", "B", "love")], tripletsFirst

    # Try with invalid vertices and edges DataFrames
    v_invalid = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")], ["invalid_colname_1", "invalid_colname_2"]
    )
    e_invalid = spark.createDataFrame(
        [(1, 2), (2, 3), (3, 1)], ["invalid_colname_3", "invalid_colname_4"]
    )
    with pytest.raises(ValueError):
        _ = GraphFrame(v_invalid, e_invalid)


def test_validate(spark: SparkSession) -> None:
    good_g = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "attr"),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3)]).toDF("src", "dst"),
    )
    good_g.validate()  # no exception should be thrown

    not_distinct_vertices = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (1, "d")]).toDF("id", "attr"),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3)]).toDF("src", "dst"),
    )
    with pytest.raises(ValueError):
        not_distinct_vertices.validate()

    missing_vertices = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "attr"),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3), (1, 4)]).toDF("src", "dst"),
    )
    with pytest.raises(ValueError):
        missing_vertices.validate()


def test_as_undirected(spark: SparkSession) -> None:
    # Test without edge attributes
    v = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "name")
    e = spark.createDataFrame([(1, 2), (2, 3)]).toDF("src", "dst")
    g = GraphFrame(v, e)
    undirected = g.as_undirected()

    # Check edge count doubled
    assert undirected.edges.count() == 2 * g.edges.count()

    # Verify reverse edges exist
    edges = undirected.edges.sort("src", "dst").collect()
    assert len(edges) == 4
    assert edges[0][0] == 1
    assert edges[0][1] == 2
    assert edges[1][0] == 2
    assert edges[1][1] == 1
    assert edges[2][0] == 2
    assert edges[2][1] == 3
    assert edges[3][0] == 3
    assert edges[3][1] == 2

    # Test with edge attributes
    v2 = spark.createDataFrame([(1, "a"), (2, "b")]).toDF("id", "name")
    e2 = spark.createDataFrame([(1, 2, "edge1")]).toDF("src", "dst", "attr")
    g2 = GraphFrame(v2, e2)
    undirected2 = g2.as_undirected()

    edges2 = undirected2.edges.collect()
    assert len(edges2) == 2
    assert any(row[0] == 1 and row[1] == 2 and row[2] == "edge1" for row in edges2)
    assert any(row[0] == 2 and row[1] == 1 and row[2] == "edge1" for row in edges2)


def test_as_reversed(spark: SparkSession) -> None:
    # Test without edge attributes
    v = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "name")
    e = spark.createDataFrame([(1, 2), (2, 3)]).toDF("src", "dst")
    g = GraphFrame(v, e)
    reversed_g = g.as_reversed()

    # Check edge count is the same
    assert reversed_g.edges.count() == g.edges.count()

    # Verify edges are reversed
    edges = reversed_g.edges.sort("src", "dst").collect()
    assert len(edges) == 2
    assert edges[0][0] == 2
    assert edges[0][1] == 1
    assert edges[1][0] == 3
    assert edges[1][1] == 2

    # Test with edge attributes
    v2 = spark.createDataFrame([(1, "a"), (2, "b")]).toDF("id", "name")
    e2 = spark.createDataFrame([(1, 2, "edge1")]).toDF("src", "dst", "attr")
    g2 = GraphFrame(v2, e2)
    reversed2 = g2.as_reversed()

    edges2 = reversed2.edges.collect()
    assert len(edges2) == 1
    assert edges2[0][0] == 2
    assert edges2[0][1] == 1
    assert edges2[0][2] == "edge1"


def test_cache(local_g: GraphFrame) -> None:
    _ = local_g.cache()
    _ = local_g.unpersist()


def test_degrees(local_g: GraphFrame) -> None:
    outDeg = local_g.outDegrees
    assert set(outDeg.columns) == {"id", "outDegree"}
    inDeg = local_g.inDegrees
    assert set(inDeg.columns) == {"id", "inDegree"}
    deg = local_g.degrees
    assert set(deg.columns) == {"id", "degree"}


def test_type_degrees(local_g: GraphFrame) -> None:
    type_out_degree = local_g.type_out_degree("action")
    assert set(type_out_degree.columns) == {"id", "outDegrees"}

    schema = type_out_degree.schema["outDegrees"].dataType
    field_names = {field.name for field in schema.fields}
    assert field_names == {"love", "hate", "follow"}

    results = {row.id: row.outDegrees for row in type_out_degree.collect()}
    assert results[1].love == 1
    assert results[1].hate == 0
    assert results[1].follow == 0
    assert results[2].love == 0
    assert results[2].hate == 1
    assert results[2].follow == 1

    type_in_degree = local_g.type_in_degree("action")
    assert set(type_in_degree.columns) == {"id", "inDegrees"}

    schema = type_in_degree.schema["inDegrees"].dataType
    field_names = {field.name for field in schema.fields}
    assert field_names == {"love", "hate", "follow"}

    results = {row.id: row.inDegrees for row in type_in_degree.collect()}
    assert results[1].love == 0
    assert results[1].hate == 1
    assert results[1].follow == 0
    assert results[2].love == 1
    assert results[2].hate == 0
    assert results[2].follow == 0
    assert results[3].love == 0
    assert results[3].hate == 0
    assert results[3].follow == 1

    type_degree = local_g.type_degree("action")
    assert set(type_degree.columns) == {"id", "degrees"}

    schema = type_degree.schema["degrees"].dataType
    field_names = {field.name for field in schema.fields}
    assert field_names == {"love", "hate", "follow"}

    results = {row.id: row.degrees for row in type_degree.collect()}
    assert results[1].love == 1
    assert results[1].hate == 1
    assert results[1].follow == 0
    assert results[2].love == 1
    assert results[2].hate == 1
    assert results[2].follow == 1
    assert results[3].love == 0
    assert results[3].hate == 0
    assert results[3].follow == 1


def test_type_degrees_with_explicit_types(local_g: GraphFrame) -> None:
    edge_types = ["love", "hate", "follow"]
    type_out_degree = local_g.type_out_degree("action", edge_types)
    assert set(type_out_degree.columns) == {"id", "outDegrees"}

    schema = type_out_degree.schema["outDegrees"].dataType
    field_names = {field.name for field in schema.fields}
    assert field_names == {"love", "hate", "follow"}

    results = {row.id: row.outDegrees for row in type_out_degree.collect()}
    assert results[1].love == 1
    assert results[1].hate == 0
    assert results[1].follow == 0
    assert results[2].love == 0
    assert results[2].hate == 1
    assert results[2].follow == 1

    type_in_degree = local_g.type_in_degree("action", edge_types)
    assert set(type_in_degree.columns) == {"id", "inDegrees"}

    results = {row.id: row.inDegrees for row in type_in_degree.collect()}
    assert results[1].love == 0
    assert results[1].hate == 1
    assert results[1].follow == 0
    assert results[2].love == 1
    assert results[2].hate == 0
    assert results[2].follow == 0
    assert results[3].love == 0
    assert results[3].hate == 0
    assert results[3].follow == 1

    type_degree = local_g.type_degree("action", edge_types)
    assert set(type_degree.columns) == {"id", "degrees"}

    results = {row.id: row.degrees for row in type_degree.collect()}
    assert results[1].love == 1
    assert results[1].hate == 1
    assert results[1].follow == 0
    assert results[2].love == 1
    assert results[2].hate == 1
    assert results[2].follow == 1
    assert results[3].love == 0
    assert results[3].hate == 0
    assert results[3].follow == 1


def test_motif_finding(local_g: GraphFrame) -> None:
    motifs = local_g.find("(a)-[e]->(b)")
    assert motifs.count() == 3
    assert set(motifs.columns) == {"a", "e", "b"}


def test_filterVertices(local_g: GraphFrame) -> None:
    conditions = ["id < 3", local_g.vertices.id < 3]
    expected_v = [(1, "A"), (2, "B")]
    expected_e = [(1, 2, "love"), (2, 1, "hate")]
    for cond in conditions:
        g2 = local_g.filterVertices(cond)
        v2 = g2.vertices.select("id", "name").collect()
        e2 = g2.edges.select("src", "dst", "action").collect()
        assert len(v2) == len(expected_v)
        assert len(e2) == len(expected_e)
        assert set(v2) == set(expected_v)
        assert set(e2) == set(expected_e)


def test_filterEdges(local_g: GraphFrame) -> None:
    conditions = ["dst > 2", local_g.edges.dst > 2]
    expected_v = [(1, "A"), (2, "B"), (3, "C")]
    expected_e = [(2, 3, "follow")]
    for cond in conditions:
        g2 = local_g.filterEdges(cond)
        v2 = g2.vertices.select("id", "name").collect()
        e2 = g2.edges.select("src", "dst", "action").collect()
        assert len(v2) == len(expected_v)
        assert len(e2) == len(expected_e)
        assert set(v2) == set(expected_v)
        assert set(e2) == set(expected_e)


def test_dropIsolatedVertices(local_g: GraphFrame) -> None:
    g2 = local_g.filterEdges("dst > 2").dropIsolatedVertices()
    v2 = g2.vertices.select("id", "name").collect()
    e2 = g2.edges.select("src", "dst", "action").collect()
    expected_v = [(2, "B"), (3, "C")]
    expected_e = [(2, 3, "follow")]
    assert len(v2) == len(expected_v)
    assert len(e2) == len(expected_e)
    assert set(v2) == set(expected_v)
    assert set(e2) == set(expected_e)


def test_bfs(local_g: GraphFrame) -> None:
    paths = local_g.bfs("name='A'", "name='C'")
    assert paths is not None
    assert paths.count() == 1
    # Expecting that the first intermediary vertex in the BFS is "B"
    head = paths.select("v1.name").head()
    assert head is not None
    assert head[0] == "B"

    paths2 = local_g.bfs("name='A'", "name='C'", edgeFilter="action!='follow'")
    assert paths2.count() == 0

    paths3 = local_g.bfs("name='A'", "name='C'", maxPathLength=1)
    assert paths3.count() == 0


def test_all_paths(local_g: GraphFrame) -> None:
    # local_g: A->B (love), B->A (hate), B->C (follow)
    # Directed: A can reach C via A->B->C (1 path)
    paths = local_g.all_paths("name='A'", "name='C'", use_local_checkpoints=True)
    assert paths is not None
    assert {"path", "len"}.issubset(set(paths.columns))
    paths_list = paths.collect()
    assert len(paths_list) == 1
    assert paths_list[0]["len"] == 2

    # With edge filter that removes the 'follow' edge: no path A->C
    paths_filtered = local_g.all_paths(
        "name='A'", "name='C'", edge_filter="action!='follow'", use_local_checkpoints=True
    )
    assert paths_filtered.count() == 0

    # Undirected: A->B->C and A->B->A (no simple path to C via A again),
    # but also C->B->A is now possible, still only A->B->C from A to C
    paths_undirected = local_g.all_paths(
        "name='A'", "name='C'", is_directed=False, use_local_checkpoints=True
    )
    assert paths_undirected.count() >= 1

    # max_path_length too short: no paths
    paths_short = local_g.all_paths(
        "name='A'", "name='C'", max_path_length=1, use_local_checkpoints=True
    )
    assert paths_short.count() == 0


def test_power_iteration_clustering(spark: SparkSession) -> None:
    vertices = [
        (1, 0, 0.5),
        (2, 0, 0.5),
        (2, 1, 0.7),
        (3, 0, 0.5),
        (3, 1, 0.7),
        (3, 2, 0.9),
        (4, 0, 0.5),
        (4, 1, 0.7),
        (4, 2, 0.9),
        (4, 3, 1.1),
        (5, 0, 0.5),
        (5, 1, 0.7),
        (5, 2, 0.9),
        (5, 3, 1.1),
        (5, 4, 1.3),
    ]
    edges = [(0,), (1,), (2,), (3,), (4,), (5,)]
    g = GraphFrame(
        v=spark.createDataFrame(edges).toDF("id"),
        e=spark.createDataFrame(vertices).toDF("src", "dst", "weight"),
    )
    clusters_df = g.powerIterationClustering(k=2, maxIter=40, weightCol="weight")

    clusters = [r["cluster"] for r in clusters_df.sort("id").collect()]

    if is_remote():
        # It returns different results on Connect/Classic;
        # For connect mode it works like a smoke-test
        assert len(clusters) == 6
    else:
        assert clusters == [0, 0, 0, 0, 1, 0]
    _ = clusters_df.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_page_rank(spark: SparkSession, args: PregelArguments) -> None:
    edges = spark.createDataFrame(
        [
            [0, 1],
            [1, 2],
            [2, 4],
            [2, 0],
            [3, 4],  # 3 has no in-links
            [4, 0],
            [4, 2],
        ],
        ["src", "dst"],
    )
    _ = edges.cache()
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    numVertices = vertices.count()

    vertices = GraphFrame(vertices, edges).outDegrees
    _ = vertices.toPandas().head()
    _ = vertices.cache()

    # Construct a new GraphFrame with the updated vertices DataFrame.
    graph = GraphFrame(vertices, edges)
    alpha = 0.15
    pregel = graph.pregel
    ranks = (
        graph.pregel.setMaxIter(5)
        .withVertexColumn(
            "rank",
            sqlfunctions.lit(1.0 / numVertices),
            sqlfunctions.coalesce(pregel.msg(), sqlfunctions.lit(0.0))
            * sqlfunctions.lit(1.0 - alpha)
            + sqlfunctions.lit(alpha / numVertices),
        )
        .sendMsgToDst(pregel.src("rank") / pregel.src("outDegree"))
        .aggMsgs(sqlfunctions.sum(pregel.msg()))
        .run()
    )
    resultRows = ranks.sort("id").collect()
    result = map(lambda x: x.rank, resultRows)
    expected = [0.245, 0.224, 0.303, 0.03, 0.197]

    # Compare each result with its expected value using a tolerance of 1e-3.
    for a, b in zip(result, expected):
        assert a == pytest.approx(b, abs=1e-3)
    _ = ranks.unpersist()


def test_graphframes_pagerank(spark: SparkSession) -> None:
    """Regression test for graphframes/graphframes#889: pageRank fails on Spark Connect due to
    AttributeError accessing self.edges and self.outDegrees on GraphFrameConnect."""
    edges = spark.createDataFrame(
        [
            [0, 1],
            [1, 2],
            [2, 4],
            [2, 0],
            [3, 4],
            [4, 0],
            [4, 2],
        ],
        ["src", "dst"],
    )
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    g = GraphFrame(vertices, edges)

    result = g.pageRank(resetProbability=0.15, maxIter=3)

    assert "pagerank" in result.vertices.columns
    assert "weight" in result.edges.columns
    assert result.vertices.count() == 5
    assert result.edges.count() == edges.count()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_pregel_early_stopping(spark: SparkSession, args: PregelArguments) -> None:
    edges = spark.createDataFrame(
        [
            [0, 1],
            [1, 2],
            [2, 4],
            [2, 0],
            [3, 4],  # 3 has no in-links
            [4, 0],
            [4, 2],
        ],
        ["src", "dst"],
    )
    _ = edges.cache()
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    numVertices = vertices.count()

    vertices = GraphFrame(vertices, edges).outDegrees
    _ = vertices.toPandas().head()
    _ = vertices.cache()

    # Construct a new GraphFrame with the updated vertices DataFrame.
    graph = GraphFrame(vertices, edges)
    alpha = 0.15
    pregel = graph.pregel
    ranks = (
        graph.pregel.setMaxIter(5)
        .setUseLocalCheckpoints(args.use_local_checkpoints)
        .setIntermediateStorageLevel(args.storage_level)
        .setCheckpointInterval(args.checkpoint_interval)
        .setEarlyStopping(True)
        .setUseLocalCheckpoints(args.use_local_checkpoints)
        .setIntermediateStorageLevel(args.storage_level)
        .setCheckpointInterval(args.checkpoint_interval)
        .withVertexColumn(
            "rank",
            sqlfunctions.lit(1.0 / numVertices),
            sqlfunctions.coalesce(pregel.msg(), sqlfunctions.lit(0.0))
            * sqlfunctions.lit(1.0 - alpha)
            + sqlfunctions.lit(alpha / numVertices),
        )
        .sendMsgToDst(pregel.src("rank") / pregel.src("outDegree"))
        .aggMsgs(sqlfunctions.sum(pregel.msg()))
        .run()
    )
    resultRows = ranks.sort("id").collect()
    result = map(lambda x: x.rank, resultRows)
    expected = [0.245, 0.224, 0.303, 0.03, 0.197]

    # Compare each result with its expected value using a tolerance of 1e-3.
    for a, b in zip(result, expected):
        assert a == pytest.approx(b, abs=1e-3)
    _ = ranks.unpersist()


def test_pregel_required_edge_columns(spark: SparkSession) -> None:
    edges = spark.createDataFrame(
        [(0, 1, 0.5), (1, 2, 1.0), (2, 0, 0.3)],
        ["src", "dst", "weight"],
    )
    vertices = spark.createDataFrame([(0,), (1,), (2,)], ["id"])
    graph = GraphFrame(vertices, edges)
    pregel = graph.pregel

    result = (
        graph.pregel.setMaxIter(2)
        .withVertexColumn(
            "value",
            sqlfunctions.lit(0.0),
            sqlfunctions.coalesce(pregel.msg(), sqlfunctions.lit(0.0)),
        )
        .sendMsgToDst(pregel.src("value") + pregel.edge("weight"))
        .aggMsgs(sqlfunctions.sum(pregel.msg()))
        .required_edge_columns("weight")
        .run()
    )
    assert "value" in result.columns
    assert result.count() == 3
    _ = result.unpersist()


def _df_hasCols(df: DataFrame, vcols: list[str] = []) -> None:
    for c in vcols:
        assert c in df.columns, f"DataFrame missing column: {c}"


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
@pytest.mark.parametrize(
    "cc_args",
    [(-1, True), (10000, True), (-1, False), (10000, False)],
    ids=["aqe,local", "skewed,local", "aqe,checkpoints", "skewed,checkpoints"],
)
def test_connected_components(
    spark: SparkSession, args: PregelArguments, cc_args: tuple[int, bool]
) -> None:
    v = spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
    e = spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"])
    g = GraphFrame(v, e)
    comps = g.connectedComponents(
        algorithm=args.algorithm,
        checkpointInterval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
        broadcastThreshold=cc_args[0],
        useLabelsAsComponents=cc_args[1],
    )
    _df_hasCols(comps, vcols=["id", "component", "vattr", "gender"])
    assert comps.count() == 1
    _ = comps.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
@pytest.mark.parametrize(
    "cc_args",
    [(-1, True), (10000, True), (-1, False), (10000, False)],
    ids=["aqe,local", "skewed,local", "aqe,checkpoints", "skewed,checkpoints"],
)
def test_connected_components2(
    spark: SparkSession, args: PregelArguments, cc_args: tuple[int, bool]
) -> None:
    v = spark.createDataFrame([(0, "a0", "b0"), (1, "a1", "b1")], ["id", "A", "B"])
    e = spark.createDataFrame([(0, 1, "a01", "b01")], ["src", "dst", "A", "B"])
    g = GraphFrame(v, e)
    comps = g.connectedComponents(
        algorithm=args.algorithm,
        checkpointInterval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
        broadcastThreshold=cc_args[0],
        useLabelsAsComponents=cc_args[1],
    )
    _df_hasCols(comps, vcols=["id", "component", "A", "B"])
    assert comps.count() == 2
    _ = comps.unpersist()


def test_connected_components_example(spark: SparkSession) -> None:
    nodes = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    nodes_df = spark.createDataFrame(nodes, ["id", "name", "age"])

    edges = [
        (1, 2, "friend"),
        (2, 1, "friend"),
        (2, 3, "friend"),
        (3, 2, "enemy"),  # eek!
    ]
    edges_df = spark.createDataFrame(edges, ["src", "dst", "relationship"])

    g = GraphFrame(nodes_df, edges_df)
    cc = g.connectedComponents()
    cc.write.mode("overwrite").format("noop").save()
    res = cc.collect()
    assert len(res) == 3
    _ = cc.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_shortest_paths(spark: SparkSession, args: PregelArguments) -> None:
    edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
    # Create bidirectional edges.
    all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edgesDF = spark.createDataFrame(all_edges, ["src", "dst"])
    vertices = spark.createDataFrame([(i,) for i in range(1, 7)], ["id"])
    g = GraphFrame(vertices, edgesDF)
    landmarks: list[str | int] = [1, 4]
    v2 = g.shortestPaths(
        landmarks=landmarks,
        algorithm=args.algorithm,
        use_local_checkpoints=args.use_local_checkpoints,
        checkpoint_interval=args.checkpoint_interval,
        storage_level=args.storage_level,
    )
    _df_hasCols(v2, vcols=["id", "distances"])
    _ = v2.unpersist()


def test_shortest_paths2(spark: SparkSession) -> None:
    # Create an undirected graph
    vertices = spark.createDataFrame([(i,) for i in range(1, 6)], ["id"])
    edges = spark.createDataFrame([(1, 2), (2, 3), (3, 4), (4, 5)], ["src", "dst"])
    g = GraphFrame(vertices, edges)
    landmarks = [1]
    result = g.shortestPaths(landmarks=landmarks, is_directed=False)

    # Check that distances are correct
    distances = result.sort("id").select("id", "distances").collect()

    assert distances[0]["distances"] == {1: 0}
    assert distances[1]["distances"] == {1: 1}
    assert distances[2]["distances"] == {1: 2}
    assert distances[3]["distances"] == {1: 3}
    assert distances[4]["distances"] == {1: 4}

    _ = result.unpersist()


def test_neighborhood_aware_cdlp_api_defaults(spark: SparkSession) -> None:
    if spark.version[:3] < "4.1":
        pytest.skip("NeighborhoodAwareCDLP requires Spark >= 4.1")

    vertices = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    edges = spark.createDataFrame([(1, 2), (2, 3), (3, 1)], ["src", "dst"])
    g = GraphFrame(vertices, edges)

    result = g.neighborhood_aware_cdlp(max_iter=1)
    _df_hasCols(result, vcols=["id", "label"])
    _ = result.unpersist()


def test_neighborhood_aware_cdlp_api_with_all_args(spark: SparkSession) -> None:
    if spark.version[:3] < "4.1":
        pytest.skip("NeighborhoodAwareCDLP requires Spark >= 4.1")

    vertices = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C"), (4, "D")],
        ["id", "seed_label"],
    )
    edges = spark.createDataFrame([(1, 2), (2, 3), (3, 4), (4, 1)], ["src", "dst"])
    g = GraphFrame(vertices, edges)

    result = g.neighborhood_aware_cdlp(
        max_iter=2,
        structural_similarity_multiplier=0.25,
        ignore_direct_links=False,
        initial_label_col="seed_label",
        is_directed=False,
        lg_nom_entries=12,
        use_local_checkpoints=False,
        checkpoint_interval=2,
        storage_level=StorageLevel.MEMORY_AND_DISK_DESER,
    )
    _df_hasCols(result, vcols=["id", "label"])
    _ = result.unpersist()


def test_neighborhood_aware_cdlp_api_rejects_invalid_multiplier_combination(
    spark: SparkSession,
) -> None:
    vertices = spark.createDataFrame([(1,), (2,)], ["id"])
    edges = spark.createDataFrame([(1, 2)], ["src", "dst"])
    g = GraphFrame(vertices, edges)

    with pytest.raises(ValueError, match="must be > 0 when ignore_direct_links is True"):
        _ = g.neighborhood_aware_cdlp(
            max_iter=1,
            structural_similarity_multiplier=0.0,
            ignore_direct_links=True,
        )


def test_random_walk_embeddings_api(local_g: GraphFrame) -> None:
    rwe = RandomWalkEmbeddings(local_g)
    rwe.set_rw_model("/tmp/")
    rwe.set_hash2vec()

    result = rwe.run()
    result.write.mode("overwrite").format("noop").save()


def test_random_walk_embeddings_invalid_args(local_g: GraphFrame) -> None:
    rwe = RandomWalkEmbeddings(local_g)

    with pytest.raises(ValueError, match="supported decay functions are"):
        rwe.set_hash2vec(decay_function="invalid_function")

    with pytest.raises(ValueError, match="TMP path or cached walks path should be provided!"):
        rwe.run()


def test_strongly_connected_components(spark: SparkSession) -> None:
    # Simple island test
    vertices = spark.createDataFrame([(i,) for i in range(1, 6)], ["id"])
    edges = spark.createDataFrame([(7, 8)], ["src", "dst"])
    g = GraphFrame(vertices, edges)
    c = g.stronglyConnectedComponents(5)
    for row in c.collect():
        assert row.id == row.component, (
            f"Vertex {row.id} not equal to its component {row.component}"
        )
    _ = c.unpersist()


@pytest.mark.parametrize("storage_level", STORAGE_LEVELS, ids=STORAGE_LEVELS_IDS)
def test_triangle_counts(spark: SparkSession, storage_level: StorageLevel) -> None:
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
    vertices = spark.createDataFrame([(0,), (1,), (2,)], ["id"])
    g = GraphFrame(vertices, edges)
    c = g.triangleCount(storage_level=storage_level)
    for row in c.select("id", "count").collect():
        assert row.asDict()["count"] == 1, f"Triangle count for vertex {row.id} is not 1"
    _ = c.unpersist()


def test_approx_triangle_counts(spark: SparkSession) -> None:
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
    vertices = spark.createDataFrame([(0,), (1,), (2,)], ["id"])
    g = GraphFrame(vertices, edges)

    if spark.version[:3] >= "4.1":
        c = g.triangleCount(storage_level=StorageLevel.MEMORY_AND_DISK, algorithm="approx")
        for row in c.select("id", "count").collect():
            assert row.asDict()["count"] == 1, f"Triangle count for vertex {row.id} is not 1"
        _ = c.unpersist()
    else:
        with pytest.raises(ValueError, match=".*requires Spark.*"):
            c = g.triangleCount(storage_level=StorageLevel.MEMORY_AND_DISK, algorithm="approx")


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_cycles_finding(spark: SparkSession, args: PregelArguments) -> None:
    vertices = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "attr"]
    )
    edges = spark.createDataFrame([(1, 2), (2, 3), (3, 1), (1, 4), (2, 5)], ["src", "dst"])
    graph = GraphFrame(vertices, edges)
    res = graph.detectingCycles(
        checkpoint_interval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
    )
    assert res.count() == 1
    collected = res.sort("id").select("found_cycles").collect()
    assert collected[0][0] == [1, 2, 3, 1]
    _ = res.unpersist()


@pytest.mark.parametrize("storage_level", STORAGE_LEVELS, ids=STORAGE_LEVELS_IDS)
def test_mis(spark: SparkSession, storage_level: StorageLevel) -> None:
    # Create a graph with isolated vertices
    vertices = spark.createDataFrame([(0, "a"), (1, "b"), (2, "c"), (3, "d")], ["id", "name"])

    # Only connect vertices 0 and 1
    edges = spark.createDataFrame([(0, 1, "edge1")], ["src", "dst", "name"])

    graph = GraphFrame(vertices, edges)
    mis = graph.maximal_independent_set(storage_level=storage_level, seed=12345)

    # Check that all vertices are in the MIS (since 2 and 3 are isolated)
    mis_ids = set(row[0] for row in mis.select("id").collect())
    assert len(mis_ids) == 3, "MIS should contain 2 isolated vertices and one of linked"
    assert 2 in mis_ids, "Isolated vertex 2 should be in MIS"
    assert 3 in mis_ids, "Isolated vertex 3 should be in MIS"

    _ = mis.unpersist()


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_svd_plus_plus(examples, spark: SparkSession):
    from pyspark.graphframes.classic.graphframe import _from_java_gf

    g = _from_java_gf(getattr(examples, "ALSSyntheticData")(), spark)
    (v2, cost) = g.svdPlusPlus()
    _df_hasCols(v2, vcols=["id", "column1", "column2", "column3", "column4"])


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_mutithreaded_sparksession_usage(spark: SparkSession):
    # Test that the GraphFrame API works correctly from multiple threads.
    localVertices = [(1, "A"), (2, "B"), (3, "C")]
    localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
    v = spark.createDataFrame(localVertices, ["id", "name"])
    e = spark.createDataFrame(localEdges, ["src", "dst", "action"])

    exc = None

    def run_graphframe() -> None:
        nonlocal exc
        try:
            GraphFrame(v, e)
        except Exception as _e:
            exc = _e

    import threading

    thread = threading.Thread(target=run_graphframe)
    thread.start()
    thread.join()
    assert exc is None, f"Exception was raised in thread: {exc}"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_belief_propagation(spark: SparkSession):
    from pyspark.graphframes.examples import BeliefPropagation, Graphs

    # Create a graphical model g of size 3x3.
    g = Graphs(spark).gridIsingModel(3)
    # Run Belief Propagation (BP) for 5 iterations.
    numIter = 5
    results = BeliefPropagation.runBPwithGraphFrames(g, numIter)
    # Check that each belief is a valid probability in [0, 1].
    for row in results.vertices.select("belief").collect():
        belief = row["belief"]
        assert 0 <= belief <= 1, f"Expected belief to be probability in [0,1], but found {belief}"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_graph_friends(spark: SparkSession):
    from pyspark.graphframes.examples import Graphs

    # Construct the graph.
    g = Graphs(spark).friends()
    # Check that the result is an instance of GraphFrame.
    assert isinstance(g, GraphFrame)


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_graph_grid_ising_model(spark: SparkSession):
    from pyspark.graphframes.examples import Graphs

    # Construct a grid Ising model graph.
    n = 3
    g = Graphs(spark).gridIsingModel(n)
    # Collect the vertex ids
    ids = [v["id"] for v in g.vertices.collect()]
    # Verify that every expected vertex id appears.
    for i in range(n):
        for j in range(n):
            assert f"{i},{j}" in ids


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_kcore(spark: SparkSession, args: PregelArguments) -> None:
    # Create a graph designed to have clear k-core layers
    v = spark.createDataFrame([(i, f"v{i}") for i in range(30)], ["id", "name"])

    # Build edges to create a hierarchical structure:
    # Core (k=5): vertices 0-4 - fully connected
    core_edges = [(i, j) for i in range(5) for j in range(i + 1, 5)]

    # Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
    mid_layer_edges = [
        (5, 0),
        (5, 1),
        (5, 2),  # Connect to core
        (6, 0),
        (6, 1),
        (6, 3),
        (7, 1),
        (7, 2),
        (7, 4),
        (8, 0),
        (8, 3),
        (8, 4),
        (9, 1),
        (9, 2),
        (9, 3),
        (10, 0),
        (10, 4),
        (11, 2),
        (11, 3),
        (12, 1),
        (12, 4),
        (13, 0),
        (13, 2),
        (14, 3),
        (14, 4),
    ]

    # Outer layer (k=1): vertices 15-29 - sparse connections
    outer_edges = [
        (15, 5),
        (16, 6),
        (17, 7),
        (18, 8),
        (19, 9),
        (20, 10),
        (21, 11),
        (22, 12),
        (23, 13),
        (24, 14),
        (25, 15),
        (26, 16),
        (27, 17),
        (28, 18),
        (29, 19),
    ]

    all_edges = core_edges + mid_layer_edges + outer_edges
    e = spark.createDataFrame(all_edges, ["src", "dst"])
    g = GraphFrame(v, e)
    result = g.k_core(
        checkpoint_interval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
    )

    assert result.count() == 30

    rows = result.collect()
    kcore_map = {row["id"]: row["kcore"] for row in rows}

    # Validate hierarchical structure
    # Core vertices (0-4) should have highest k-core
    for i in range(5):
        assert kcore_map[i] >= 4, f"Core vertex {i} should have high k-core, got {kcore_map[i]}"

    # Mid-layer vertices (5-14) should have medium k-core
    for i in range(5, 15):
        assert 2 <= kcore_map[i] <= 4, (
            f"Mid-layer vertex {i} should have medium k-core, got {kcore_map[i]}"
        )

    # Outer vertices (15-29) should have low k-core
    for i in range(15, 30):
        assert kcore_map[i] <= 2, f"Outer vertex {i} should have low k-core, got {kcore_map[i]}"

    _ = result.unpersist()


def test_aggregate_neighbors_basic(spark: SparkSession) -> None:
    """Test basic AggregateNeighbors functionality with argument verification."""
    # Create a simple graph: 1 -> 2 -> 3
    v = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])
    e = spark.createDataFrame([(1, 2), (2, 3)], ["src", "dst"])
    g = GraphFrame(v, e)

    # Test basic path finding from vertex 1 to vertex 3
    result = g.aggregate_neighbors(
        starting_vertices=sqlfunctions.col("id") == 1,
        max_hops=3,
        accumulator_names=["path_length"],
        accumulator_inits=[sqlfunctions.lit(0)],
        accumulator_updates=[sqlfunctions.col("path_length") + 1],
        target_condition=AggregateNeighbors.dst_attr("id") == 3,
        required_vertex_attributes=["id"],
    )

    # Verify result structure
    assert "id" in result.columns
    assert "hop" in result.columns
    assert "path_length" in result.columns

    # Should find one path: 1 -> 2 -> 3
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 3
    assert rows[0]["hop"] == 2
    assert rows[0]["path_length"] == 2

    _ = result.unpersist()


def test_aggregate_neighbors_with_edge_filter(spark: SparkSession) -> None:
    """Test AggregateNeighbors with edge filtering."""
    # Create a graph with different edge types
    v = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C"), (4, "D")], ["id", "name"])
    e = spark.createDataFrame(
        [(1, 2, "allowed"), (2, 3, "allowed"), (1, 3, "blocked")],
        ["src", "dst", "edge_type"],
    )
    g = GraphFrame(v, e)

    result = g.aggregate_neighbors(
        starting_vertices=sqlfunctions.col("id") == 1,
        max_hops=3,
        accumulator_names=["count"],
        accumulator_inits=[sqlfunctions.lit(0)],
        accumulator_updates=[sqlfunctions.col("count") + 1],
        target_condition=AggregateNeighbors.dst_attr("id") == 3,
        edge_filter=AggregateNeighbors.edge_attr("edge_type") == "allowed",
        required_edge_attributes=["edge_type"],
    )

    rows = result.collect()
    # Should only find path 1 -> 2 -> 3 (not 1 -> 3 directly due to filter)
    assert len(rows) == 1
    assert rows[0]["count"] == 2  # Two hops

    _ = result.unpersist()


def test_aggregate_neighbors_multiple_accumulators(spark: SparkSession) -> None:
    """Test AggregateNeighbors with multiple accumulators."""
    v = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "value"])
    e = spark.createDataFrame([(1, 2, 5.0), (2, 3, 6.0)], ["src", "dst", "weight"])
    g = GraphFrame(v, e)

    result = g.aggregate_neighbors(
        starting_vertices=sqlfunctions.col("id") == 1,
        max_hops=3,
        accumulator_names=["sum_values", "sum_weights"],
        accumulator_inits=[sqlfunctions.lit(0), sqlfunctions.lit(0.0)],
        accumulator_updates=[
            sqlfunctions.col("sum_values") + AggregateNeighbors.dst_attr("value"),
            sqlfunctions.col("sum_weights") + AggregateNeighbors.edge_attr("weight"),
        ],
        target_condition=AggregateNeighbors.dst_attr("id") == 3,
        required_vertex_attributes=["id", "value"],
        required_edge_attributes=["weight"],
    )

    rows = result.collect()
    assert len(rows) == 1
    # sum_values: 20 + 30 = 50
    assert rows[0]["sum_values"] == 50
    # sum_weights: 5.0 + 6.0 = 11.0
    assert abs(rows[0]["sum_weights"] - 11.0) < 0.001

    _ = result.unpersist()


def test_hyper_anf_basic(spark: SparkSession) -> None:
    """Smoke test: verify hyper_anf returns correct columns and basic functionality."""
    # Directed graph: 1 -> 2, 2 -> 3, 3 -> 1 (cycle)
    v = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    e = spark.createDataFrame([(1, 2), (2, 3), (3, 1)], ["src", "dst"])
    g = GraphFrame(v, e)

    result = g.hyper_anf(n_hops=2, use_local_checkpoints=True)

    # Verify columns: id, hop_0, hop_1, hop_2
    assert "id" in result.columns
    assert "hop_0" in result.columns
    assert "hop_1" in result.columns
    assert "hop_2" in result.columns

    # Every vertex with an outgoing edge should be present (all 3)
    assert result.count() == 3

    _ = result.unpersist()


def test_hyper_anf_args_passed(spark: SparkSession) -> None:
    """Verify that non-default args (lg_nom_entries, edge_filter) are passed correctly."""
    v = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["id"])
    e = spark.createDataFrame([(1, 2), (2, 3), (3, 4), (1, 3)], ["src", "dst"])
    g = GraphFrame(v, e)

    # Use non-default lg_nom_entries to verify the arg is forwarded
    result_default = g.hyper_anf(n_hops=1, lg_nom_entries=10, use_local_checkpoints=True)
    assert "hop_0" in result_default.columns
    assert "hop_1" in result_default.columns
    assert result_default.count() > 0
    _ = result_default.unpersist()

    # Use edge_filter to restrict computation to edges where src == 1
    result_filtered = g.hyper_anf(
        n_hops=1,
        edge_filter=sqlfunctions.col("src") == 1,
        use_local_checkpoints=True,
    )
    rows = result_filtered.collect()
    # Only vertex 1 has outgoing edges with src == 1
    ids = {row["id"] for row in rows}
    assert ids == {1}

    _ = result_filtered.unpersist()


def test_hyper_anf_invalid_args(spark: SparkSession) -> None:
    """Verify that invalid arguments raise ValueError on the Python side."""
    v = spark.createDataFrame([(1,), (2,)], ["id"])
    e = spark.createDataFrame([(1, 2)], ["src", "dst"])
    g = GraphFrame(v, e)

    with pytest.raises(ValueError, match="n_hops must be a positive integer"):
        g.hyper_anf(n_hops=0)

    with pytest.raises(ValueError, match="n_hops must be a positive integer"):
        g.hyper_anf(n_hops=-1)

    with pytest.raises(ValueError, match="lg_nom_entries must be between 4 and 21"):
        g.hyper_anf(lg_nom_entries=3)

    with pytest.raises(ValueError, match="lg_nom_entries must be between 4 and 21"):
        g.hyper_anf(lg_nom_entries=22)


class GraphFramesUpstreamTests(ReusedSQLTestCase):
    @classmethod
    def conf(cls) -> SparkConf:
        return SparkConf().set("spark.driver.memory", "4g")

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

    def local_graph(self) -> GraphFrame:
        vertices = self.spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])
        edges = self.spark.createDataFrame(
            [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")],
            ["src", "dst", "action"],
        )
        return GraphFrame(vertices, edges)

    def examples(self):
        from pyspark.graphframes.classic.graphframe import _java_api

        return _java_api(self.spark.sparkContext).examples()

    def test_construction(self) -> None:
        test_construction(self.spark, self.local_graph())

    def test_svd_plus_plus(self) -> None:
        test_svd_plus_plus(self.examples(), self.spark)

    def test_page_rank(self) -> None:
        for args in PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                test_page_rank(self.spark, args)

    def test_pregel_early_stopping(self) -> None:
        for args in PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                test_pregel_early_stopping(self.spark, args)

    def test_connected_components(self) -> None:
        for args in PREGEL_ARGUMENTS:
            for cc_args in [(-1, True), (10000, True), (-1, False), (10000, False)]:
                with self.subTest(args=args, cc_args=cc_args):
                    test_connected_components(self.spark, args, cc_args)

    def test_connected_components2(self) -> None:
        for args in PREGEL_ARGUMENTS:
            for cc_args in [(-1, True), (10000, True), (-1, False), (10000, False)]:
                with self.subTest(args=args, cc_args=cc_args):
                    test_connected_components2(self.spark, args, cc_args)

    def test_shortest_paths(self) -> None:
        for args in PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                test_shortest_paths(self.spark, args)

    def test_triangle_counts(self) -> None:
        for storage_level in STORAGE_LEVELS:
            with self.subTest(storage_level=storage_level):
                test_triangle_counts(self.spark, storage_level)

    def test_cycles_finding(self) -> None:
        for args in PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                test_cycles_finding(self.spark, args)

    def test_mis(self) -> None:
        for storage_level in STORAGE_LEVELS:
            with self.subTest(storage_level=storage_level):
                test_mis(self.spark, storage_level)

    def test_kcore(self) -> None:
        for args in PREGEL_ARGUMENTS:
            with self.subTest(args=args):
                test_kcore(self.spark, args)


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
    test_validate,
    test_as_undirected,
    test_as_reversed,
    test_power_iteration_clustering,
    test_graphframes_pagerank,
    test_pregel_required_edge_columns,
    test_connected_components_example,
    test_shortest_paths2,
    test_neighborhood_aware_cdlp_api_defaults,
    test_neighborhood_aware_cdlp_api_with_all_args,
    test_neighborhood_aware_cdlp_api_rejects_invalid_multiplier_combination,
    test_strongly_connected_components,
    test_approx_triangle_counts,
    test_mutithreaded_sparksession_usage,
    test_belief_propagation,
    test_graph_friends,
    test_graph_grid_ising_model,
    test_aggregate_neighbors_basic,
    test_aggregate_neighbors_with_edge_filter,
    test_aggregate_neighbors_multiple_accumulators,
    test_hyper_anf_basic,
    test_hyper_anf_args_passed,
    test_hyper_anf_invalid_args,
]:
    setattr(GraphFramesUpstreamTests, _test_function.__name__, _spark_test(_test_function))


for _test_function in [
    test_cache,
    test_degrees,
    test_type_degrees,
    test_type_degrees_with_explicit_types,
    test_motif_finding,
    test_filterVertices,
    test_filterEdges,
    test_dropIsolatedVertices,
    test_bfs,
    test_all_paths,
    test_random_walk_embeddings_api,
    test_random_walk_embeddings_invalid_args,
]:
    setattr(GraphFramesUpstreamTests, _test_function.__name__, _local_graph_test(_test_function))


if __name__ == "__main__":
    from pyspark.testing.unittestutils import main

    main()
