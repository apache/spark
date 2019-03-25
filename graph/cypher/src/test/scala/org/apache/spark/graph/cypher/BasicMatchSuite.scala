package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{CypherResult, NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.graph.cypher.algos.GraphAlgorithms._
import org.apache.spark.sql.DataFrame

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))

    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame))

    graph.cypher("MATCH (n) RETURN n").df.show()
  }

  test("match simple pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(0, 0, 1))).toDF("id", "source", "target")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
    val relationshipFrame: RelationshipFrame = RelationshipFrame(relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")

    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))
    graph.nodes.show()
    graph.relationships.show()

    graph.cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

  test("create property graph from query results") {
    val personData: DataFrame = spark.createDataFrame(Seq(Tuple3(0, "Alice", 42), Tuple3(1, "Bob", 23), Tuple3(2, "Eve", 19))).toDF("id", "name", "age")
    val universityData: DataFrame = spark.createDataFrame(Seq(2 -> "UC Berkeley", 3 -> "Stanford")).toDF("id", "title")
    val knowsData: DataFrame = spark.createDataFrame(Seq(Tuple3(0, 0, 1), Tuple3(1, 0, 2))).toDF("id", "source", "target")
    val studyAtData: DataFrame = spark.createDataFrame(Seq(Tuple3(2, 0, 2), Tuple3(3, 1, 3), Tuple3(4, 2, 2))).toDF("id", "source", "target")
    val personDataFrame: NodeFrame = NodeFrame(df = personData, idColumn = "id", labels = Set("Student"))
    val universityDataFrame: NodeFrame = NodeFrame(df = universityData, idColumn = "id", labels = Set("University"))
    val knowsDataFrame: RelationshipFrame = RelationshipFrame(knowsData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
    val studyAtDataFrame: RelationshipFrame = RelationshipFrame(studyAtData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "STUDY_AT")

    val graph: PropertyGraph = cypherSession.createGraph(Seq(personDataFrame, universityDataFrame), Seq(knowsDataFrame, studyAtDataFrame))

    val result: CypherResult = graph.cypher(
      """
        |MATCH (p:Student)-[:STUDY_AT]->(u:University),
        |      (p)-[k:KNOWS]->(o:Student)-[:STUDY_AT]->(u)
        |WHERE u.title = 'UC Berkeley'
        |RETURN p, o, k
        |""".stripMargin)

    // Option 1: Return NodeFrames and RelationshipFrames
    val berkeleyStudents: Seq[NodeFrame] = result.nodeFrames("p")
    val berkeleyStudentFriends: Seq[NodeFrame] = result.nodeFrames("o")
    val knows: Seq[RelationshipFrame] = result.relationshipFrames("k")
    val berkeleyGraph: PropertyGraph = cypherSession.createGraph(berkeleyStudents ++ berkeleyStudentFriends, knows)
    berkeleyGraph.cypher("MATCH (n:Student)-[:KNOWS]->(o:Student) RETURN n.name AS person, o.name AS friend").df.show()

    // Option 2: Use CypherResult to create a new PropertyGraph
    val berkeleyGraph2 = result.graph
    berkeleyGraph2.cypher("MATCH (n:Student)-[:KNOWS]->(o:Student) RETURN n.name AS person, o.name AS friend").df.show()
  }

  test("round trip example using column name conventions") {
    val graph1: PropertyGraph = cypherSession.createGraph(nodes, relationships)
    val graph2: PropertyGraph = cypherSession.createGraph(graph1.nodes, graph1.relationships)
    graph2.nodes.show()
    graph2.relationships.show()
  }

  test("example for retaining user ids") {
    val nodesWithRetainedId = nodes.withColumn("retainedId", nodes.col("$ID"))
    val relsWithRetainedId = relationships.withColumn("retainedId", relationships.col("$ID"))

    cypherSession
      .createGraph(nodesWithRetainedId, relsWithRetainedId)
      .cypher("MATCH (n:Student)-[:STUDY_AT]->(u:University) RETURN n, u").df.show()
  }

  test("query composition + graph analytics workflow") {
    cypherSession.createGraph(nodes, relationships)
      .cypher(
        """|MATCH (student:Student)-[:STUDY_AT]->(uni:University),
           |      (student)-[knows:KNOWS]-(:Student)
           |WHERE uni.title = 'UC Berkeley'
           |RETURN student, knows""".stripMargin)
      .graph
      .withPageRank(tolerance = 0.02)
      .cypher("MATCH (n:Student) RETURN n.name, n.pageRank ORDER BY n.pageRank DESC")
      .df.show()
  }

  lazy val nodes: DataFrame = spark.createDataFrame(Seq(
    (0L, true, false, Some("Alice"), Some(42), None),
    (1L, true, false, Some("Bob"), Some(23), None),
    (2L, true, false, Some("Carol"), Some(22), None),
    (3L, true, false, Some("Eve"), Some(19), None),
    (4L, false, true, None, None, Some("UC Berkeley")),
    (5L, false, true, None, None, Some("Stanford"))
  )).toDF("$ID", ":Student", ":University", "name", "age", "title")

  lazy val relationships: DataFrame = spark.createDataFrame(Seq(
    (0L, 0L, 1L, true, false),
    (1L, 0L, 3L, true, false),
    (2L, 1L, 3L, true, false),
    (3L, 3L, 0L, true, false),
    (4L, 3L, 1L, true, false),
    (5L, 0L, 4L, false, true),
    (6L, 1L, 4L, false, true),
    (7L, 3L, 4L, false, true),
    (8L, 2L, 5L, false, true),
  )).toDF("$ID", "$SOURCE_ID", "$TARGET_ID", ":KNOWS", ":STUDY_AT")
}
