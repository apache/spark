package org.apache.spark.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{CypherResult, NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.sql.DataFrame

class GraphExamplesSuite extends SparkFunSuite with SharedCypherContext {

  test("create graph with nodes") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame))
    val result: CypherResult = graph.cypher("MATCH (n) RETURN n")
    result.df.show()
  }

  test("create graph with nodes and relationships") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq((0, 0, 1))).toDF("id", "source", "target")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
    val relationshipFrame: RelationshipFrame = RelationshipFrame(relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))
    val result: CypherResult = graph.cypher(
      """
        |MATCH (a:Person)-[r:KNOWS]->(:Person)
        |RETURN a, r""".stripMargin)
    result.df.show()
  }

  test("create graph with multiple node and relationship types") {
    val studentDF: DataFrame = spark.createDataFrame(Seq((0, "Alice", 42), (1, "Bob", 23))).toDF("id", "name", "age")
    val teacherDF: DataFrame = spark.createDataFrame(Seq((2, "Eve", "CS"))).toDF("id", "name", "subject")

    val studentNF: NodeFrame = NodeFrame(df = studentDF, idColumn = "id", labels = Set("Person", "Student"))
    val teacherNF: NodeFrame = NodeFrame(df = teacherDF, idColumn = "id", labels = Set("Person", "Teacher"))

    val knowsDF: DataFrame = spark.createDataFrame(Seq((0, 0, 1, 1984))).toDF("id", "source", "target", "since")
    val teachesDF: DataFrame = spark.createDataFrame(Seq((1, 2, 1))).toDF("id", "source", "target")

    val knowsRF: RelationshipFrame = RelationshipFrame(df = knowsDF, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
    val teachesRF: RelationshipFrame = RelationshipFrame(df = teachesDF, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "TEACHES")

    val graph: PropertyGraph = cypherSession.createGraph(Seq(studentNF, teacherNF), Seq(knowsRF, teachesRF))
    val result: CypherResult = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    result.df.show()
  }

  test("create graph with multiple node and relationship types from wide tables") {
    val nodeDF: DataFrame = spark.createDataFrame(Seq(
      (0L, true, true, false, Some("Alice"), Some(42), None),
      (1L, true, true, false, Some("Bob"), Some(23), None),
      (2L, true, false, true, Some("Eve"), None, Some("CS")),
    )).toDF("$ID", ":Person", ":Student", ":Teacher", "name", "age", "subject")

    val relsDF: DataFrame = spark.createDataFrame(Seq(
      (0L, 0L, 1L, true, false, Some(1984)),
      (1L, 2L, 1L, false, true, None),
    )).toDF("$ID", "$SOURCE_ID", "$TARGET_ID", ":KNOWS", ":TEACHES", "since")

    val graph: PropertyGraph = cypherSession.createGraph(nodeDF, relsDF)
    val result: CypherResult = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    result.df.show()
  }

  test("save and load Property Graph") {
    val graph1: PropertyGraph = cypherSession.createGraph(nodes, relationships)
    graph1.nodes.show()
    graph1.save("/tmp/my-storage")
    val graph2: PropertyGraph = cypherSession.load("/tmp/my-storage")
    graph2.nodes.show()
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
