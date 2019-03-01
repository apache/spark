package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{CypherResult, NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.sql.DataFrame

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(id(0) -> "Alice", id(1) -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))

    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame))

    graph.cypher("MATCH (n) RETURN n").df.show()
  }

  test("match simple pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(id(0) -> "Alice", id(1) -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), id(0), id(1)))).toDF("id", "source", "target")
    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
    val relationshipFrame: RelationshipFrame = RelationshipFrame(df = relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")

    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))
    graph.nodes.show()
    graph.relationships.show()

    graph.cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

  test("create property graph from query results") {
    val personData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), "Alice", 42), Tuple3(id(1), "Bob", 23), Tuple3(id(2), "Eve", 19))).toDF("id", "name", "age")
    val universityData: DataFrame = spark.createDataFrame(Seq(id(2) -> "UC Berkeley", id(3)-> "Stanford")).toDF("id", "title")
    val knowsData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), id(0), id(1)), Tuple3(id(1), id(0), id(2)))).toDF("id", "source", "target")
    val studyAtData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(2), id(0), id(2)), Tuple3(id(3), id(1), id(3)), Tuple3(id(4), id(2), id(2)))).toDF("id", "source", "target")
    val personDataFrame: NodeFrame = NodeFrame(df = personData, idColumn = "id", labels = Set("Student"))
    val universityDataFrame: NodeFrame = NodeFrame(df = universityData, idColumn = "id", labels = Set("University"))
    val knowsDataFrame: RelationshipFrame = RelationshipFrame(df = knowsData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
    val studyAtDataFrame: RelationshipFrame = RelationshipFrame(df = studyAtData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "STUDY_AT")

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
    val berkeleyGraph2 = cypherSession.createGraph(result)
    berkeleyGraph2.cypher("MATCH (n:Student)-[:KNOWS]->(o:Student) RETURN n.name AS person, o.name AS friend").df.show()
  }

  private def id(l: Long): Array[Byte] = BigInt(l).toByteArray
}
