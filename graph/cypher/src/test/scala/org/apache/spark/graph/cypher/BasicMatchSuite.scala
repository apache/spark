package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{CypherResult, NodeDataFrame, PropertyGraph, RelationshipDataFrame}
import org.apache.spark.sql.DataFrame

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(id(0) -> "Alice", id(1) -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeDataFrame = NodeDataFrame(df = nodeData, idColumn = "id", labels = Set("Person"), properties = Map("name" -> "name"))

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(nodeDataFrame))

    graph.cypher("MATCH (n) RETURN n").df.show()
  }

  test("match simple pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(id(0) -> "Alice", id(1) -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), id(0), id(1)))).toDF("id", "source", "target")
    val nodeDataFrame: NodeDataFrame = NodeDataFrame(df = nodeData, idColumn = "id", labels = Set("Person"), properties = Map("name" -> "name"))
    val relationshipFrame: RelationshipDataFrame = RelationshipDataFrame(df = relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))

    graph.cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

  // TODO: requires fixing escaping in CAPS
  ignore("create property graph from query results") {
    val personData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), "Alice", 42), Tuple3(id(1), "Bob", 23), Tuple3(id(2), "Eve", 19))).toDF("id", "name", "age")
    val universityData: DataFrame = spark.createDataFrame(Seq(id(2) -> "UC Berkeley", id(3)-> "Stanford")).toDF("id", "title")
    val knowsData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), id(0), id(1)), Tuple3(id(1), id(0), id(2)))).toDF("id", "source", "target")
    val studyAtData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(2), id(0), id(2)), Tuple3(id(3), id(1), id(3)), Tuple3(id(4), id(2), id(2)))).toDF("id", "source", "target")
    val personDataFrame: NodeDataFrame = NodeDataFrame(df = personData, idColumn = "id", labels = Set("Student"), properties = Map("name" -> "name", "age" -> "age"))
    val universityDataFrame: NodeDataFrame = NodeDataFrame(df = universityData, idColumn = "id", labels = Set("University"), properties = Map("title" -> "title"))
    val knowsDataFrame: RelationshipDataFrame = RelationshipDataFrame(df = knowsData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
    val studyAtDataFrame: RelationshipDataFrame = RelationshipDataFrame(df = studyAtData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "STUDY_AT")

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(personDataFrame, universityDataFrame), Seq(knowsDataFrame, studyAtDataFrame))
    val result: CypherResult = graph.cypher(
      """
        |MATCH (p:Student)-[:STUDY_AT]->(u:University),
        |      (p)-[k:KNOWS]->(o:Student)-[:STUDY_AT]->(u)
        |WHERE u.title = 'UC Berkeley'
        |RETURN p, o, k
        |""".stripMargin)

    val berkeleyStudents: Seq[NodeDataFrame] = result.nodeDataFrame("p")
    val berkeleyStudentFriends: Seq[NodeDataFrame] = result.nodeDataFrame("o")
    val knows: Seq[RelationshipDataFrame] = result.relationshipDataFrame("k")

    val berkeleyGraph: PropertyGraph = cypherEngine.createGraph(berkeleyStudents ++ berkeleyStudentFriends, knows)
    berkeleyGraph.cypher("MATCH (n:Student)-[:KNOWS]->(o:Student) RETURN n.name AS person, o.name AS friend").df.show()

  }

  private def id(l: Long): Array[Byte] = BigInt(l).toByteArray
}
