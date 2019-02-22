package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{NodeDataFrame, PropertyGraph, RelationshipDataFrame}
import org.apache.spark.sql.DataFrame

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeDataFrame = NodeDataFrame(df = nodeData, idColumn = "id", labels = Set("Person"), properties = Map("name" -> "name"))

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(nodeDataFrame))

    graph.cypher("MATCH (n) RETURN n").df.show()
  }

  test("match simple pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(Array[Byte](0), Array[Byte](0), Array[Byte](1)))).toDF("id", "source", "target")
    val nodeDataFrame: NodeDataFrame = NodeDataFrame(df = nodeData, idColumn = "id", labels = Set("Person"), properties = Map("name" -> "name"))
    val relationshipFrame: RelationshipDataFrame = RelationshipDataFrame(df = relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))

    graph.cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

}
