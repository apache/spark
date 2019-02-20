package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{NodeDataFrame, PropertyGraph}
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{PropertyGraph => OkapiPropertyGraph}
import org.opencypher.okapi.api.io.conversion.NodeMapping

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern using spark-graph-cypher") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val nodesMapping: NodeMapping = NodeMapping.on("id").withImpliedLabel("Person").withPropertyKey("name")
    val sparkNodeTable: SparkNodeTable = SparkNodeTable(nodesMapping, nodeData)

    val graph: OkapiPropertyGraph = cypherEngine.readFrom(sparkNodeTable)

    graph.cypher("MATCH (n) RETURN n").show
  }

  test("match single node pattern using spark-graph-api") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val nodeDataFrame: NodeDataFrame = NodeDataFrame(df = nodeData, idColumn = "id", labels = Set("Person"), properties = Map("name" -> "name"))

    val graph: PropertyGraph = cypherEngine.createGraph(Seq(nodeDataFrame))

    graph.cypher("MATCH (n) RETURN n").df.show()
  }

}
