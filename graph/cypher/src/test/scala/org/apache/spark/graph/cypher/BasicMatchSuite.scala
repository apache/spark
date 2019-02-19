package org.apache.spark.graph.cypher

import org.apache.spark.SparkFunSuite
import org.opencypher.okapi.api.io.conversion.NodeMapping

class BasicMatchSuite extends SparkFunSuite with SharedCypherContext {

  test("match single node pattern") {
    val nodesDf = spark.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val nodesMapping = NodeMapping.on("id").withImpliedLabel("Person").withPropertyKey("name")

    val sparkNodeTable = SparkNodeTable(nodesMapping, nodesDf)

    val graph = sparkCypher.readFrom(sparkNodeTable)

    graph.cypher("MATCH (n) RETURN n").show
  }

}
