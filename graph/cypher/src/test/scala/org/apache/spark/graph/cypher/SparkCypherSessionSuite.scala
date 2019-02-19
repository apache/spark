package org.apache.spark.graph.cypher

import org.apache.spark.sql.SparkSession
import org.apache.spark.{LocalSparkContext, SparkFunSuite}
import org.opencypher.okapi.api.io.conversion.NodeMapping

class SparkCypherSessionSuite extends SparkFunSuite with LocalSparkContext {

  test("Initialize SparkCypherSession") {
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    implicit val sparkCypherSession: SparkCypherSession = SparkCypherSession.create

    val nodesDf = sparkSession.createDataFrame(Seq(Array[Byte](0) -> "Alice", Array[Byte](1) -> "Bob")).toDF("id", "name")
    val nodesMapping = NodeMapping.on("id").withImpliedLabel("Person").withPropertyKey("name")

    val sparkNodeTable = SparkNodeTable(nodesMapping, nodesDf)

    val graph = sparkCypherSession.readFrom(sparkNodeTable)

    graph.cypher("MATCH (n) RETURN n").show
  }

}
