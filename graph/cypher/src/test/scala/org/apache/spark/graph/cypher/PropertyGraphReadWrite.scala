package org.apache.spark.graph.cypher

import java.nio.file.Paths

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{NodeFrame, RelationshipFrame}
import org.apache.spark.sql.DataFrame
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterEach

class PropertyGraphReadWrite extends SparkFunSuite with SharedCypherContext with BeforeAndAfterEach {

  private var tempDir: TemporaryFolder = _

  override def beforeEach(): Unit = {
    tempDir = new TemporaryFolder()
    tempDir.create()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    tempDir.delete()
  }

  private def basePath: String = s"file://${Paths.get(tempDir.getRoot.getAbsolutePath)}"

  private lazy val nodeData: DataFrame = spark.createDataFrame(Seq(id(0) -> "Alice", id(1) -> "Bob")).toDF("id", "name")
  private lazy val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(id(0), id(0), id(1)))).toDF("id", "source", "target")
  private lazy val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
  private lazy val relationshipFrame: RelationshipFrame = RelationshipFrame(df = relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")

  test("write a graph with orc") {
    cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame)).write.orc(basePath)
    cypherSession.read.orc(basePath).cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

  test("write a graph with parquet") {
    cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame)).write.parquet(basePath)
    cypherSession.read.parquet(basePath).cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
  }

  private def id(l: Long): Array[Byte] = BigInt(l).toByteArray

}
