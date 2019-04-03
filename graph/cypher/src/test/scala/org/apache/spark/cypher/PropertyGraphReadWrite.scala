package org.apache.spark.cypher

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

  private lazy val nodeData: DataFrame = spark.createDataFrame(Seq(
    0 -> "Alice",
    1 -> "Bob"
  )).toDF("id", "name")

  private lazy val relationshipData: DataFrame = spark.createDataFrame(Seq(
    Tuple3(0, 0, 1)
  )).toDF("id", "source", "target")

  private lazy val nodeDataFrame: NodeFrame = NodeFrame(
    df = nodeData, idColumn = "id", labels = Set("Person")
  )

  private lazy val relationshipFrame: RelationshipFrame = RelationshipFrame(
    relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS"
  )

  test("save and load a graph") {
    val graph = cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))
    graph.save(basePath)

    val readGraph = cypherSession.load(basePath)
    readGraph.cypher(
      "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2"
    ).df.show()
  }

}
