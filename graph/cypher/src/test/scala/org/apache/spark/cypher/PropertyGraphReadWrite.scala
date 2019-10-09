/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.cypher

import java.nio.file.Paths

import org.apache.spark.graph.api.{NodeFrame, RelationshipFrame}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterEach

class PropertyGraphReadWrite extends QueryTest with SharedCypherContext with BeforeAndAfterEach {

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

  private lazy val nodeDataFrame: NodeFrame = cypherSession.buildNodeFrame(nodeData)
    .idColumn("id")
    .labelSet(Array("Person"))
    .properties(Map("name" -> "name"))
    .build()

  private lazy val relationshipFrame: RelationshipFrame = RelationshipFrame.create(relationshipData, "id", "source", "target", "KNOWS")

  test("save and load a graph") {
    val graph = cypherSession.createGraph(Array(nodeDataFrame), Array(relationshipFrame))
    graph.write.save(basePath)

    val readGraph = cypherSession.read.load(basePath)
    readGraph.cypher(
      "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2"
    ).df.show()
  }

  test("save and loads a property graph") {
    val nodeData = spark.createDataFrame(Seq(0L -> "Alice", 1L -> "Bob")).toDF("id", "name")
    val nodeFrame = cypherSession.buildNodeFrame(nodeData)
      .idColumn("id")
      .labelSet(Array("Person"))
      .properties(Map("name" -> "name"))
      .build()

    val relationshipData = spark
      .createDataFrame(Seq((0L, 0L, 1L, 1984)))
      .toDF("id", "source", "target", "since")
    val relationshipFrame =
      RelationshipFrame.create(relationshipData, "id", "source", "target", "KNOWS")

    val writeGraph = cypherSession.createGraph(Array(nodeFrame), Array(relationshipFrame))

    withTempDir(file => {
      writeGraph.write.mode(SaveMode.Overwrite).save(file.getAbsolutePath)
      val readGraph = cypherSession.read.load(file.getAbsolutePath)

      checkAnswer(readGraph.nodes, writeGraph.nodes)
      checkAnswer(readGraph.relationships, writeGraph.relationships)
    })
  }
}
