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

import org.apache.spark.graph.api.CypherSession._
import org.apache.spark.graph.api.{CypherSession, NodeFrame, RelationshipFrame}
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.scalatest.Matchers

class PropertyGraphSuite extends QueryTest with SharedSparkSession with Matchers {

  type IdType = Array[Byte]

  def convertId(inputId: Long): IdType = NumberConverter.toBinary(inputId)

  def cypherSession: CypherSession = SparkCypherSession.create

  test("create graph from NodeFrame") {
    val nodeData = spark.createDataFrame(Seq(0L -> "Alice", 1L -> "Bob")).toDF("id", "name")
    val nodeFrame = NodeFrame.create(nodeData, "id", Set("Person"))
    val graph = cypherSession.createGraph(Array(nodeFrame), Array.empty[RelationshipFrame])

    val expectedDf = spark
      .createDataFrame(Seq((convertId(0L), true, "Alice"), (convertId(1L), true, "Bob")))
      .toDF(ID_COLUMN, label("Person"), "name")

    checkAnswer(graph.nodes, expectedDf)
  }

  test("create graph from NodeFrame and RelationshipFrame") {
    val nodeData = spark.createDataFrame(Seq(0L -> "Alice", 1L -> "Bob")).toDF("id", "name")
    val nodeFrame = NodeFrame.create(nodeData, "id", Set("Person"))

    val relationshipData = spark
      .createDataFrame(Seq((0L, 0L, 1L, 1984)))
      .toDF("id", "source", "target", "since")
    val relationshipFrame = RelationshipFrame.create(
      relationshipData,
      "id",
      "source",
      "target",
      "KNOWS")

    val graph = cypherSession.createGraph(Array(nodeFrame), Array(relationshipFrame))

    val expectedNodeDf = spark
      .createDataFrame(Seq((convertId(0L), true, "Alice"), (convertId(1L), true, "Bob")))
      .toDF(ID_COLUMN, label("Person"), "name")

    val expectedRelDf = spark
      .createDataFrame(Seq((convertId(0L), convertId(0L), convertId(1L), true, 1984)))
      .toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, label("KNOWS"), "since")

    checkAnswer(graph.nodes, expectedNodeDf)
    checkAnswer(graph.relationships, expectedRelDf)
  }

  test("create graph with multiple node and relationship types") {
    val studentDF = spark
      .createDataFrame(Seq((0L, "Alice", 42), (1L, "Bob", 23)))
      .toDF("id", "name", "age")
    val teacherDF = spark
      .createDataFrame(Seq((2L, "Eve", "CS")))
      .toDF("id", "name", "subject")

    val studentNF =
      NodeFrame.create(studentDF, "id", Set("Person", "Student"))
    val teacherNF =
      NodeFrame.create(teacherDF, "id", Set("Person", "Teacher"))

    val knowsDF = spark
      .createDataFrame(Seq((0L, 0L, 1L, 1984)))
      .toDF("id", "source", "target", "since")
    val teachesDF = spark
      .createDataFrame(Seq((1L, 2L, 1L)))
      .toDF("id", "source", "target")

    val knowsRF = RelationshipFrame.create(
      knowsDF,
      "id",
      "source",
      "target",
      "KNOWS")
    val teachesRF = RelationshipFrame.create(
      teachesDF,
      "id",
      "source",
      "target",
      "TEACHES")

    val graph = cypherSession.createGraph(Array(studentNF, teacherNF), Array(knowsRF, teachesRF))

    val expectedNodeDf = spark
      .createDataFrame(
        Seq(
          (convertId(0L), true, true, false, Some(42), Some("Alice"), None),
          (convertId(1L), true, true, false, Some(23), Some("Bob"), None),
          (convertId(2L), true, false, true, None, Some("Eve"), Some("CS"))
        ))
      .toDF(
        ID_COLUMN,
        label("Person"),
        label("Student"),
        label("Teacher"),
        "age",
        "name",
        "subject")

    val expectedRelDf = spark
      .createDataFrame(
        Seq(
          (convertId(0L), convertId(0L), convertId(1L), true, false, Some(1984)),
          (convertId(1L), convertId(2L), convertId(1L), false, true, None)))
      .toDF(
        ID_COLUMN,
        SOURCE_ID_COLUMN,
        TARGET_ID_COLUMN,
        label("KNOWS"),
        label("TEACHES"),
        "since")

    checkAnswer(graph.nodes, expectedNodeDf)
    checkAnswer(graph.relationships, expectedRelDf)
  }

  test("create graph with explicit property-to-column mappings") {
    val studentDF = spark
      .createDataFrame(Seq((0L, "Alice", 42), (1L, "Bob", 23)))
      .toDF("id", "col_name", "col_age")
    val teacherDF = spark
      .createDataFrame(Seq((2L, "Eve", "CS")))
      .toDF("id", "col_name", "col_subject")

    val studentNF = NodeFrame(
      studentDF,
      "id",
      Set("Person", "Student"),
      properties = Map("name" -> "col_name", "age" -> "col_age"))
    val teacherNF = NodeFrame(
      teacherDF,
      "id",
      Set("Person", "Teacher"),
      properties = Map("name" -> "col_name", "subject" -> "col_subject"))

    val knowsDF = spark.createDataFrame(Seq((0L, 0L, 1L, 1984))).toDF("id", "source", "target", "col_since")
    val teachesDF = spark.createDataFrame(Seq((1L, 2L, 1L))).toDF("id", "source", "target")

    val knowsRF = RelationshipFrame(
      knowsDF,
      "id",
      "source",
      "target",
      relationshipType = "KNOWS",
      properties = Map("since" -> "col_since"))
    val teachesRF = RelationshipFrame.create(
      teachesDF,
      "id",
      "source",
      "target",
      "TEACHES")

    val graph = cypherSession.createGraph(Array(studentNF, teacherNF), Array(knowsRF, teachesRF))

    val expectedNodeDf = spark
      .createDataFrame(
        Seq(
          (convertId(0L), true, true, false, Some(42), Some("Alice"), None),
          (convertId(1L), true, true, false, Some(23), Some("Bob"), None),
          (convertId(2L), true, false, true, None, Some("Eve"), Some("CS"))
        ))
      .toDF(
        ID_COLUMN,
        label("Person"),
        label("Student"),
        label("Teacher"),
        "age",
        "name",
        "subject")

    val expectedRelDf = spark
      .createDataFrame(
        Seq(
          (convertId(0L), convertId(0L), convertId(1L), true, false, Some(1984)),
          (convertId(1L), convertId(2L), convertId(1L), false, true, None)))
      .toDF(
        ID_COLUMN,
        SOURCE_ID_COLUMN,
        TARGET_ID_COLUMN,
        label("KNOWS"),
        label("TEACHES"),
        "since")

    checkAnswer(graph.nodes, expectedNodeDf)
    checkAnswer(graph.relationships, expectedRelDf)
  }

  lazy val nodes: DataFrame = spark
    .createDataFrame(
      Seq(
        (0L, true, true, false, false, Some(42), Some("Alice"), None, None),
        (1L, true, true, false, false, Some(23), Some("Bob"), None, None),
        (2L, true, false, true, false, Some(22), Some("Carol"), Some("CS"), None),
        (3L, true, true, false, false, Some(19), Some("Eve"), None, None),
        (4L, false, false, false, true, None, None, None, Some("UC Berkeley")),
        (5L, false, false, false, true, None, None, None, Some("Stanford"))))
    .toDF(
      ID_COLUMN,
      label("Person"),
      label("Student"),
      label("Teacher"),
      label("University"),
      "age",
      "name",
      "subject",
      "title")

  lazy val relationships: DataFrame = spark
    .createDataFrame(
      Seq(
        (0L, 0L, 1L, true, false),
        (1L, 0L, 3L, true, false),
        (2L, 1L, 3L, true, false),
        (3L, 3L, 0L, true, false),
        (4L, 3L, 1L, true, false),
        (5L, 0L, 4L, false, true),
        (6L, 1L, 4L, false, true),
        (7L, 3L, 4L, false, true),
        (8L, 2L, 5L, false, true)))
    .toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, label("KNOWS"), label("STUDY_AT"))


  test("select nodes via label set") {
    val graph = cypherSession.createGraph(nodes, relationships)
    val nodeFrame = graph.nodeFrame(Array("Person", "Teacher"))

    nodeFrame.labelSet shouldEqual Set("Person", "Teacher")
    nodeFrame.idColumn shouldEqual ID_COLUMN
    nodeFrame.properties shouldEqual Map("age" -> "age", "name" -> "name", "subject" -> "subject", "title" -> "title")

    val expectedNodeDf = spark
      .createDataFrame(Seq((convertId(2L), Some(22), Some("Carol"), Some("CS"), None: Option[String])))
      .toDF(ID_COLUMN, "age", "name", "subject", "title")

    checkAnswer(nodeFrame.df, expectedNodeDf)
  }

  test("select relationships via type") {
    val graph = cypherSession.createGraph(nodes, relationships)
    val relationshipFrame = graph.relationshipFrame("KNOWS")

    relationshipFrame.relationshipType shouldEqual "KNOWS"
    relationshipFrame.idColumn shouldEqual ID_COLUMN
    relationshipFrame.sourceIdColumn shouldEqual SOURCE_ID_COLUMN
    relationshipFrame.targetIdColumn shouldEqual TARGET_ID_COLUMN
    relationshipFrame.properties shouldBe empty

    val expectedRelDf = spark
      .createDataFrame(
        Seq(
          (convertId(0L), convertId(0L), convertId(1L)),
          (convertId(1L), convertId(0L), convertId(3L)),
          (convertId(2L), convertId(1L), convertId(3L)),
          (convertId(3L), convertId(3L), convertId(0L)),
          (convertId(4L), convertId(3L), convertId(1L))))
      .toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN)
    
    checkAnswer(relationshipFrame.df, expectedRelDf)
  }

  private def label(label: String): String = s"$LABEL_COLUMN_PREFIX$label"

}
