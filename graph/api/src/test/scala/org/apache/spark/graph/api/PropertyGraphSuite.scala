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

package org.apache.spark.graph.api

import org.apache.spark.graph.api.CypherSession.{
  ID_COLUMN,
  LABEL_COLUMN_PREFIX,
  SOURCE_ID_COLUMN,
  TARGET_ID_COLUMN
}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

abstract class PropertyGraphSuite extends QueryTest with SharedSparkSession {

  test("create graph from NodeFrame") {
    val nodeData = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val nodeFrame = NodeFrame(df = nodeData, idColumn = "id", labelSet = Set("Person"))
    val graph = cypherSession.createGraph(Seq(nodeFrame), Seq.empty)

    val expectedDf = spark
      .createDataFrame(Seq((0L, true, "Alice"), (1L, true, "Bob")))
      .toDF("id", label("Person"), "name")

    checkAnswer(graph.nodes, expectedDf)
  }

  test("create graph from NodeFrame and RelationshipFrame") {
    val nodeData = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val nodeFrame = NodeFrame(df = nodeData, idColumn = "id", labelSet = Set("Person"))

    val relationshipData = spark
      .createDataFrame(Seq((0, 0, 1, 1984)))
      .toDF("id", "source", "target", "since")
    val relationshipFrame = RelationshipFrame(
      relationshipData,
      idColumn = "id",
      sourceIdColumn = "source",
      targetIdColumn = "target",
      relationshipType = "KNOWS")

    val graph = cypherSession.createGraph(Seq(nodeFrame), Seq(relationshipFrame))

    val expectedNodeDf = spark
      .createDataFrame(Seq((0L, true, "Alice"), (1L, true, "Bob")))
      .toDF(ID_COLUMN, label("Person"), "name")

    val expectedRelDf = spark
      .createDataFrame(Seq((0L, 0L, 1L, true, 1984)))
      .toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, label("KNOWS"), "since")

    checkAnswer(graph.nodes, expectedNodeDf)
    checkAnswer(graph.relationships, expectedRelDf)
  }

  test("create graph with multiple node and relationship types") {
    val studentDF = spark
      .createDataFrame(Seq((0, "Alice", 42), (1, "Bob", 23)))
      .toDF("id", "name", "age")
    val teacherDF = spark
      .createDataFrame(Seq((2, "Eve", "CS")))
      .toDF("id", "name", "subject")

    val studentNF =
      NodeFrame(df = studentDF, idColumn = "id", labelSet = Set("Person", "Student"))
    val teacherNF =
      NodeFrame(df = teacherDF, idColumn = "id", labelSet = Set("Person", "Teacher"))

    val knowsDF = spark
      .createDataFrame(Seq((0, 0, 1, 1984)))
      .toDF("id", "source", "target", "since")
    val teachesDF = spark
      .createDataFrame(Seq((1, 2, 1)))
      .toDF("id", "source", "target")

    val knowsRF = RelationshipFrame(
      df = knowsDF,
      idColumn = "id",
      sourceIdColumn = "source",
      targetIdColumn = "target",
      relationshipType = "KNOWS")
    val teachesRF = RelationshipFrame(
      df = teachesDF,
      idColumn = "id",
      sourceIdColumn = "source",
      targetIdColumn = "target",
      relationshipType = "TEACHES")

    val graph = cypherSession.createGraph(Seq(studentNF, teacherNF), Seq(knowsRF, teachesRF))

    val expectedNodeDf = spark
      .createDataFrame(
        Seq(
          (0L, true, true, false, Some("Alice"), Some(42), None),
          (1L, true, true, false, Some("Bob"), Some(23), None),
          (2L, true, false, true, Some("Eve"), None, Some("CS")),
        ))
      .toDF(
        ID_COLUMN,
        label("Person"),
        label("Student"),
        label("Teacher"),
        "name",
        "age",
        "subject")

    val expectedRelDf = spark
      .createDataFrame(
        Seq((0L, 0L, 1L, true, false, Some(1984)), (1L, 2L, 1L, false, true, None)))
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
      .createDataFrame(Seq((0, "Alice", 42), (1, "Bob", 23)))
      .toDF("id", "col_name", "col_age")
    val teacherDF = spark
      .createDataFrame(Seq((2, "Eve", "CS")))
      .toDF("id", "col_name", "col_subject")

    val studentNF = NodeFrame(
      df = studentDF,
      idColumn = "id",
      labelSet = Set("Person", "Student"),
      properties = Map("name" -> "col_name", "age" -> "col_age"))
    val teacherNF = NodeFrame(
      df = teacherDF,
      idColumn = "id",
      labelSet = Set("Person", "Teacher"),
      properties = Map("name" -> "col_name", "subject" -> "col_subject"))

    val knowsDF =
      spark.createDataFrame(Seq((0, 0, 1, 1984))).toDF("id", "source", "target", "col_since")
    val teachesDF = spark.createDataFrame(Seq((1, 2, 1))).toDF("id", "source", "target")

    val knowsRF = RelationshipFrame(
      df = knowsDF,
      idColumn = "id",
      sourceIdColumn = "source",
      targetIdColumn = "target",
      relationshipType = "KNOWS",
      properties = Map("since" -> "col_since"))
    val teachesRF = RelationshipFrame(
      df = teachesDF,
      idColumn = "id",
      sourceIdColumn = "source",
      targetIdColumn = "target",
      relationshipType = "TEACHES")

    val graph = cypherSession.createGraph(Seq(studentNF, teacherNF), Seq(knowsRF, teachesRF))

    val expectedNodeDf = spark
      .createDataFrame(
        Seq(
          (0L, true, true, false, Some("Alice"), Some(42), None),
          (1L, true, true, false, Some("Bob"), Some(23), None),
          (2L, true, false, true, Some("Eve"), None, Some("CS")),
        ))
      .toDF(
        ID_COLUMN,
        label("Person"),
        label("Student"),
        label("Teacher"),
        "name",
        "age",
        "subject")

    val expectedRelDf = spark
      .createDataFrame(
        Seq((0L, 0L, 1L, true, false, Some(1984)), (1L, 2L, 1L, false, true, None)))
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
        (0L, true, true, false, false, Some("Alice"), None, Some(42), None),
        (1L, true, true, false, false, Some("Bob"), None, Some(23), None),
        (2L, true, false, true, false, Some("Carol"), Some("CS"), Some(22), None),
        (3L, true, true, false, false, Some("Eve"), None, Some(19), None),
        (4L, false, false, false, true, None, None, None, Some("UC Berkeley")),
        (5L, false, false, false, true, None, None, None, Some("Stanford"))))
    .toDF(
      ID_COLUMN,
      label("Person"),
      label("Student"),
      label("Teacher"),
      label("University"),
      "name",
      "subject",
      "age",
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

  def cypherSession: CypherSession

  test("select multiple nodes via parent label") {
    val graph = cypherSession.createGraph(nodes, relationships)
    val nodeFrame = graph.nodeFrame(Set("Person"))
    val expectedNodeDf = spark
      .createDataFrame(
        Seq(
          (0L, true, true, false, Some("Alice"), None, Some(42)),
          (1L, true, true, false, Some("Bob"), None, Some(23)),
          (2L, true, false, true, Some("Carol"), Some("CS"), Some(22)),
          (3L, true, true, false, Some("Eve"), None, Some(19))))
      .toDF(
        ID_COLUMN,
        label("Person"),
        label("Student"),
        label("Teacher"),
        "name",
        "subject",
        "age")

    checkAnswer(nodeFrame.df, expectedNodeDf)
  }

  test("select nodes via specific label set") {
    val graph = cypherSession.createGraph(nodes, relationships)
    val nodeFrame = graph.nodeFrame(Set("Teacher"))
    val expectedNodeDf = spark
      .createDataFrame(Seq((2L, true, true, Some("Carol"), Some("CS"))))
      .toDF(ID_COLUMN, label("Person"), label("Teacher"), "name", "subject")

    checkAnswer(nodeFrame.df, expectedNodeDf)
  }

  test("select relationships via type") {
    val graph = cypherSession.createGraph(nodes, relationships)
    val relationshipFrame = graph.relationshipFrame("KNOWS")
    val expectedRelDf = spark
      .createDataFrame(
        Seq(
          (0L, 0L, 1L, true),
          (1L, 0L, 3L, true),
          (2L, 1L, 3L, true),
          (3L, 3L, 0L, true),
          (4L, 3L, 1L, true)))
      .toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, label("KNOWS"))
    checkAnswer(relationshipFrame.df, expectedRelDf)
  }

  private def label(label: String): String = s"$LABEL_COLUMN_PREFIX$label"

}
