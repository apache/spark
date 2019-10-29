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
 */

package org.apache.spark.graph.api

import scala.collection.Set

import org.scalatest.Matchers

import org.apache.spark.graph.api.CypherSession.{ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession


class CypherSessionCompanionSuite extends QueryTest with SharedSparkSession with Matchers {

  test("extracts a node dataset from a data frame") {
    val inputDf = spark.createDataFrame(Seq(
      (0L, true)
    )).toDF(ID_COLUMN, ":Person")

    val nodeDatasets = CypherSession.extractNodeDatasets(inputDf)
    nodeDatasets.length shouldBe 2

    val (personNodeDatasets, emptyDatasets) = nodeDatasets.partition(_.labelSet == Set("Person"))

    emptyDatasets.head.ds shouldBe empty

    val personNodeDataset = personNodeDatasets.head
    personNodeDataset.idColumn shouldEqual ID_COLUMN
    personNodeDataset.propertyColumns shouldBe empty
    checkAnswer(personNodeDataset.ds, inputDf)
  }

  test("extracts node datasets with properties and multiple labels") {
    val inputDf = spark.createDataFrame(Seq(
      (0L, true, true, "Alice"),
      (0L, true, false, "Bob")
    )).toDF(ID_COLUMN, ":Person", ":Employee", "name")

    val nodeDatasets = CypherSession.extractNodeDatasets(inputDf)
    nodeDatasets.length shouldBe 4

    nodeDatasets.foreach { nodeDs =>
      nodeDs.propertyColumns should equal(Map("name" -> "name"))
      nodeDs.idColumn should equal(ID_COLUMN)
    }

    nodeDatasets.foreach {
      case nodeDs if nodeDs.labelSet == Set("Person") =>
        checkAnswer(nodeDs.ds, Seq(Row(0L, true, false, "Bob")))
      case nodeDs if nodeDs.labelSet == Set("Person", "Employee") =>
        checkAnswer(nodeDs.ds, Seq(Row(0L, true, true, "Alice")))
      case nodeDs if nodeDs.labelSet == Set("Employee") =>
        nodeDs.ds shouldBe empty
      case nodeDs if nodeDs.labelSet == Set() =>
        nodeDs.ds shouldBe empty
      case other => fail(s"Unexpected label combination: ${other.labelSet}")
    }
  }

  test("extracts a relationship dataset from a data frame") {
    val inputDf = spark.createDataFrame(Seq(
      (0L, 0L, 1L, true)
    )).toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, ":KNOWS")

    val relationshipDatasets = CypherSession.extractRelationshipDatasets(inputDf)
    relationshipDatasets.length shouldBe 1

    val relationshipDataset = relationshipDatasets.head
    relationshipDataset.idColumn shouldEqual ID_COLUMN
    relationshipDataset.sourceIdColumn shouldEqual SOURCE_ID_COLUMN
    relationshipDataset.targetIdColumn shouldEqual TARGET_ID_COLUMN
    relationshipDataset.relationshipType shouldEqual "KNOWS"
    relationshipDataset.propertyColumns shouldBe empty
    checkAnswer(relationshipDataset.ds, inputDf)
  }

  test("extracts relationship datasets with properties and multiple labels") {
    val inputDf = spark.createDataFrame(Seq(
      (0L, 0L, 1L, true, false, "01-01-2000"),
      (1L, 1L, 2L, false, true, "02-01-2000")
    )).toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, ":KNOWS", ":LIKES", "since")

    val relationshipDatasets = CypherSession.extractRelationshipDatasets(inputDf)
    relationshipDatasets.length shouldBe 2

    relationshipDatasets.foreach { relationshipDataset =>
      relationshipDataset.propertyColumns should equal(Map("since" -> "since"))
      relationshipDataset.idColumn shouldEqual ID_COLUMN
      relationshipDataset.sourceIdColumn shouldEqual SOURCE_ID_COLUMN
      relationshipDataset.targetIdColumn shouldEqual TARGET_ID_COLUMN
    }

    relationshipDatasets.foreach {
      case relationshipDs if relationshipDs.relationshipType == "KNOWS" =>
        checkAnswer(relationshipDs.ds, Seq(Row(0L, 0L, 1L, true, false, "01-01-2000")))
      case relationshipDs if relationshipDs.relationshipType == "LIKES" =>
        checkAnswer(relationshipDs.ds, Seq(Row(1L, 1L, 2L, false, true, "02-01-2000")))
      case other => fail(s"Unexpected relationship type: ${other.relationshipType}")
    }
  }

  test("node extraction fails on invalid input") {
    val wrongColumnNameDf = spark.createDataFrame(Seq(
      (0L, true)
    )).toDF(ID_COLUMN, ":Person:Employee")

    val wrongColumnTypeDf = spark.createDataFrame(Seq(
      (0L, "true")
    )).toDF(ID_COLUMN, ":Person")

    val e1 = the[IllegalArgumentException] thrownBy {
      CypherSession.extractNodeDatasets(wrongColumnNameDf)
    }
    e1.getMessage should include("must contain exactly one type")

    val e2 = the[IllegalArgumentException] thrownBy {
      CypherSession.extractNodeDatasets(wrongColumnTypeDf)
    }
    e2.getMessage should include("must be of type BooleanType")
  }

  test("relationship extraction fails on invalid input") {
    val wrongColumnNameDf = spark.createDataFrame(Seq(
      (0L, 0L, 1L, true)
    )).toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, ":KNOWS:LIKES")

    val wrongColumnTypeDf = spark.createDataFrame(Seq(
      (0L, 0L, 1L, "true")
    )).toDF(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, ":KNOWS")

    val e1 = the[IllegalArgumentException] thrownBy {
      CypherSession.extractRelationshipDatasets(wrongColumnNameDf)
    }
    e1.getMessage should include("must contain exactly one type")

    val e2 = the[IllegalArgumentException] thrownBy {
      CypherSession.extractRelationshipDatasets(wrongColumnTypeDf)
    }
    e2.getMessage should include("must be of type BooleanType")
  }
}
