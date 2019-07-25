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

import org.scalatest.Matchers

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.test.SharedSparkSession

abstract class CypherSessionSuite extends QueryTest with SharedSparkSession with Matchers {

  def cypherSession: CypherSession

  test("save and loads a property graph") {
    val nodeData = spark.createDataFrame(Seq(0L -> "Alice", 1L -> "Bob")).toDF("id", "name")
    val nodeFrame = NodeFrame.create(nodeData, "id", Set("Person"))

    val relationshipData = spark
      .createDataFrame(Seq((0L, 0L, 1L, 1984)))
      .toDF("id", "source", "target", "since")
    val relationshipFrame =
      RelationshipFrame.create(relationshipData, "id", "source", "target", "KNOWS")

    val writeGraph = cypherSession.createGraph(Array(nodeFrame), Array(relationshipFrame))

    withTempDir(file => {
      cypherSession.save(writeGraph, file.getAbsolutePath, SaveMode.Overwrite)
      val readGraph = cypherSession.load(file.getAbsolutePath)

      checkAnswer(readGraph.nodes, writeGraph.nodes)
      checkAnswer(readGraph.relationships, writeGraph.relationships)
    })
  }

}
