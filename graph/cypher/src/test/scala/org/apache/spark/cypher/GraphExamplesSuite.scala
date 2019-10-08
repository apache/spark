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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api.{CypherResult, NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.sql.{DataFrame, SaveMode}

class GraphExamplesSuite extends SparkFunSuite with SharedCypherContext {

  test("create PropertyGraph from single NodeFrame") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val nodeFrame: NodeFrame = NodeFrame.create(df = nodeData, "id", Set("Person"))
    val graph: PropertyGraph = cypherSession.createGraph(Array(nodeFrame), Array.empty[RelationshipFrame])
    val result: CypherResult = graph.cypher("MATCH (n) RETURN n")
    result.df.show()
  }

  test("create PropertyGraph from Node- and RelationshipFrames") {
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq((0, 0, 1))).toDF("id", "source", "target")
    val nodeFrame: NodeFrame = NodeFrame.create(nodeData, "id", Set("Person"))
    val relationshipFrame: RelationshipFrame = RelationshipFrame.create(relationshipData, "id", "source", "target", "KNOWS")
    val graph: PropertyGraph = cypherSession.createGraph(Array(nodeFrame), Array(relationshipFrame))
    val result: CypherResult = graph.cypher(
      """
        |MATCH (a:Person)-[r:KNOWS]->(:Person)
        |RETURN a, r""".stripMargin)
    result.df.show()
  }

  test("create PropertyGraph with multiple node and relationship types") {
    val studentDF: DataFrame = spark.createDataFrame(Seq((0, "Alice", 42), (1, "Bob", 23))).toDF("id", "name", "age")
    val teacherDF: DataFrame = spark.createDataFrame(Seq((2, "Eve", "CS"))).toDF("id", "name", "subject")

    val studentNF: NodeFrame = NodeFrame.create(studentDF, "id", Set("Person", "Student"))
    val teacherNF: NodeFrame = NodeFrame.create(teacherDF, "id", Set("Person", "Teacher"))

    val knowsDF: DataFrame = spark.createDataFrame(Seq((0, 0, 1, 1984))).toDF("id", "source", "target", "since")
    val teachesDF: DataFrame = spark.createDataFrame(Seq((1, 2, 1))).toDF("id", "source", "target")

    val knowsRF: RelationshipFrame = RelationshipFrame.create(knowsDF, "id", "source", "target", "KNOWS")
    val teachesRF: RelationshipFrame = RelationshipFrame.create(teachesDF, "id", "source", "target", "TEACHES")

    val graph: PropertyGraph = cypherSession.createGraph(Array(studentNF, teacherNF), Array(knowsRF, teachesRF))
    val result: CypherResult = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    result.df.show()
  }

  test("create PropertyGraph with multiple node and relationship types and explicit property-to-column mappings") {
    val studentDF: DataFrame = spark.createDataFrame(Seq((0, "Alice", 42), (1, "Bob", 23))).toDF("id", "col_name", "col_age")
    val teacherDF: DataFrame = spark.createDataFrame(Seq((2, "Eve", "CS"))).toDF("id", "col_name", "col_subject")

    val studentNF: NodeFrame = NodeFrame(studentDF, "id", Set("Person", "Student"), Map("name" -> "col_name", "age" -> "col_age"))
    val teacherNF: NodeFrame = NodeFrame(teacherDF, "id", Set("Person", "Teacher"), Map("name" -> "col_name", "subject" -> "col_subject"))

    val knowsDF: DataFrame = spark.createDataFrame(Seq((0, 0, 1, 1984))).toDF("id", "source", "target", "col_since")
    val teachesDF: DataFrame = spark.createDataFrame(Seq((1, 2, 1))).toDF("id", "source", "target")

    val knowsRF: RelationshipFrame = RelationshipFrame(knowsDF, "id", "source", "target", "KNOWS", Map("since" -> "col_since"))
    val teachesRF: RelationshipFrame = RelationshipFrame.create(teachesDF, "id", "source", "target", "TEACHES")

    val graph: PropertyGraph = cypherSession.createGraph(Array(studentNF, teacherNF), Array(knowsRF, teachesRF))
    val result: CypherResult = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    result.df.show()
  }

  test("create PropertyGraph with multiple node and relationship types stored in wide tables") {
    val nodeDF: DataFrame = spark.createDataFrame(Seq(
      (0L, true, true, false, Some("Alice"), Some(42), None),
      (1L, true, true, false, Some("Bob"), Some(23), None),
      (2L, true, false, true, Some("Eve"), None, Some("CS")),
    )).toDF("$ID", ":Person", ":Student", ":Teacher", "name", "age", "subject")

    val relsDF: DataFrame = spark.createDataFrame(Seq(
      (0L, 0L, 1L, true, false, Some(1984)),
      (1L, 2L, 1L, false, true, None)
    )).toDF("$ID", "$SOURCE_ID", "$TARGET_ID", ":KNOWS", ":TEACHES", "since")

    val graph: PropertyGraph = cypherSession.createGraph(nodeDF, relsDF)
    val result: CypherResult = graph.cypher("MATCH (n)-[r]->(m) RETURN n, r, m")
    result.df.show()
  }

  test("save and load PropertyGraph") {
    val graph1: PropertyGraph = cypherSession.createGraph(nodes, relationships)
    graph1.nodes.show()
    graph1.write.mode(SaveMode.Overwrite).save("/tmp/my-storage")
    val graph2: PropertyGraph = cypherSession.read.load("/tmp/my-storage")
    graph2.nodes.show()
  }

  test("round trip example using column name conventions") {
    val graph1: PropertyGraph = cypherSession.createGraph(nodes, relationships)
    val graph2: PropertyGraph = cypherSession.createGraph(graph1.nodes, graph1.relationships)
    graph2.nodes.show()
    graph2.relationships.show()
  }

  test("example for retaining user ids") {
    val nodesWithRetainedId = nodes.withColumn("retainedId", nodes.col("$ID"))
    val relsWithRetainedId = relationships.withColumn("retainedId", relationships.col("$ID"))

    cypherSession
      .createGraph(nodesWithRetainedId, relsWithRetainedId)
      .cypher("MATCH (n:Student)-[:STUDY_AT]->(u:University) RETURN n, u").df.show()
  }

  lazy val nodes: DataFrame = spark.createDataFrame(Seq(
    (0L, true, false, Some("Alice"), Some(42), None),
    (1L, true, false, Some("Bob"), Some(23), None),
    (2L, true, false, Some("Carol"), Some(22), None),
    (3L, true, false, Some("Eve"), Some(19), None),
    (4L, false, true, None, None, Some("UC Berkeley")),
    (5L, false, true, None, None, Some("Stanford"))
  )).toDF("$ID", ":Student", ":University", "name", "age", "title")

  lazy val relationships: DataFrame = spark.createDataFrame(Seq(
    (0L, 0L, 1L, true, false),
    (1L, 0L, 3L, true, false),
    (2L, 1L, 3L, true, false),
    (3L, 3L, 0L, true, false),
    (4L, 3L, 1L, true, false),
    (5L, 0L, 4L, false, true),
    (6L, 1L, 4L, false, true),
    (7L, 3L, 4L, false, true),
    (8L, 2L, 5L, false, true),
  )).toDF("$ID", "$SOURCE_ID", "$TARGET_ID", ":KNOWS", ":STUDY_AT")
}
