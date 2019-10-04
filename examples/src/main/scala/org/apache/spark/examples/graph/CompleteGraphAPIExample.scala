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
package org.apache.spark.examples.graph

// $example on$

import org.apache.spark.cypher.SparkCypherSession
import org.apache.spark.graph.api.{NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.sql.{SaveMode, SparkSession}
// $example off$

object CompleteGraphAPIExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // Load node dfs and edge df
    val userData = spark.read.csv("examples/src/main/resources/mini_yelp/user.csv")
    val businessData = spark.read.csv("examples/src/main/resources/mini_yelp/business.csv")
    val reviewData = spark.read.csv("examples/src/main/resources/mini_yelp/review.csv")

    // Initialise a GraphSession
    val cypherSession = SparkCypherSession.create(spark)

    // Create Node- and RelationshipFrames
    val userNodeFrame = NodeFrame.create(userData, "id", Set("USER"))
    val businessNodeFrame = NodeFrame.create(businessData, "id", Set("BUSINESS"))
    val reviewRelationshipFrame = RelationshipFrame.create(reviewData, "id", "user", "business", "REVIEWS")


    // Create a PropertyGraph
    val graph: PropertyGraph = cypherSession.createGraph(Array(userNodeFrame, businessNodeFrame), Array(reviewRelationshipFrame))

    // Get existing node labels
    val labelSet = graph.schema.labels
    print(s"The graph contains nodes with the following labels: ${labelSet.mkString(",")}")

    val businessNodes = graph.nodeFrame(Array("Business"))
    businessNodes.df.show()

    // Run parameterized cypher query
    val result = graph.cypher(
      """
        |MATCH (a:USER)-[r:REVIEWS]->(b:BUSINESS)
        |WHERE a.name = $name
        |RETURN a, r.rating, b.name""".stripMargin, Map("name" -> "Bob"))

    print("Reviews from Bob")
    result.df.show()

    // Store the PropertyGraph
    val savePath = "examples/src/main/resources/exampleGraph/"
    graph.save(savePath, SaveMode.Overwrite)

    // Load the PropertyGraph
    val importedGraph = cypherSession.load(savePath)


    spark.stop()
  }
}

