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

import scala.io.StdIn
// $example off$

object CompleteGraphAPIExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val csvConfig = Map("header" -> "true", "delimiter" -> ",", "inferSchema" -> "true")
    val resourcePath = "examples/src/main/resources/movies"
    // Load node dfs and edge df
    val moviesData = spark.read.options(csvConfig).csv(s"$resourcePath/movies.csv")
    val personsData = spark.read.options(csvConfig).csv(s"$resourcePath/persons.csv")
    // TODO: get persons.roles as array type (is not infered correctly)
    val actedInData = spark.read.options(csvConfig).csv(s"$resourcePath/acted_in.csv")

    // Initialise a GraphSession
    val cypherSession = SparkCypherSession.create(spark)

    // Create Node- and RelationshipFrames
    val moviesNodeFrame = NodeFrame.create(moviesData, "id", Set("Movie"))
    val personsNodeFrame = NodeFrame.create(personsData, "id", Set("Person"))
    val actedInRelationshipFrame = RelationshipFrame.create(actedInData, "id", "source", "target", "ACTED_IN")


    // Create a PropertyGraph
    val graph: PropertyGraph = cypherSession.createGraph(Array(moviesNodeFrame, personsNodeFrame), Array(actedInRelationshipFrame))

    // Get existing node labels
    val labelSet = graph.schema.labels
    println(s"The graph contains nodes with the following labels: ${labelSet.mkString(",")}")
    println()
    StdIn.readLine("Press Enter to continue: ")

    val businessNodes = graph.nodeFrame(Array("Movie"))
    businessNodes.df.show()
    StdIn.readLine("Press Enter to continue: ")

    // Run parameterised cypher query
    val parameters = Map("name" -> "Tom Hanks")
    val result = graph.cypher(
      """
        |MATCH (p:Person {name: $name})-[:ACTED_IN]->(movie)
        |RETURN p.name, movie.title""".stripMargin, parameters)
    println(s"Movies with ${parameters.get("name")}")
    result.df.show()

    // Store the PropertyGraph
    val savePath = "examples/src/main/resources/exampleGraph/"
    graph.save(savePath, SaveMode.Overwrite)

    // Load the PropertyGraph
    val importedGraph = cypherSession.load(savePath)


    spark.stop()
  }
}

