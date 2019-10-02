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
// $example off$
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object SimpleDemo {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // Initialise a GraphSession
    val cypherSession = SparkCypherSession.create(spark)

    // Create node df and edge df
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob"))
      .toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq((0, 0, 1)))
      .toDF("id", "source", "target")

    // Create a PropertyGraph
    val graph = cypherSession.createGraph(nodeData, relationshipData)

    // Run our first query
    val result = graph.cypher(
      """
        |MATCH (a:Person)-[r:KNOWS]->(:Person)
        |RETURN a, r""".stripMargin)

    // Print the result
    result.df.show()


    spark.stop()
  }
}

