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
import org.apache.spark.graph.api.CypherSession
// $example off$
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object SimpleExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Create node df and edge df
    val nodeData: DataFrame = spark.createDataFrame(Seq((0, "Alice", true), (1, "Bob", true)))
      .toDF(CypherSession.ID_COLUMN, "name", ":Person")
    val relationshipData: DataFrame = spark.createDataFrame(Seq((0, 0, 1, true)))
      .toDF(CypherSession.ID_COLUMN, CypherSession.SOURCE_ID_COLUMN, CypherSession.TARGET_ID_COLUMN, ":KNOWS")

    // Initialise a GraphSession
    val cypherSession = SparkCypherSession.create(spark)

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

