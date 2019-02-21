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

// scalastyle:off println
package org.apache.spark.examples

import java.util.Locale

import org.apache.spark.sql.SparkSession

// Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]

object HDShuffleSpark {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Shuffle Test")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val logFile = "test.md"
    val data = spark.read.text(logFile).as[String]
    var p12 = data.repartition(12, $"value")

    val words = p12.flatMap(value => value.split("\\s+"))
    val p8 = words.repartition(8, $"value")

    var outputDirectory = "/hdinput/"
    p8.rdd.saveAsTextFile(outputDirectory)

    spark.stop()
  }
}
// scalastyle:on println