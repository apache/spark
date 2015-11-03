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
package org.apache.spark.examples.sql

import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Given a Dataset of Strings, we tokenize by splitting on whitespace and count the number of
 * occurrences of each unique word using `groupBy`.
 */
object DatasetWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DatasetWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val lines: Dataset[String] = Seq("hello world", "say hello to the world").toDS()
    val words: Dataset[(String, Int)] = lines.flatMap(_.split(" ")).map(word => word -> 1)
    val counts: Dataset[(String, Int)] = words.groupBy(_._1).mapGroups {
      case (word, iter) => Iterator(word -> iter.length)
    }

    counts.collect().foreach(println)
  }
}
// scalastyle:on println
