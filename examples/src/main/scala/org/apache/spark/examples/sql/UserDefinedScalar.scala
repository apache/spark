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
package org.apache.spark.examples.sql

// $example on:udf_scalar$
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
// $example off:udf_scalar$

object UserDefinedScalar {

  def main(args: Array[String]): Unit = {
    // $example on:udf_scalar$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL UDF scalar example")
      .getOrCreate()

    // Define and register a zero-argument non-deterministic UDF
    // UDF is deterministic by default, i.e. produces the same result for the same input.
    val random = udf(() => Math.random())
    spark.udf.register("random", random.asNondeterministic())
    spark.sql("SELECT random()").show()
    // +-------+
    // |UDF()  |
    // +-------+
    // |xxxxxxx|
    // +-------+

    // Define and register a one-argument UDF
    val plusOne = udf((x: Int) => x + 1)
    spark.udf.register("plusOne", plusOne)
    spark.sql("SELECT plusOne(5)").show()
    // +------+
    // |UDF(5)|
    // +------+
    // |     6|
    // +------+

    // Define a two-argument UDF and register it with Spark in one step
    spark.udf.register("strLenScala", (_: String).length + (_: Int))
    spark.sql("SELECT strLenScala('test', 1)").show()
    // +--------------------+
    // |strLenScala(test, 1)|
    // +--------------------+
    // |                   5|
    // +--------------------+

    // UDF in a WHERE clause
    spark.udf.register("oneArgFilter", (n: Int) => { n > 5 })
    spark.range(1, 10).createOrReplaceTempView("test")
    spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show()
    // +---+
    // | id|
    // +---+
    // |  6|
    // |  7|
    // |  8|
    // |  9|
    // +---+

    // $example off:udf_scalar$

    spark.stop()
  }
}
