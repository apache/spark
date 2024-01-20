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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


/** Basic Spark DataFrame operations */
object SparkDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark DataFrame")
      .getOrCreate()
    import spark.implicits._
    val df = Seq(
      ("Boston", "USA", 0.67),
      ("Dubai", "UAE", 3.1),
      ("Cordoba", "Argentina", 1.39)
    ).toDF("city", "country", "population")
    df.show()
    df.printSchema()
    val df2 = df.withColumn("is_big_city", col("population") > 1)
    df2.show()
    df.filter(col("population") > 1).show()
    spark.stop()
  }
}
// scalastyle:on println