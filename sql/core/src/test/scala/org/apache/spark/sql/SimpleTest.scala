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

package org.apache.spark.sql


object SimpleTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setCallSite("short")
//    spark.sparkContext.setCallSite(new CallSite("shot","long"))

    val df = spark.read
//        .option("parquet.filter.columnindex.enabled", "false")
      .parquet("/Users/lxian/Documents/parquet-playground/part-00000-66712089-3639-4c41-84fb-36790dec7c79-c000.snappy.parquet")

//    df.filter(" _1 = 100000").queryExecution.debug.codegen()
    df.filter(" _1 = 100000").show(false)
//    df.filter(" _1 = 100000").show(false)
  }
}
