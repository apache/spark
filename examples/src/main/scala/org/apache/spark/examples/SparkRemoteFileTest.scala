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

import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

/** Usage: SparkRemoteFileTest [file] */
object SparkRemoteFileTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SparkRemoteFileTest <file>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder()
      .appName("SparkRemoteFileTest")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq(1)).map(_ => {
      val localLocation = SparkFiles.get(args(0))
      println(s"${args(0)} is stored at: $localLocation")
      new File(localLocation).isFile
    })
    val truthCheck = rdd.collect().head
    println(s"Mounting of ${args(0)} was $truthCheck")
    spark.stop()
  }
}
// scalastyle:on println
