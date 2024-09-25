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

package org.apache.spark.sql.connect

import java.util.concurrent.TimeUnit

import scala.io.StdIn
import scala.sys.exit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * A simple main class method to start the spark connect server as a service for client tests
 * using spark-submit:
 * {{{
 *     bin/spark-submit --class org.apache.spark.sql.connect.SimpleSparkConnectService
 * }}}
 * The service can be stopped by receiving a stop command or until the service get killed.
 */
private[sql] object SimpleSparkConnectService {
  private val stopCommand = "q"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
      .set("spark.repl.class.isolatedUri", "true")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext // init spark context
    // scalastyle:off println
    println("Ready for client connections.")
    // scalastyle:on println
    while (true) {
      val code = StdIn.readLine()
      if (code == stopCommand) {
        // scalastyle:off println
        println("No more client connections.")
        // scalastyle:on println
        // Wait for 1 min for the server to stop
        SparkConnectService.stop(Some(1), Some(TimeUnit.MINUTES))
        sparkSession.close()
        exit(0)
      }
    }
  }
}
