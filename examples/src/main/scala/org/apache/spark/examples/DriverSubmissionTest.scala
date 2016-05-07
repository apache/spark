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

import scala.collection.JavaConverters._

import org.apache.spark.util.Utils

/**
 * Prints out environmental information, sleeps, and then exits. Made to
 * test driver submission in the standalone scheduler.
 */
object DriverSubmissionTest {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: DriverSubmissionTest <seconds-to-sleep>")
      System.exit(0)
    }
    val numSecondsToSleep = args(0).toInt

    val env = System.getenv()
    val properties = Utils.getSystemProperties

    println("Environment variables containing SPARK_TEST:")
    env.asScala.filter { case (k, _) => k.contains("SPARK_TEST")}.foreach(println)

    println("System properties containing spark.test:")
    properties.filter { case (k, _) => k.toString.contains("spark.test") }.foreach(println)

    for (i <- 1 until numSecondsToSleep) {
      println(s"Alive for $i out of $numSecondsToSleep seconds")
      Thread.sleep(1000)
    }
  }
}
// scalastyle:on println
