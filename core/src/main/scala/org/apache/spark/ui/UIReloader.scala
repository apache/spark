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

package org.apache.spark.ui

import org.apache.spark.SparkConf

/**
 * A simple example that reloads a persisted UI independently from a SparkContext
 */
object UIReloader {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: ./bin/spark-class org.apache.spark.ui.UIReloader [log path] [port]")
      System.exit(1)
    }

    val port = if (args.length == 2) args(1).toInt else 14040
    val conf = new SparkConf()
    val ui = new SparkUI(conf, port)
    ui.setAppName("Reloaded Application")
    ui.bind()
    ui.start()
    val success = ui.renderFromPersistedStorage(args(0))
    if (!success) {
      ui.stop()
    }

    println("\nTo exit, type exit or quit.")
    var line = ""
    while (line != "exit" && line != "quit") {
      print("> ")
      line = readLine()
    }
    println("\nReceived signal to exit.")
  }
}
