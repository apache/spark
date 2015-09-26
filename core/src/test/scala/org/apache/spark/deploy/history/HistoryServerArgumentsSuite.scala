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
package org.apache.spark.deploy.history

import java.io.File
import java.nio.charset.StandardCharsets._

import com.google.common.io.Files
import org.apache.spark._
import org.apache.spark.util.Utils

class HistoryServerArgumentsSuite extends SparkFunSuite {

  private val logDir = new File("src/test/resources/spark-events")
  private val conf = new SparkConf()
    .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
    .set("spark.history.fs.updateInterval", "1")
    .set("spark.testing", "true")

  test("No Arguments Parsing") {
    val argStrings = Array[String]()
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get("spark.history.fs.logDirectory") === logDir.getAbsolutePath)
    assert(conf.get("spark.history.fs.updateInterval") === "1")
    assert(conf.get("spark.testing") === "true")
  }

  test("Directory Arguments Parsing --dir or -d") {
    val argStrings = Array("--dir", "src/test/resources/spark-events1")
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get("spark.history.fs.logDirectory").equals("src/test/resources/spark-events1"))
  }

  test("Properties File Arguments Parsing --properties-file") {
    val tmpDir = Utils.createTempDir()
    val outFile = File.createTempFile("test-load-spark-properties", "test", tmpDir)
    val argStrings = Array("--properties-file", outFile.getAbsolutePath)
    try {
      System.setProperty("spark.test.DefaultPropertyB", "1")
      Files.write("spark.test.CustomPropertyA true\n" +
        "spark.test.DefaultPropertyB 3\n", outFile, UTF_8)
      val properties = Utils.getPropertiesFromFile(outFile.getAbsolutePath)
      properties
        .filter { case (k, v) => k.startsWith("spark.")}
        .foreach { case (k, v) => sys.props.getOrElseUpdate(k, v)}
      val hsa = new HistoryServerArguments(conf, argStrings)
      assert(conf.getBoolean("spark.test.CustomPropertyA", false))
      assert(conf.getInt("spark.test.DefaultPropertyB", 1) === 3)
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }
}
