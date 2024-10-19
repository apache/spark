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
import org.apache.spark.internal.config.{ConfigEntry, History}
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Tests._

class HistoryServerArgumentsSuite extends SparkFunSuite {

  private val logDir = new File("src/test/resources/spark-events")
  private val conf = new SparkConf()
    .set(HISTORY_LOG_DIR, logDir.getAbsolutePath)
    .set(UPDATE_INTERVAL_S, 1L)
    .set(IS_TESTING, true)

  test("No Arguments Parsing") {
    val argStrings = Array.empty[String]
    val hsa = new HistoryServerArguments(conf, argStrings)
    assert(conf.get(HISTORY_LOG_DIR) === logDir.getAbsolutePath)
    assert(conf.get(UPDATE_INTERVAL_S) === 1L)
    assert(conf.get(IS_TESTING).getOrElse(false))
  }

  test("Properties File Arguments Parsing --properties-file") {
    withTempDir { tmpDir =>
      val outFile = File.createTempFile("test-load-spark-properties", "test", tmpDir)
      Files.asCharSink(outFile, UTF_8).write("spark.test.CustomPropertyA blah\n" +
        "spark.test.CustomPropertyB notblah\n")
      val argStrings = Array("--properties-file", outFile.getAbsolutePath)
      val hsa = new HistoryServerArguments(conf, argStrings)
      assert(conf.get("spark.test.CustomPropertyA") === "blah")
      assert(conf.get("spark.test.CustomPropertyB") === "notblah")
    }
  }

  test("SPARK-48471: all history configurations should have documentations") {
    val configs = History.getClass.getDeclaredFields
      .filter(f => classOf[ConfigEntry[_]].isAssignableFrom(f.getType))
      .map { f =>
        f.setAccessible(true)
        f.get(History).asInstanceOf[ConfigEntry[_]]
      }
    configs.foreach { config =>
      assert(config.doc.nonEmpty, s"Config ${config.key} doesn't have documentation")
    }
  }
}
