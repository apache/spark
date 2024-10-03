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

package org.apache.spark

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils

import org.apache.spark.internal.config.{DRIVER_LOG_DFS_DIR, DRIVER_LOG_PERSISTTODFS,
  DRIVER_THREAD_DUMP_COLLECTOR_ENABLED,
  EXECUTOR_THREAD_DUMP_COLLECTOR_ENABLED, THREAD_DUMP_COLLECTOR_DIR,
  THREAD_DUMP_COLLECTOR_INTERVAL, THREAD_DUMP_COLLECTOR_OUTPUT_TYPE}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

class ThreadDumpCollectorSuite extends SparkFunSuite {

  private var tmpDir : File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tmpDir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    JavaUtils.deleteRecursively(tmpDir)
  }

  def writeThreadDump(confEntry: ConfigEntry[Boolean], pattern: String, outputType: String)
  : Unit = {
    val conf = new SparkConf()
    conf.set(confEntry, true)
    conf.set(THREAD_DUMP_COLLECTOR_INTERVAL, 1000L)
    conf.set(THREAD_DUMP_COLLECTOR_OUTPUT_TYPE, outputType)

    if (outputType == "LOG") {
      conf.set(DRIVER_LOG_DFS_DIR, tmpDir.getAbsolutePath)
      conf.set(DRIVER_LOG_PERSISTTODFS, true)
    }
    else {
      conf.set(THREAD_DUMP_COLLECTOR_DIR, tmpDir.getAbsolutePath)
    }

    val sc = new SparkContext("local", "ThreadDumpWriteToFileTest", conf)
    Thread.sleep(3000)
    // Run a simple spark application
    sc.parallelize(1 to 10).count()
    sc.stop()
    val threadDumpDir = FileUtils.getFile(tmpDir.getAbsolutePath)
    assert(threadDumpDir.exists())
    val files = threadDumpDir.listFiles().filter(file => !file.isHidden)
    assert(files.length >= 1)
    assert(files.forall { file =>
      Files.lines(file.toPath).anyMatch(line => line.contains(pattern))
    })
  }

  test("Spark executor thread dumps are persisted to dfs") {
    writeThreadDump(EXECUTOR_THREAD_DUMP_COLLECTOR_ENABLED, "executor-heartbeater", "FILE")
  }

  test("Spark driver thread dumps are persisted to dfs") {
    writeThreadDump(DRIVER_THREAD_DUMP_COLLECTOR_ENABLED, "driver-heartbeater", "FILE")
  }

  test("Spark driver thread dumps are persisted to driver log") {
    writeThreadDump(DRIVER_THREAD_DUMP_COLLECTOR_ENABLED, "driver-heartbeater", "LOG")
  }

}
