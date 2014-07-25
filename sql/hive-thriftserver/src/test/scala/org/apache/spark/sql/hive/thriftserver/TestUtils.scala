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

package org.apache.spark.sql.hive.thriftserver

import java.io.{BufferedReader, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException

object TestUtils {
  val timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss")

  def getWarehousePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-warehouse-" +
      timestamp.format(new Date)
  }

  def getMetastorePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-metastore-" +
      timestamp.format(new Date)
  }

  // Dummy function for initialize the log4j properties.
  def init() { }

  // initialize log4j
  try {
    LogUtils.initHiveLog4j()
  } catch {
    case e: LogInitializationException => // Ignore the error.
  }
}

trait TestUtils {
  var process : Process = null
  var outputWriter : PrintWriter = null
  var inputReader : BufferedReader = null
  var errorReader : BufferedReader = null

  def executeQuery(
    cmd: String, outputMessage: String = "OK", timeout: Long = 15000): String = {
    println("Executing: " + cmd + ", expecting output: " + outputMessage)
    outputWriter.write(cmd + "\n")
    outputWriter.flush()
    waitForQuery(timeout, outputMessage)
  }

  protected def waitForQuery(timeout: Long, message: String): String = {
    if (waitForOutput(errorReader, message, timeout)) {
      Thread.sleep(500)
      readOutput()
    } else {
      assert(false, "Didn't find \"" + message + "\" in the output:\n" + readOutput())
      null
    }
  }

  // Wait for the specified str to appear in the output.
  protected def waitForOutput(
    reader: BufferedReader, str: String, timeout: Long = 10000): Boolean = {
    val startTime = System.currentTimeMillis
    var out = ""
    while (!out.contains(str) && System.currentTimeMillis < (startTime + timeout)) {
      out += readFrom(reader)
    }
    out.contains(str)
  }

  // Read stdout output and filter out garbage collection messages.
  protected def readOutput(): String = {
    val output = readFrom(inputReader)
    // Remove GC Messages
    val filteredOutput = output.lines.filterNot(x => x.contains("[GC") || x.contains("[Full GC"))
      .mkString("\n")
    filteredOutput
  }

  protected def readFrom(reader: BufferedReader): String = {
    var out = ""
    var c = 0
    while (reader.ready) {
      c = reader.read()
      out += c.asInstanceOf[Char]
    }
    out
  }

  protected def getDataFile(name: String) = {
    Thread.currentThread().getContextClassLoader.getResource(name)
  }
}
