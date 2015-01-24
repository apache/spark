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

import java.io.{File, FileOutputStream, OutputStreamWriter}

import scala.io.Source

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

class FsHistoryProviderSuite extends FunSuite with BeforeAndAfter with Matchers with Logging {

  private var testDir: File = null

  private var provider: FsHistoryProvider = null

  before {
    testDir = Utils.createTempDir()
    provider = new FsHistoryProvider(new SparkConf()
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
      .set("spark.history.fs.updateInterval", "0"))
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Parse new and old application logs") {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
      .set("spark.history.fs.updateInterval", "0")
    val provider = new FsHistoryProvider(conf)

    // Write a new-style application log.
    val logFile1 = new File(testDir, "new1")
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1-1", None, 1L, "test"),
      SparkListenerApplicationEnd(2L)
      )

    // Write an unfinished app, new-style.
    val logFile2 = new File(testDir, "new2" + EventLoggingListener.IN_PROGRESS)
    writeFile(logFile2, true, None,
      SparkListenerApplicationStart("app2-2", None, 1L, "test")
      )

    // Write an old-style application log.
    val oldLog = new File(testDir, "old1")
    oldLog.mkdir()
    createEmptyFile(new File(oldLog, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldLog, provider.LOG_PREFIX + "1"), false, None,
      SparkListenerApplicationStart("app3", None, 2L, "test"),
      SparkListenerApplicationEnd(3L)
      )
    createEmptyFile(new File(oldLog, provider.APPLICATION_COMPLETE))

    // Write an unfinished app, old-style.
    val oldLog2 = new File(testDir, "old2")
    oldLog2.mkdir()
    createEmptyFile(new File(oldLog2, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldLog2, provider.LOG_PREFIX + "1"), false, None,
      SparkListenerApplicationStart("app4", None, 2L, "test")
      )

    // Force a reload of data from the log directory, and check that both logs are loaded.
    // Take the opportunity to check that the offset checks work as expected.
    provider.checkForLogs()

    val list = provider.getListing().toSeq
    list should not be (null)
    list.size should be (4)
    list.count(e => e.completed) should be (2)

    list(0) should be (ApplicationHistoryInfo(oldLog.getName(), "app3", 2L, 3L,
      oldLog.lastModified(), "test", true))
    list(1) should be (ApplicationHistoryInfo(logFile1.getName(), "app1-1", 1L, 2L,
      logFile1.lastModified(), "test", true))
    list(2) should be (ApplicationHistoryInfo(oldLog2.getName(), "app4", 2L, -1L,
      oldLog2.lastModified(), "test", false))
     list(3) should be (ApplicationHistoryInfo(logFile2.getName(), "app2-2", 1L, -1L,
      logFile2.lastModified(), "test", false))

    // Make sure the UI can be rendered.
    list.foreach { case info =>
      val appUi = provider.getAppUI(info.id)
      appUi should not be null
    }
  }

  test("Parse legacy logs with compression codec set") {
    val testCodecs = List((classOf[LZFCompressionCodec].getName(), true),
      (classOf[SnappyCompressionCodec].getName(), true),
      ("invalid.codec", false))

    testCodecs.foreach { case (codecName, valid) =>
      val codec = if (valid) CompressionCodec.createCodec(new SparkConf(), codecName) else null
      val logDir = new File(testDir, codecName)
      logDir.mkdir()
      createEmptyFile(new File(logDir, provider.SPARK_VERSION_PREFIX + "1.0"))
      writeFile(new File(logDir, provider.LOG_PREFIX + "1"), false, Option(codec),
        SparkListenerApplicationStart("app2", None, 2L, "test"),
        SparkListenerApplicationEnd(3L)
        )
      createEmptyFile(new File(logDir, provider.COMPRESSION_CODEC_PREFIX + codecName))

      val logPath = new Path(logDir.getAbsolutePath())
      try {
        val (logInput, sparkVersion) = provider.openLegacyEventLog(logPath)
        try {
          Source.fromInputStream(logInput).getLines().toSeq.size should be (2)
        } finally {
          logInput.close()
        }
      } catch {
        case e: IllegalArgumentException =>
          valid should be (false)
      }
    }
  }

  test("SPARK-3697: ignore directories that cannot be read.") {
    val logFile1 = new File(testDir, "new1")
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1-1", None, 1L, "test"),
      SparkListenerApplicationEnd(2L)
      )
    val logFile2 = new File(testDir, "new2")
    writeFile(logFile2, true, None,
      SparkListenerApplicationStart("app1-2", None, 1L, "test"),
      SparkListenerApplicationEnd(2L)
      )
    logFile2.setReadable(false, false)

    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
      .set("spark.history.fs.updateInterval", "0")
    val provider = new FsHistoryProvider(conf)
    provider.checkForLogs()

    val list = provider.getListing().toSeq
    list should not be (null)
    list.size should be (1)
  }

  private def writeFile(file: File, isNewFormat: Boolean, codec: Option[CompressionCodec],
    events: SparkListenerEvent*) = {
    val out =
      if (isNewFormat) {
        EventLoggingListener.initEventLog(new FileOutputStream(file), codec)
      } else {
        val fileStream = new FileOutputStream(file)
        codec.map(_.compressedOutputStream(fileStream)).getOrElse(fileStream)
      }
    val writer = new OutputStreamWriter(out, "UTF-8")
    try {
      events.foreach(e => writer.write(compact(render(JsonProtocol.sparkEventToJson(e))) + "\n"))
    } finally {
      writer.close()
    }
  }

  private def createEmptyFile(file: File) = {
    new FileOutputStream(file).close()
  }

}
