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

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers

import org.apache.spark.SparkConf
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

class FsHistoryProviderSuite extends FunSuite with BeforeAndAfter with Matchers {

  private var testDir: File = null

  private var provider: FsHistoryProvider = null

  before {
    testDir = Files.createTempDir()
    provider = new FsHistoryProvider(new SparkConf()
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
      .set("spark.history.fs.updateInterval", "0"))
  }

  after {
    Utils.getHadoopFileSystem("/").delete(new Path(testDir.getAbsolutePath()), true)
  }

  test("Parse new and old application logs") {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
      .set("spark.history.fs.updateInterval", "0")
    val provider = new FsHistoryProvider(conf)

    // Write a new-style application log.
    val logFile1 = new File(testDir, "app1-1-2-1.0")
    writeFile(logFile1,
      SparkListenerApplicationStart("app1-1", 1L, "test"),
      SparkListenerApplicationEnd(2L)
      )

    // Write an unfinished app, new-style.
    writeFile(new File(testDir, "app2-2-1-1.0.inprogress"),
      SparkListenerApplicationStart("app2-2", 1L, "test")
      )

    // Write an old-style application log.
    val oldLog = new File(testDir, "app3-1234")
    oldLog.mkdir()
    writeFile(new File(oldLog, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldLog, provider.LOG_PREFIX + "1"),
      SparkListenerApplicationStart("app3", 2L, "test"),
      SparkListenerApplicationEnd(3L)
      )
    writeFile(new File(oldLog, provider.APPLICATION_COMPLETE))

    // Write an unfinished app, old-style.
    val oldLog2 = new File(testDir, "app4-1234")
    oldLog2.mkdir()
    writeFile(new File(oldLog2, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldLog2, provider.LOG_PREFIX + "1"),
      SparkListenerApplicationStart("app4", 2L, "test")
      )

    // Force a reload of data from the log directory, and check that both logs are loaded.
    // Take the opportunity to check that the offset checks work as expected.
    provider.checkForLogs()

    val list = provider.getListing()
    list should not be (null)
    list.size should be (2)

    list(0) should be (ApplicationHistoryInfo(oldLog.getName(), "app3", 2L, 3L,
      oldLog.lastModified(), "test"))
    list(1) should be (ApplicationHistoryInfo(logFile1.getName(), "app1-1", 1L, 2L,
      logFile1.lastModified(), "test"))

    // Make sure the UI can be rendered.
    list.foreach { case info =>
      val appUi = provider.getAppUI(info.id)
      appUi should not be null
    }
  }

  test("Parse logs with compression codec set") {
    val testCodecs = List((classOf[LZFCompressionCodec].getName(), true),
      (classOf[SnappyCompressionCodec].getName(), true),
      ("invalid.codec", false))

    testCodecs.foreach { case (codec, valid) =>
      val logDir = new File(testDir, "test")
      logDir.mkdir()
      writeFile(new File(logDir, provider.SPARK_VERSION_PREFIX + "1.0"))
      writeFile(new File(logDir, provider.LOG_PREFIX + "1"),
        SparkListenerApplicationStart("app2", 2L, "test"),
        SparkListenerApplicationEnd(3L)
        )
      writeFile(new File(logDir, provider.COMPRESSION_CODEC_PREFIX + codec))

      val logPath = new Path(logDir.getAbsolutePath())
      val info = provider.loadOldLoggingInfo(logPath)
      if (valid) {
        info.path.toUri().getPath() should be (logPath.toUri().getPath())
        info.sparkVersion should be ("1.0")
        info.compressionCodec should not be (None)
        info.compressionCodec.get.getClass().getName() should be (codec)
        info.applicationComplete should be (false)
      } else {
        info should be (null)
      }
    }
  }

  private def writeFile(file: File, events: SparkListenerEvent*) = {
    val out = new OutputStreamWriter(new FileOutputStream(file), "UTF-8")
    try {
      events.foreach(e => out.write(compact(render(JsonProtocol.sparkEventToJson(e))) + "\n"))
    } finally {
      out.close()
    }
  }

}
