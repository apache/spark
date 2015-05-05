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

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.TimeUnit

import scala.io.Source

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, ManualClock, Utils}

class FsHistoryProviderSuite extends FunSuite with BeforeAndAfter with Matchers with Logging {

  private var testDir: File = null

  before {
    testDir = Utils.createTempDir()
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  /** Create a fake log file using the new log format used in Spark 1.3+ */
  private def newLogFile(
      appId: String,
      appAttemptId: Option[String],
      inProgress: Boolean,
      codec: Option[String] = None): File = {
    val ip = if (inProgress) EventLoggingListener.IN_PROGRESS else ""
    val logUri = EventLoggingListener.getLogPath(testDir.toURI, appId, appAttemptId)
    val logPath = new URI(logUri).getPath + ip
    new File(logPath)
  }

  test("Parse new and old application logs") {
    val provider = new FsHistoryProvider(createTestConf())

    // Write a new-style application log.
    val newAppComplete = newLogFile("new1", None, inProgress = false)
    writeFile(newAppComplete, true, None,
      SparkListenerApplicationStart("new-app-complete", None, 1L, "test", None),
      SparkListenerApplicationEnd(5L)
      )

    // Write a new-style application log.
    val newAppCompressedComplete = newLogFile("new1compressed", None, inProgress = false,
      Some("lzf"))
    writeFile(newAppCompressedComplete, true, None,
      SparkListenerApplicationStart("new-app-compressed-complete", None, 1L, "test", None),
      SparkListenerApplicationEnd(4L))

    // Write an unfinished app, new-style.
    val newAppIncomplete = newLogFile("new2", None, inProgress = true)
    writeFile(newAppIncomplete, true, None,
      SparkListenerApplicationStart("new-app-incomplete", None, 1L, "test", None)
      )

    // Write an old-style application log.
    val oldAppComplete = new File(testDir, "old1")
    oldAppComplete.mkdir()
    createEmptyFile(new File(oldAppComplete, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldAppComplete, provider.LOG_PREFIX + "1"), false, None,
      SparkListenerApplicationStart("old-app-complete", None, 2L, "test", None),
      SparkListenerApplicationEnd(3L)
      )
    createEmptyFile(new File(oldAppComplete, provider.APPLICATION_COMPLETE))

    // Check for logs so that we force the older unfinished app to be loaded, to make
    // sure unfinished apps are also sorted correctly.
    provider.checkForLogs()

    // Write an unfinished app, old-style.
    val oldAppIncomplete = new File(testDir, "old2")
    oldAppIncomplete.mkdir()
    createEmptyFile(new File(oldAppIncomplete, provider.SPARK_VERSION_PREFIX + "1.0"))
    writeFile(new File(oldAppIncomplete, provider.LOG_PREFIX + "1"), false, None,
      SparkListenerApplicationStart("old-app-incomplete", None, 2L, "test", None)
      )

    // Force a reload of data from the log directory, and check that both logs are loaded.
    // Take the opportunity to check that the offset checks work as expected.
    updateAndCheck(provider) { list =>
      list.size should be (5)
      list.count(_.attempts.head.completed) should be (3)

      def makeAppInfo(
          id: String,
          name: String,
          start: Long,
          end: Long,
          lastMod: Long,
          user: String,
          completed: Boolean): ApplicationHistoryInfo = {
        ApplicationHistoryInfo(id, name,
          List(ApplicationAttemptInfo(None, start, end, lastMod, user, completed)))
      }

      list(0) should be (makeAppInfo(newAppComplete.getName(), "new-app-complete", 1L, 5L,
        newAppComplete.lastModified(), "test", true))
      list(1) should be (makeAppInfo(newAppCompressedComplete.getName(),
        "new-app-compressed-complete", 1L, 4L, newAppCompressedComplete.lastModified(), "test",
        true))
      list(2) should be (makeAppInfo(oldAppComplete.getName(), "old-app-complete", 2L, 3L,
        oldAppComplete.lastModified(), "test", true))
      list(3) should be (makeAppInfo(oldAppIncomplete.getName(), "old-app-incomplete", 2L, -1L,
        oldAppIncomplete.lastModified(), "test", false))
      list(4) should be (makeAppInfo(newAppIncomplete.getName(), "new-app-incomplete", 1L, -1L,
        newAppIncomplete.lastModified(), "test", false))

      // Make sure the UI can be rendered.
      list.foreach { case info =>
        val appUi = provider.getAppUI(info.id, None)
        appUi should not be null
        appUi should not be None
      }
    }
  }

  test("Parse legacy logs with compression codec set") {
    val provider = new FsHistoryProvider(createTestConf())
    val testCodecs = List((classOf[LZFCompressionCodec].getName(), true),
      (classOf[SnappyCompressionCodec].getName(), true),
      ("invalid.codec", false))

    testCodecs.foreach { case (codecName, valid) =>
      val codec = if (valid) CompressionCodec.createCodec(new SparkConf(), codecName) else null
      val logDir = new File(testDir, codecName)
      logDir.mkdir()
      createEmptyFile(new File(logDir, provider.SPARK_VERSION_PREFIX + "1.0"))
      writeFile(new File(logDir, provider.LOG_PREFIX + "1"), false, Option(codec),
        SparkListenerApplicationStart("app2", None, 2L, "test", None),
        SparkListenerApplicationEnd(3L)
        )
      createEmptyFile(new File(logDir, provider.COMPRESSION_CODEC_PREFIX + codecName))

      val logPath = new Path(logDir.getAbsolutePath())
      try {
        val logInput = provider.openLegacyEventLog(logPath)
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
    val logFile1 = newLogFile("new1", None, inProgress = false)
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1-1", None, 1L, "test", None),
      SparkListenerApplicationEnd(2L)
      )
    val logFile2 = newLogFile("new2", None, inProgress = false)
    writeFile(logFile2, true, None,
      SparkListenerApplicationStart("app1-2", None, 1L, "test", None),
      SparkListenerApplicationEnd(2L)
      )
    logFile2.setReadable(false, false)

    val provider = new FsHistoryProvider(createTestConf())
    updateAndCheck(provider) { list =>
      list.size should be (1)
    }
  }

  test("history file is renamed from inprogress to completed") {
    val provider = new FsHistoryProvider(createTestConf())

    val logFile1 = newLogFile("app1", None, inProgress = true)
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", None),
      SparkListenerApplicationEnd(2L)
    )
    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.head.asInstanceOf[FsApplicationAttemptInfo].logPath should
        endWith(EventLoggingListener.IN_PROGRESS)
    }

    logFile1.renameTo(newLogFile("app1", None, inProgress = false))
    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.head.asInstanceOf[FsApplicationAttemptInfo].logPath should not
        endWith(EventLoggingListener.IN_PROGRESS)
    }
  }

  test("SPARK-5582: empty log directory") {
    val provider = new FsHistoryProvider(createTestConf())

    val logFile1 = newLogFile("app1", None, inProgress = true)
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", None),
      SparkListenerApplicationEnd(2L))

    val oldLog = new File(testDir, "old1")
    oldLog.mkdir()

    provider.checkForLogs()
    val appListAfterRename = provider.getListing()
    appListAfterRename.size should be (1)
  }

  test("apps with multiple attempts") {
    val provider = new FsHistoryProvider(createTestConf())

    val attempt1 = newLogFile("app1", Some("attempt1"), inProgress = false)
    writeFile(attempt1, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1")),
      SparkListenerApplicationEnd(2L)
      )

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (1)
    }

    val attempt2 = newLogFile("app1", Some("attempt2"), inProgress = true)
    writeFile(attempt2, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt2"))
      )

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (2)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }

    val completedAttempt2 = newLogFile("app1", Some("attempt2"), inProgress = false)
    attempt2.delete()
    writeFile(attempt2, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt2")),
      SparkListenerApplicationEnd(4L)
      )

    updateAndCheck(provider) { list =>
      list should not be (null)
      list.size should be (1)
      list.head.attempts.size should be (2)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }

    val app2Attempt1 = newLogFile("app2", Some("attempt1"), inProgress = false)
    writeFile(attempt2, true, None,
      SparkListenerApplicationStart("app2", Some("app2"), 5L, "test", Some("attempt1")),
      SparkListenerApplicationEnd(6L)
      )

    updateAndCheck(provider) { list =>
      list.size should be (2)
      list.head.attempts.size should be (1)
      list.last.attempts.size should be (2)
      list.head.attempts.head.attemptId should be (Some("attempt1"))

      list.foreach { case app =>
        app.attempts.foreach { attempt =>
          val appUi = provider.getAppUI(app.id, attempt.attemptId)
          appUi should not be null
        }
      }

    }
  }

  test("log cleaner") {
    val maxAge = TimeUnit.SECONDS.toMillis(10)
    val clock = new ManualClock(maxAge / 2)
    val provider = new FsHistoryProvider(
      createTestConf().set("spark.history.fs.cleaner.maxAge", s"${maxAge}ms"), clock)

    val log1 = newLogFile("app1", Some("attempt1"), inProgress = false)
    writeFile(log1, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1")),
      SparkListenerApplicationEnd(2L)
      )
    log1.setLastModified(0L)

    val log2 = newLogFile("app1", Some("attempt2"), inProgress = false)
    writeFile(log2, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt2")),
      SparkListenerApplicationEnd(4L)
      )
    log2.setLastModified(clock.getTimeMillis())

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (2)
    }

    // Move the clock forward so log1 exceeds the max age.
    clock.advance(maxAge)

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (1)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }
    assert(!log1.exists())

    // Do the same for the other log.
    clock.advance(maxAge)

    updateAndCheck(provider) { list =>
      list.size should be (0)
    }
    assert(!log2.exists())
  }

  /**
   * Asks the provider to check for logs and calls a function to perform checks on the updated
   * app list. Example:
   *
   *     updateAndCheck(provider) { list =>
   *       // asserts
   *     }
   */
  private def updateAndCheck(provider: FsHistoryProvider)
      (checkFn: Seq[ApplicationHistoryInfo] => Unit): Unit = {
    provider.checkForLogs()
    provider.cleanLogs()
    checkFn(provider.getListing().toSeq)
  }

  private def writeFile(file: File, isNewFormat: Boolean, codec: Option[CompressionCodec],
    events: SparkListenerEvent*) = {
    val fstream = new FileOutputStream(file)
    val cstream = codec.map(_.compressedOutputStream(fstream)).getOrElse(fstream)
    val bstream = new BufferedOutputStream(cstream)
    if (isNewFormat) {
      EventLoggingListener.initEventLog(new FileOutputStream(file))
    }
    val writer = new OutputStreamWriter(bstream, "UTF-8")
    Utils.tryWithSafeFinally {
      events.foreach(e => writer.write(compact(render(JsonProtocol.sparkEventToJson(e))) + "\n"))
    } {
      writer.close()
    }
  }

  private def createEmptyFile(file: File) = {
    new FileOutputStream(file).close()
  }

  private def createTestConf(): SparkConf = {
    new SparkConf().set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
  }

}
