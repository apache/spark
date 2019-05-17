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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.{Date, Locale}
import java.util.concurrent.TimeUnit
import java.util.zip.{ZipInputStream, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.google.common.io.{ByteStreams, Files}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.hdfs.{DFSInputStream, DistributedFileSystem}
import org.apache.hadoop.security.AccessControlException
import org.json4s.jackson.JsonMethods._
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{doThrow, mock, spy, verify, when}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DRIVER_LOG_DFS_DIR
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.UI.{ADMIN_ACLS, ADMIN_ACLS_GROUPS, USER_GROUPS_MAPPING}
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.security.GroupMappingServiceProvider
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.util.{Clock, JsonProtocol, ManualClock, Utils}
import org.apache.spark.util.logging.DriverLogger

class FsHistoryProviderSuite extends SparkFunSuite with Matchers with Logging {

  private var testDir: File = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    testDir = Utils.createTempDir(namePrefix = s"a b%20c+d")
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(testDir)
    } finally {
      super.afterEach()
    }
  }

  /** Create a fake log file using the new log format used in Spark 1.3+ */
  private def newLogFile(
      appId: String,
      appAttemptId: Option[String],
      inProgress: Boolean,
      codec: Option[String] = None): File = {
    val ip = if (inProgress) EventLoggingListener.IN_PROGRESS else ""
    val logUri = EventLoggingListener.getLogPath(testDir.toURI, appId, appAttemptId)
    val logPath = new Path(logUri).toUri.getPath + ip
    new File(logPath)
  }

  Seq(true, false).foreach { inMemory =>
    test(s"Parse application logs (inMemory = $inMemory)") {
      testAppLogParsing(inMemory)
    }
  }

  private def testAppLogParsing(inMemory: Boolean) {
    val clock = new ManualClock(12345678)
    val provider = new FsHistoryProvider(createTestConf(inMemory = inMemory), clock)

    // Write a new-style application log.
    val newAppComplete = newLogFile("new1", None, inProgress = false)
    writeFile(newAppComplete, true, None,
      SparkListenerApplicationStart(newAppComplete.getName(), Some("new-app-complete"), 1L, "test",
        None),
      SparkListenerApplicationEnd(5L)
      )

    // Write a new-style application log.
    val newAppCompressedComplete = newLogFile("new1compressed", None, inProgress = false,
      Some("lzf"))
    writeFile(newAppCompressedComplete, true, None,
      SparkListenerApplicationStart(newAppCompressedComplete.getName(), Some("new-complete-lzf"),
        1L, "test", None),
      SparkListenerApplicationEnd(4L))

    // Write an unfinished app, new-style.
    val newAppIncomplete = newLogFile("new2", None, inProgress = true)
    writeFile(newAppIncomplete, true, None,
      SparkListenerApplicationStart(newAppIncomplete.getName(), Some("new-incomplete"), 1L, "test",
        None)
      )

    // Force a reload of data from the log directory, and check that logs are loaded.
    // Take the opportunity to check that the offset checks work as expected.
    updateAndCheck(provider) { list =>
      list.size should be (3)
      list.count(_.attempts.head.completed) should be (2)

      def makeAppInfo(
          id: String,
          name: String,
          start: Long,
          end: Long,
          lastMod: Long,
          user: String,
          completed: Boolean): ApplicationInfo = {

        val duration = if (end > 0) end - start else 0
        new ApplicationInfo(id, name, None, None, None, None,
          List(ApplicationAttemptInfo(None, new Date(start),
            new Date(end), new Date(lastMod), duration, user, completed, "")))
      }

      // For completed files, lastUpdated would be lastModified time.
      list(0) should be (makeAppInfo("new-app-complete", newAppComplete.getName(), 1L, 5L,
        newAppComplete.lastModified(), "test", true))
      list(1) should be (makeAppInfo("new-complete-lzf", newAppCompressedComplete.getName(),
        1L, 4L, newAppCompressedComplete.lastModified(), "test", true))

      // For Inprogress files, lastUpdated would be current loading time.
      list(2) should be (makeAppInfo("new-incomplete", newAppIncomplete.getName(), 1L, -1L,
        clock.getTimeMillis(), "test", false))

      // Make sure the UI can be rendered.
      list.foreach { info =>
        val appUi = provider.getAppUI(info.id, None)
        appUi should not be null
        appUi should not be None
      }
    }
  }

  test("SPARK-3697: ignore files that cannot be read.") {
    // setReadable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)

    class TestFsHistoryProvider extends FsHistoryProvider(createTestConf()) {
      var mergeApplicationListingCall = 0
      override protected def mergeApplicationListing(
          fileStatus: FileStatus,
          lastSeen: Long,
          enableSkipToEnd: Boolean): Unit = {
        super.mergeApplicationListing(fileStatus, lastSeen, enableSkipToEnd)
        mergeApplicationListingCall += 1
      }
    }
    val provider = new TestFsHistoryProvider

    val logFile1 = newLogFile("new1", None, inProgress = false)
    writeFile(logFile1, true, None,
      SparkListenerApplicationStart("app1-1", Some("app1-1"), 1L, "test", None),
      SparkListenerApplicationEnd(2L)
      )
    val logFile2 = newLogFile("new2", None, inProgress = false)
    writeFile(logFile2, true, None,
      SparkListenerApplicationStart("app1-2", Some("app1-2"), 1L, "test", None),
      SparkListenerApplicationEnd(2L)
      )
    logFile2.setReadable(false, false)

    updateAndCheck(provider) { list =>
      list.size should be (1)
    }

    provider.mergeApplicationListingCall should be (1)
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
      provider.getAttempt("app1", None).logPath should endWith(EventLoggingListener.IN_PROGRESS)
    }

    logFile1.renameTo(newLogFile("app1", None, inProgress = false))
    updateAndCheck(provider) { list =>
      list.size should be (1)
      provider.getAttempt("app1", None).logPath should not endWith(EventLoggingListener.IN_PROGRESS)
    }
  }

  test("Parse logs that application is not started") {
    val provider = new FsHistoryProvider(createTestConf())

    val logFile1 = newLogFile("app1", None, inProgress = true)
    writeFile(logFile1, true, None,
      SparkListenerLogStart("1.4")
    )
    updateAndCheck(provider) { list =>
      list.size should be (0)
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

  test("apps with multiple attempts with order") {
    val provider = new FsHistoryProvider(createTestConf())

    val attempt1 = newLogFile("app1", Some("attempt1"), inProgress = true)
    writeFile(attempt1, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1"))
      )

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (1)
    }

    val attempt2 = newLogFile("app1", Some("attempt2"), inProgress = true)
    writeFile(attempt2, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 2L, "test", Some("attempt2"))
      )

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (2)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }

    val attempt3 = newLogFile("app1", Some("attempt3"), inProgress = false)
    writeFile(attempt3, true, None,
      SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt3")),
      SparkListenerApplicationEnd(4L)
      )

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (3)
      list.head.attempts.head.attemptId should be (Some("attempt3"))
    }

    val app2Attempt1 = newLogFile("app2", Some("attempt1"), inProgress = false)
    writeFile(app2Attempt1, true, None,
      SparkListenerApplicationStart("app2", Some("app2"), 5L, "test", Some("attempt1")),
      SparkListenerApplicationEnd(6L)
      )

    updateAndCheck(provider) { list =>
      list.size should be (2)
      list.head.attempts.size should be (1)
      list.last.attempts.size should be (3)
      list.head.attempts.head.attemptId should be (Some("attempt1"))

      list.foreach { app =>
        app.attempts.foreach { attempt =>
          val appUi = provider.getAppUI(app.id, attempt.attemptId)
          appUi should not be null
        }
      }

    }
  }

  test("log urls without customization") {
    val conf = createTestConf()
    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      execInfo -> execInfo.logUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected)
  }

  test("custom log urls, including FILE_NAME") {
    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = true))

    // some of available attributes are not used in pattern which should be OK

    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      val attr = execInfo.attributes
      val newLogUrlMap = attr("LOG_FILES").split(",").map { file =>
        val newLogUrl = getExpectedExecutorLogUrl(attr, Some(file))
        file -> newLogUrl
      }.toMap

      execInfo -> newLogUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected)
  }

  test("custom log urls, excluding FILE_NAME") {
    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = false))

    // some of available attributes are not used in pattern which should be OK

    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      val attr = execInfo.attributes
      val newLogUrl = getExpectedExecutorLogUrl(attr, None)

      execInfo -> Map("log" -> newLogUrl)
    }.toMap

    testHandlingExecutorLogUrl(conf, expected)
  }

  test("custom log urls with invalid attribute") {
    // Here we are referring {{NON_EXISTING}} which is not available in attributes,
    // which Spark will fail back to provide origin log url with warning log.

    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = true) +
        "/{{NON_EXISTING}}")

    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      execInfo -> execInfo.logUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected)
  }

  test("custom log urls, LOG_FILES not available while FILE_NAME is specified") {
    // For this case Spark will fail back to provide origin log url with warning log.

    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = true))

    val executorInfos = (1 to 5).map(
      createTestExecutorInfo("app1", "user1", _, includingLogFiles = false))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      execInfo -> execInfo.logUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected)
  }

  test("custom log urls, app not finished, applyIncompleteApplication: true") {
    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = true))
      .set(APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP, true)

    // ensure custom log urls are applied to incomplete application

    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      val attr = execInfo.attributes
      val newLogUrlMap = attr("LOG_FILES").split(",").map { file =>
        val newLogUrl = getExpectedExecutorLogUrl(attr, Some(file))
        file -> newLogUrl
      }.toMap

      execInfo -> newLogUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected, isCompletedApp = false)
  }

  test("custom log urls, app not finished, applyIncompleteApplication: false") {
    val conf = createTestConf()
      .set(CUSTOM_EXECUTOR_LOG_URL, getCustomExecutorLogUrl(includeFileName = true))
      .set(APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP, false)

    // ensure custom log urls are NOT applied to incomplete application

    val executorInfos = (1 to 5).map(createTestExecutorInfo("app1", "user1", _))

    val expected: Map[ExecutorInfo, Map[String, String]] = executorInfos.map { execInfo =>
      execInfo -> execInfo.logUrlMap
    }.toMap

    testHandlingExecutorLogUrl(conf, expected, isCompletedApp = false)
  }

  private def getCustomExecutorLogUrl(includeFileName: Boolean): String = {
    val baseUrl = "http://newhost:9999/logs/clusters/{{CLUSTER_ID}}/users/{{USER}}/containers/" +
      "{{CONTAINER_ID}}"
    if (includeFileName) baseUrl + "/{{FILE_NAME}}" else baseUrl
  }

  private def getExpectedExecutorLogUrl(
      attributes: Map[String, String],
      fileName: Option[String]): String = {
    val baseUrl = s"http://newhost:9999/logs/clusters/${attributes("CLUSTER_ID")}" +
      s"/users/${attributes("USER")}/containers/${attributes("CONTAINER_ID")}"

    fileName match {
      case Some(file) => baseUrl + s"/$file"
      case None => baseUrl
    }
  }

  private def testHandlingExecutorLogUrl(
      conf: SparkConf,
      expectedLogUrlMap: Map[ExecutorInfo, Map[String, String]],
      isCompletedApp: Boolean = true): Unit = {
    val provider = new FsHistoryProvider(conf)

    val attempt1 = newLogFile("app1", Some("attempt1"), inProgress = true)

    val executorAddedEvents = expectedLogUrlMap.keys.zipWithIndex.map { case (execInfo, idx) =>
      SparkListenerExecutorAdded(1 + idx, s"exec$idx", execInfo)
    }.toList.sortBy(_.time)
    val allEvents = List(SparkListenerApplicationStart("app1", Some("app1"), 1L,
      "test", Some("attempt1"))) ++ executorAddedEvents ++
      (if (isCompletedApp) List(SparkListenerApplicationEnd(1000L)) else Seq())

    writeFile(attempt1, true, None, allEvents: _*)

    updateAndCheck(provider) { list =>
      list.size should be (1)
      list.head.attempts.size should be (1)

      list.foreach { app =>
        app.attempts.foreach { attempt =>
          val appUi = provider.getAppUI(app.id, attempt.attemptId)
          appUi should not be null
          val executors = appUi.get.ui.store.executorList(false).iterator
          executors should not be null

          val iterForExpectation = expectedLogUrlMap.iterator

          var executorCount = 0
          while (executors.hasNext) {
            val executor = executors.next()
            val (expectedExecInfo, expectedLogs) = iterForExpectation.next()

            executor.hostPort should startWith(expectedExecInfo.executorHost)
            executor.executorLogs should be(expectedLogs)

            executorCount += 1
          }

          executorCount should be (expectedLogUrlMap.size)
        }
      }
    }
  }

  test("log cleaner") {
    val maxAge = TimeUnit.SECONDS.toMillis(10)
    val clock = new ManualClock(maxAge / 2)
    val provider = new FsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S.key, s"${maxAge}ms"), clock)

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

  test("should not clean inprogress application with lastUpdated time less than maxTime") {
    val firstFileModifiedTime = TimeUnit.DAYS.toMillis(1)
    val secondFileModifiedTime = TimeUnit.DAYS.toMillis(6)
    val maxAge = TimeUnit.DAYS.toMillis(7)
    val clock = new ManualClock(0)
    val provider = new FsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S, maxAge / 1000), clock)
    val log = newLogFile("inProgressApp1", None, inProgress = true)
    writeFile(log, true, None,
      SparkListenerApplicationStart(
        "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1"))
    )
    clock.setTime(firstFileModifiedTime)
    log.setLastModified(clock.getTimeMillis())
    provider.checkForLogs()
    writeFile(log, true, None,
      SparkListenerApplicationStart(
        "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1")),
      SparkListenerJobStart(0, 1L, Nil, null)
    )

    clock.setTime(secondFileModifiedTime)
    log.setLastModified(clock.getTimeMillis())
    provider.checkForLogs()
    clock.setTime(TimeUnit.DAYS.toMillis(10))
    writeFile(log, true, None,
      SparkListenerApplicationStart(
        "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1")),
      SparkListenerJobStart(0, 1L, Nil, null),
      SparkListenerJobEnd(0, 1L, JobSucceeded)
    )
    log.setLastModified(clock.getTimeMillis())
    provider.checkForLogs()
    // This should not trigger any cleanup
    updateAndCheck(provider) { list =>
      list.size should be(1)
    }
  }

  test("log cleaner for inProgress files") {
    val firstFileModifiedTime = TimeUnit.SECONDS.toMillis(10)
    val secondFileModifiedTime = TimeUnit.SECONDS.toMillis(20)
    val maxAge = TimeUnit.SECONDS.toMillis(40)
    val clock = new ManualClock(0)
    val provider = new FsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S.key, s"${maxAge}ms"), clock)

    val log1 = newLogFile("inProgressApp1", None, inProgress = true)
    writeFile(log1, true, None,
      SparkListenerApplicationStart(
        "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1"))
    )

    clock.setTime(firstFileModifiedTime)
    provider.checkForLogs()

    val log2 = newLogFile("inProgressApp2", None, inProgress = true)
    writeFile(log2, true, None,
      SparkListenerApplicationStart(
        "inProgressApp2", Some("inProgressApp2"), 23L, "test2", Some("attempt2"))
    )

    clock.setTime(secondFileModifiedTime)
    provider.checkForLogs()

    // This should not trigger any cleanup
    updateAndCheck(provider) { list =>
      list.size should be(2)
    }

    // Should trigger cleanup for first file but not second one
    clock.setTime(firstFileModifiedTime + maxAge + 1)
    updateAndCheck(provider) { list =>
      list.size should be(1)
    }
    assert(!log1.exists())
    assert(log2.exists())

    // Should cleanup the second file as well.
    clock.setTime(secondFileModifiedTime + maxAge + 1)
    updateAndCheck(provider) { list =>
      list.size should be(0)
    }
    assert(!log1.exists())
    assert(!log2.exists())
  }

  test("Event log copy") {
    val provider = new FsHistoryProvider(createTestConf())
    val logs = (1 to 2).map { i =>
      val log = newLogFile("downloadApp1", Some(s"attempt$i"), inProgress = false)
      writeFile(log, true, None,
        SparkListenerApplicationStart(
          "downloadApp1", Some("downloadApp1"), 5000L * i, "test", Some(s"attempt$i")),
        SparkListenerApplicationEnd(5001L * i)
      )
      log
    }
    provider.checkForLogs()

    (1 to 2).foreach { i =>
      val underlyingStream = new ByteArrayOutputStream()
      val outputStream = new ZipOutputStream(underlyingStream)
      provider.writeEventLogs("downloadApp1", Some(s"attempt$i"), outputStream)
      outputStream.close()
      val inputStream = new ZipInputStream(new ByteArrayInputStream(underlyingStream.toByteArray))
      var totalEntries = 0
      var entry = inputStream.getNextEntry
      entry should not be null
      while (entry != null) {
        val actual = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
        val expected =
          Files.toString(logs.find(_.getName == entry.getName).get, StandardCharsets.UTF_8)
        actual should be (expected)
        totalEntries += 1
        entry = inputStream.getNextEntry
      }
      totalEntries should be (1)
      inputStream.close()
    }
  }

  test("driver log cleaner") {
    val firstFileModifiedTime = TimeUnit.SECONDS.toMillis(10)
    val secondFileModifiedTime = TimeUnit.SECONDS.toMillis(20)
    val maxAge = TimeUnit.SECONDS.toSeconds(40)
    val clock = new ManualClock(0)
    val testConf = new SparkConf()
    testConf.set(HISTORY_LOG_DIR, Utils.createTempDir(namePrefix = "eventLog").getAbsolutePath())
    testConf.set(DRIVER_LOG_DFS_DIR, testDir.getAbsolutePath())
    testConf.set(DRIVER_LOG_CLEANER_ENABLED, true)
    testConf.set(DRIVER_LOG_CLEANER_INTERVAL, maxAge / 4)
    testConf.set(MAX_DRIVER_LOG_AGE_S, maxAge)
    val provider = new FsHistoryProvider(testConf, clock)

    val log1 = FileUtils.getFile(testDir, "1" + DriverLogger.DRIVER_LOG_FILE_SUFFIX)
    createEmptyFile(log1)
    clock.setTime(firstFileModifiedTime)
    log1.setLastModified(clock.getTimeMillis())
    provider.cleanDriverLogs()

    val log2 = FileUtils.getFile(testDir, "2" + DriverLogger.DRIVER_LOG_FILE_SUFFIX)
    createEmptyFile(log2)
    val log3 = FileUtils.getFile(testDir, "3" + DriverLogger.DRIVER_LOG_FILE_SUFFIX)
    createEmptyFile(log3)
    clock.setTime(secondFileModifiedTime)
    log2.setLastModified(clock.getTimeMillis())
    log3.setLastModified(clock.getTimeMillis())
    // This should not trigger any cleanup
    provider.cleanDriverLogs()
    provider.listing.view(classOf[LogInfo]).iterator().asScala.toSeq.size should be(3)

    // Should trigger cleanup for first file but not second one
    clock.setTime(firstFileModifiedTime + TimeUnit.SECONDS.toMillis(maxAge) + 1)
    provider.cleanDriverLogs()
    provider.listing.view(classOf[LogInfo]).iterator().asScala.toSeq.size should be(2)
    assert(!log1.exists())
    assert(log2.exists())
    assert(log3.exists())

    // Update the third file length while keeping the original modified time
    Files.write("Add logs to file".getBytes(), log3)
    log3.setLastModified(secondFileModifiedTime)
    // Should cleanup the second file but not the third file, as filelength changed.
    clock.setTime(secondFileModifiedTime + TimeUnit.SECONDS.toMillis(maxAge) + 1)
    provider.cleanDriverLogs()
    provider.listing.view(classOf[LogInfo]).iterator().asScala.toSeq.size should be(1)
    assert(!log1.exists())
    assert(!log2.exists())
    assert(log3.exists())

    // Should cleanup the third file as well.
    clock.setTime(secondFileModifiedTime + 2 * TimeUnit.SECONDS.toMillis(maxAge) + 2)
    provider.cleanDriverLogs()
    provider.listing.view(classOf[LogInfo]).iterator().asScala.toSeq.size should be(0)
    assert(!log3.exists())
  }

  test("SPARK-8372: new logs with no app ID are ignored") {
    val provider = new FsHistoryProvider(createTestConf())

    // Write a new log file without an app id, to make sure it's ignored.
    val logFile1 = newLogFile("app1", None, inProgress = true)
    writeFile(logFile1, true, None,
      SparkListenerLogStart("1.4")
    )

    updateAndCheck(provider) { list =>
      list.size should be (0)
    }
  }

  test("provider correctly checks whether fs is in safe mode") {
    val provider = spy(new FsHistoryProvider(createTestConf()))
    val dfs = mock(classOf[DistributedFileSystem])
    // Asserts that safe mode is false because we can't really control the return value of the mock,
    // since the API is different between hadoop 1 and 2.
    assert(!provider.isFsInSafeMode(dfs))
  }

  test("provider waits for safe mode to finish before initializing") {
    val clock = new ManualClock()
    val provider = new SafeModeTestProvider(createTestConf(), clock)
    val initThread = provider.initialize()
    try {
      provider.getConfig().keys should contain ("HDFS State")

      clock.setTime(5000)
      provider.getConfig().keys should contain ("HDFS State")

      provider.inSafeMode = false
      clock.setTime(10000)

      eventually(timeout(3.second), interval(10.milliseconds)) {
        provider.getConfig().keys should not contain ("HDFS State")
      }
    } finally {
      provider.stop()
    }
  }

  testRetry("provider reports error after FS leaves safe mode") {
    testDir.delete()
    val clock = new ManualClock()
    val provider = new SafeModeTestProvider(createTestConf(), clock)
    val errorHandler = mock(classOf[Thread.UncaughtExceptionHandler])
    provider.startSafeModeCheckThread(Some(errorHandler))
    try {
      provider.inSafeMode = false
      clock.setTime(10000)

      eventually(timeout(3.second), interval(10.milliseconds)) {
        verify(errorHandler).uncaughtException(any(), any())
      }
    } finally {
      provider.stop()
    }
  }

  test("ignore hidden files") {

    // FsHistoryProvider should ignore hidden files.  (It even writes out a hidden file itself
    // that should be ignored).

    // write out one totally bogus hidden file
    val hiddenGarbageFile = new File(testDir, ".garbage")
    val out = new PrintWriter(hiddenGarbageFile)
    // scalastyle:off println
    out.println("GARBAGE")
    // scalastyle:on println
    out.close()

    // also write out one real event log file, but since its a hidden file, we shouldn't read it
    val tmpNewAppFile = newLogFile("hidden", None, inProgress = false)
    val hiddenNewAppFile = new File(tmpNewAppFile.getParentFile, "." + tmpNewAppFile.getName)
    tmpNewAppFile.renameTo(hiddenNewAppFile)

    // and write one real file, which should still get picked up just fine
    val newAppComplete = newLogFile("real-app", None, inProgress = false)
    writeFile(newAppComplete, true, None,
      SparkListenerApplicationStart(newAppComplete.getName(), Some("new-app-complete"), 1L, "test",
        None),
      SparkListenerApplicationEnd(5L)
    )

    val provider = new FsHistoryProvider(createTestConf())
    updateAndCheck(provider) { list =>
      list.size should be (1)
      list(0).name should be ("real-app")
    }
  }

  test("support history server ui admin acls") {
    def createAndCheck(conf: SparkConf, properties: (String, String)*)
      (checkFn: SecurityManager => Unit): Unit = {
      // Empty the testDir for each test.
      if (testDir.exists() && testDir.isDirectory) {
        testDir.listFiles().foreach { f => if (f.isFile) f.delete() }
      }

      var provider: FsHistoryProvider = null
      try {
        provider = new FsHistoryProvider(conf)
        val log = newLogFile("app1", Some("attempt1"), inProgress = false)
        writeFile(log, true, None,
          SparkListenerApplicationStart("app1", Some("app1"), System.currentTimeMillis(),
            "test", Some("attempt1")),
          SparkListenerEnvironmentUpdate(Map(
            "Spark Properties" -> properties.toSeq,
            "Hadoop Properties" -> Seq.empty,
            "JVM Information" -> Seq.empty,
            "System Properties" -> Seq.empty,
            "Classpath Entries" -> Seq.empty
          )),
          SparkListenerApplicationEnd(System.currentTimeMillis()))

        provider.checkForLogs()
        val appUi = provider.getAppUI("app1", Some("attempt1"))

        assert(appUi.nonEmpty)
        val securityManager = appUi.get.ui.securityManager
        checkFn(securityManager)
      } finally {
        if (provider != null) {
          provider.stop()
        }
      }
    }

    // Test both history ui admin acls and application acls are configured.
    val conf1 = createTestConf()
      .set(HISTORY_SERVER_UI_ACLS_ENABLE, true)
      .set(HISTORY_SERVER_UI_ADMIN_ACLS, Seq("user1", "user2"))
      .set(HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS, Seq("group1"))
      .set(USER_GROUPS_MAPPING, classOf[TestGroupsMappingProvider].getName)

    createAndCheck(conf1, (ADMIN_ACLS.key, "user"), (ADMIN_ACLS_GROUPS.key, "group")) {
      securityManager =>
        // Test whether user has permission to access UI.
        securityManager.checkUIViewPermissions("user1") should be (true)
        securityManager.checkUIViewPermissions("user2") should be (true)
        securityManager.checkUIViewPermissions("user") should be (true)
        securityManager.checkUIViewPermissions("abc") should be (false)

        // Test whether user with admin group has permission to access UI.
        securityManager.checkUIViewPermissions("user3") should be (true)
        securityManager.checkUIViewPermissions("user4") should be (true)
        securityManager.checkUIViewPermissions("user5") should be (true)
        securityManager.checkUIViewPermissions("user6") should be (false)
    }

    // Test only history ui admin acls are configured.
    val conf2 = createTestConf()
      .set(HISTORY_SERVER_UI_ACLS_ENABLE, true)
      .set(HISTORY_SERVER_UI_ADMIN_ACLS, Seq("user1", "user2"))
      .set(HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS, Seq("group1"))
      .set(USER_GROUPS_MAPPING, classOf[TestGroupsMappingProvider].getName)
    createAndCheck(conf2) { securityManager =>
      // Test whether user has permission to access UI.
      securityManager.checkUIViewPermissions("user1") should be (true)
      securityManager.checkUIViewPermissions("user2") should be (true)
      // Check the unknown "user" should return false
      securityManager.checkUIViewPermissions("user") should be (false)

      // Test whether user with admin group has permission to access UI.
      securityManager.checkUIViewPermissions("user3") should be (true)
      securityManager.checkUIViewPermissions("user4") should be (true)
      // Check the "user5" without mapping relation should return false
      securityManager.checkUIViewPermissions("user5") should be (false)
    }

    // Test neither history ui admin acls nor application acls are configured.
     val conf3 = createTestConf()
      .set(HISTORY_SERVER_UI_ACLS_ENABLE, true)
      .set(USER_GROUPS_MAPPING, classOf[TestGroupsMappingProvider].getName)
    createAndCheck(conf3) { securityManager =>
      // Test whether user has permission to access UI.
      securityManager.checkUIViewPermissions("user1") should be (false)
      securityManager.checkUIViewPermissions("user2") should be (false)
      securityManager.checkUIViewPermissions("user") should be (false)

      // Test whether user with admin group has permission to access UI.
      // Check should be failed since we don't have acl group settings.
      securityManager.checkUIViewPermissions("user3") should be (false)
      securityManager.checkUIViewPermissions("user4") should be (false)
      securityManager.checkUIViewPermissions("user5") should be (false)
    }
  }

  test("mismatched version discards old listing") {
    val conf = createTestConf()
    val oldProvider = new FsHistoryProvider(conf)

    val logFile1 = newLogFile("app1", None, inProgress = false)
    writeFile(logFile1, true, None,
      SparkListenerLogStart("2.3"),
      SparkListenerApplicationStart("test", Some("test"), 1L, "test", None),
      SparkListenerApplicationEnd(5L)
    )

    updateAndCheck(oldProvider) { list =>
      list.size should be (1)
    }
    assert(oldProvider.listing.count(classOf[ApplicationInfoWrapper]) === 1)

    // Manually overwrite the version in the listing db; this should cause the new provider to
    // discard all data because the versions don't match.
    val meta = new FsHistoryProviderMetadata(FsHistoryProvider.CURRENT_LISTING_VERSION + 1,
      AppStatusStore.CURRENT_VERSION, conf.get(LOCAL_STORE_DIR).get)
    oldProvider.listing.setMetadata(meta)
    oldProvider.stop()

    val mistatchedVersionProvider = new FsHistoryProvider(conf)
    assert(mistatchedVersionProvider.listing.count(classOf[ApplicationInfoWrapper]) === 0)
  }

  test("invalidate cached UI") {
    val provider = new FsHistoryProvider(createTestConf())
    val appId = "new1"

    // Write an incomplete app log.
    val appLog = newLogFile(appId, None, inProgress = true)
    writeFile(appLog, true, None,
      SparkListenerApplicationStart(appId, Some(appId), 1L, "test", None)
      )
    provider.checkForLogs()

    // Load the app UI.
    val oldUI = provider.getAppUI(appId, None)
    assert(oldUI.isDefined)
    intercept[NoSuchElementException] {
      oldUI.get.ui.store.job(0)
    }

    // Add more info to the app log, and trigger the provider to update things.
    writeFile(appLog, true, None,
      SparkListenerApplicationStart(appId, Some(appId), 1L, "test", None),
      SparkListenerJobStart(0, 1L, Nil, null)
      )
    provider.checkForLogs()

    // Manually detach the old UI; ApplicationCache would do this automatically in a real SHS
    // when the app's UI was requested.
    provider.onUIDetached(appId, None, oldUI.get.ui)

    // Load the UI again and make sure we can get the new info added to the logs.
    val freshUI = provider.getAppUI(appId, None)
    assert(freshUI.isDefined)
    assert(freshUI != oldUI)
    freshUI.get.ui.store.job(0)
  }

  test("clean up stale app information") {
    withTempDir { storeDir =>
      val conf = createTestConf().set(LOCAL_STORE_DIR, storeDir.getAbsolutePath())
      val clock = new ManualClock()
      val provider = spy(new FsHistoryProvider(conf, clock))
      val appId = "new1"

      // Write logs for two app attempts.
      clock.advance(1)
      val attempt1 = newLogFile(appId, Some("1"), inProgress = false)
      writeFile(attempt1, true, None,
        SparkListenerApplicationStart(appId, Some(appId), 1L, "test", Some("1")),
        SparkListenerJobStart(0, 1L, Nil, null),
        SparkListenerApplicationEnd(5L)
      )
      val attempt2 = newLogFile(appId, Some("2"), inProgress = false)
      writeFile(attempt2, true, None,
        SparkListenerApplicationStart(appId, Some(appId), 1L, "test", Some("2")),
        SparkListenerJobStart(0, 1L, Nil, null),
        SparkListenerApplicationEnd(5L)
      )
      updateAndCheck(provider) { list =>
        assert(list.size === 1)
        assert(list(0).id === appId)
        assert(list(0).attempts.size === 2)
      }

      // Load the app's UI.
      val ui = provider.getAppUI(appId, Some("1"))
      assert(ui.isDefined)

      // Delete the underlying log file for attempt 1 and rescan. The UI should go away, but since
      // attempt 2 still exists, listing data should be there.
      clock.advance(1)
      attempt1.delete()
      updateAndCheck(provider) { list =>
        assert(list.size === 1)
        assert(list(0).id === appId)
        assert(list(0).attempts.size === 1)
      }
      assert(!ui.get.valid)
      assert(provider.getAppUI(appId, None) === None)

      // Delete the second attempt's log file. Now everything should go away.
      clock.advance(1)
      attempt2.delete()
      updateAndCheck(provider) { list =>
        assert(list.isEmpty)
      }
    }
  }

  test("SPARK-21571: clean up removes invalid history files") {
    val clock = new ManualClock()
    val conf = createTestConf().set(MAX_LOG_AGE_S.key, s"2d")
    val provider = new FsHistoryProvider(conf, clock)

    // Create 0-byte size inprogress and complete files
    var logCount = 0
    var validLogCount = 0

    val emptyInProgress = newLogFile("emptyInprogressLogFile", None, inProgress = true)
    emptyInProgress.createNewFile()
    emptyInProgress.setLastModified(clock.getTimeMillis())
    logCount += 1

    val slowApp = newLogFile("slowApp", None, inProgress = true)
    slowApp.createNewFile()
    slowApp.setLastModified(clock.getTimeMillis())
    logCount += 1

    val emptyFinished = newLogFile("emptyFinishedLogFile", None, inProgress = false)
    emptyFinished.createNewFile()
    emptyFinished.setLastModified(clock.getTimeMillis())
    logCount += 1

    // Create an incomplete log file, has an end record but no start record.
    val corrupt = newLogFile("nonEmptyCorruptLogFile", None, inProgress = false)
    writeFile(corrupt, true, None, SparkListenerApplicationEnd(0))
    corrupt.setLastModified(clock.getTimeMillis())
    logCount += 1

    provider.checkForLogs()
    provider.cleanLogs()
    assert(new File(testDir.toURI).listFiles().size === logCount)

    // Move the clock forward 1 day and scan the files again. They should still be there.
    clock.advance(TimeUnit.DAYS.toMillis(1))
    provider.checkForLogs()
    provider.cleanLogs()
    assert(new File(testDir.toURI).listFiles().size === logCount)

    // Update the slow app to contain valid info. Code should detect the change and not clean
    // it up.
    writeFile(slowApp, true, None,
      SparkListenerApplicationStart(slowApp.getName(), Some(slowApp.getName()), 1L, "test", None))
    slowApp.setLastModified(clock.getTimeMillis())
    validLogCount += 1

    // Move the clock forward another 2 days and scan the files again. This time the cleaner should
    // pick up the invalid files and get rid of them.
    clock.advance(TimeUnit.DAYS.toMillis(2))
    provider.checkForLogs()
    provider.cleanLogs()
    assert(new File(testDir.toURI).listFiles().size === validLogCount)
  }

  test("always find end event for finished apps") {
    // Create a log file where the end event is before the configure chunk to be reparsed at
    // the end of the file. The correct listing should still be generated.
    val log = newLogFile("end-event-test", None, inProgress = false)
    writeFile(log, true, None,
      Seq(
        SparkListenerApplicationStart("end-event-test", Some("end-event-test"), 1L, "test", None),
        SparkListenerEnvironmentUpdate(Map(
          "Spark Properties" -> Seq.empty,
          "Hadoop Properties" -> Seq.empty,
          "JVM Information" -> Seq.empty,
          "System Properties" -> Seq.empty,
          "Classpath Entries" -> Seq.empty
        )),
        SparkListenerApplicationEnd(5L)
      ) ++ (1 to 1000).map { i => SparkListenerJobStart(i, i, Nil) }: _*)

    val conf = createTestConf().set(END_EVENT_REPARSE_CHUNK_SIZE.key, s"1k")
    val provider = new FsHistoryProvider(conf)
    updateAndCheck(provider) { list =>
      assert(list.size === 1)
      assert(list(0).attempts.size === 1)
      assert(list(0).attempts(0).completed)
    }
  }

  test("parse event logs with optimizations off") {
    val conf = createTestConf()
      .set(END_EVENT_REPARSE_CHUNK_SIZE, 0L)
      .set(FAST_IN_PROGRESS_PARSING, false)
    val provider = new FsHistoryProvider(conf)

    val complete = newLogFile("complete", None, inProgress = false)
    writeFile(complete, true, None,
      SparkListenerApplicationStart("complete", Some("complete"), 1L, "test", None),
      SparkListenerApplicationEnd(5L)
      )

    val incomplete = newLogFile("incomplete", None, inProgress = true)
    writeFile(incomplete, true, None,
      SparkListenerApplicationStart("incomplete", Some("incomplete"), 1L, "test", None)
      )

    updateAndCheck(provider) { list =>
      list.size should be (2)
      list.count(_.attempts.head.completed) should be (1)
    }
  }

  test("SPARK-24948: blacklist files we don't have read permission on") {
    val clock = new ManualClock(1533132471)
    val provider = new FsHistoryProvider(createTestConf(), clock)
    val accessDenied = newLogFile("accessDenied", None, inProgress = false)
    writeFile(accessDenied, true, None,
      SparkListenerApplicationStart("accessDenied", Some("accessDenied"), 1L, "test", None))
    val accessGranted = newLogFile("accessGranted", None, inProgress = false)
    writeFile(accessGranted, true, None,
      SparkListenerApplicationStart("accessGranted", Some("accessGranted"), 1L, "test", None),
      SparkListenerApplicationEnd(5L))
    val mockedFs = spy(provider.fs)
    doThrow(new AccessControlException("Cannot read accessDenied file")).when(mockedFs).open(
      argThat((path: Path) => path.getName.toLowerCase(Locale.ROOT) == "accessdenied"))
    val mockedProvider = spy(provider)
    when(mockedProvider.fs).thenReturn(mockedFs)
    updateAndCheck(mockedProvider) { list =>
      list.size should be(1)
    }
    writeFile(accessDenied, true, None,
      SparkListenerApplicationStart("accessDenied", Some("accessDenied"), 1L, "test", None),
      SparkListenerApplicationEnd(5L))
    // Doing 2 times in order to check the blacklist filter too
    updateAndCheck(mockedProvider) { list =>
      list.size should be(1)
    }
    val accessDeniedPath = new Path(accessDenied.getPath)
    assert(mockedProvider.isBlacklisted(accessDeniedPath))
    clock.advance(24 * 60 * 60 * 1000 + 1) // add a bit more than 1d
    mockedProvider.cleanLogs()
    assert(!mockedProvider.isBlacklisted(accessDeniedPath))
  }

  test("check in-progress event logs absolute length") {
    val path = new Path("testapp.inprogress")
    val provider = new FsHistoryProvider(createTestConf())
    val mockedProvider = spy(provider)
    val mockedFs = mock(classOf[FileSystem])
    val in = mock(classOf[FSDataInputStream])
    val dfsIn = mock(classOf[DFSInputStream])
    when(mockedProvider.fs).thenReturn(mockedFs)
    when(mockedFs.open(path)).thenReturn(in)
    when(in.getWrappedStream).thenReturn(dfsIn)
    when(dfsIn.getFileLength).thenReturn(200)
    // FileStatus.getLen is more than logInfo fileSize
    var fileStatus = new FileStatus(200, false, 0, 0, 0, path)
    var logInfo = new LogInfo(path.toString, 0, LogType.EventLogs, Some("appId"),
      Some("attemptId"), 100)
    assert(mockedProvider.shouldReloadLog(logInfo, fileStatus))

    fileStatus = new FileStatus()
    fileStatus.setPath(path)
    // DFSInputStream.getFileLength is more than logInfo fileSize
    logInfo = new LogInfo(path.toString, 0, LogType.EventLogs, Some("appId"),
      Some("attemptId"), 100)
    assert(mockedProvider.shouldReloadLog(logInfo, fileStatus))
    // DFSInputStream.getFileLength is equal to logInfo fileSize
    logInfo = new LogInfo(path.toString, 0, LogType.EventLogs, Some("appId"),
      Some("attemptId"), 200)
    assert(!mockedProvider.shouldReloadLog(logInfo, fileStatus))
    // in.getWrappedStream returns other than DFSInputStream
    val bin = mock(classOf[BufferedInputStream])
    when(in.getWrappedStream).thenReturn(bin)
    assert(!mockedProvider.shouldReloadLog(logInfo, fileStatus))
    // fs.open throws exception
    when(mockedFs.open(path)).thenThrow(new IOException("Throwing intentionally"))
    assert(!mockedProvider.shouldReloadLog(logInfo, fileStatus))
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
      (checkFn: Seq[ApplicationInfo] => Unit): Unit = {
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
      val newFormatStream = new FileOutputStream(file)
      Utils.tryWithSafeFinally {
        EventLoggingListener.initEventLog(newFormatStream, false, null)
      } {
        newFormatStream.close()
      }
    }

    val writer = new OutputStreamWriter(bstream, StandardCharsets.UTF_8)
    Utils.tryWithSafeFinally {
      events.foreach(e => writer.write(compact(render(JsonProtocol.sparkEventToJson(e))) + "\n"))
    } {
      writer.close()
    }
  }

  private def createEmptyFile(file: File) = {
    new FileOutputStream(file).close()
  }

  private def createTestConf(inMemory: Boolean = false): SparkConf = {
    val conf = new SparkConf()
      .set(HISTORY_LOG_DIR, testDir.getAbsolutePath())
      .set(FAST_IN_PROGRESS_PARSING, true)

    if (!inMemory) {
      conf.set(LOCAL_STORE_DIR, Utils.createTempDir().getAbsolutePath())
    }

    conf
  }

  private def createTestExecutorInfo(
      appId: String,
      user: String,
      executorSeqNum: Int,
      includingLogFiles: Boolean = true): ExecutorInfo = {
    val host = s"host$executorSeqNum"
    val container = s"container$executorSeqNum"
    val cluster = s"cluster$executorSeqNum"
    val logUrlPrefix = s"http://$host:8888/$appId/$container/origin"

    val executorLogUrlMap = Map("stdout" -> s"$logUrlPrefix/stdout",
      "stderr" -> s"$logUrlPrefix/stderr")

    val extraAttributes = if (includingLogFiles) Map("LOG_FILES" -> "stdout,stderr") else Map.empty
    val executorAttributes = Map("CONTAINER_ID" -> container, "CLUSTER_ID" -> cluster,
      "USER" -> user) ++ extraAttributes

    new ExecutorInfo(host, 1, executorLogUrlMap, executorAttributes)
  }

  private class SafeModeTestProvider(conf: SparkConf, clock: Clock)
    extends FsHistoryProvider(conf, clock) {

    @volatile var inSafeMode = true

    // Skip initialization so that we can manually start the safe mode check thread.
    private[history] override def initialize(): Thread = null

    private[history] override def isFsInSafeMode(): Boolean = inSafeMode

  }

}

class TestGroupsMappingProvider extends GroupMappingServiceProvider {
  private val mappings = Map(
    "user3" -> "group1",
    "user4" -> "group1",
    "user5" -> "group")

  override def getGroups(username: String): Set[String] = {
    mappings.get(username).map(Set(_)).getOrElse(Set.empty)
  }
}
