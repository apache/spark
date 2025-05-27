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

import scala.concurrent.duration._

import com.google.common.io.{ByteStreams, Files}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.hdfs.{DFSInputStream, DistributedFileSystem}
import org.apache.hadoop.ipc.{CallerContext => HadoopCallerContext}
import org.apache.hadoop.security.AccessControlException
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{doThrow, mock, spy, verify, when}
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{JobExecutionStatus, SecurityManager, SPARK_VERSION, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.internal.config.DRIVER_LOG_DFS_DIR
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.UI.{ADMIN_ACLS, ADMIN_ACLS_GROUPS, UI_VIEW_ACLS, UI_VIEW_ACLS_GROUPS, USER_GROUPS_MAPPING}
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.KVUtils
import org.apache.spark.status.KVUtils.KVStoreScalaSerializer
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.{Clock, JsonProtocol, ManualClock, Utils}
import org.apache.spark.util.kvstore.InMemoryStore
import org.apache.spark.util.logging.DriverLogger

abstract class GlobFsHistoryProviderSuite
  extends SparkFunSuite with Matchers with PrivateMethodTester {

  private var testDirs: IndexedSeq[File] = null
  private var numSubDirs: Int = 0
  private var testGlob: String = "a b%20c+d"

  override def beforeEach(): Unit = {
    super.beforeEach()
    val random = new scala.util.Random
    numSubDirs = random.nextInt(3) + 1
    testDirs = (0 until numSubDirs).map { i =>
      Utils.createTempDir(namePrefix = testGlob + "-" + i)
    }
  }

  override def afterEach(): Unit = {
    try {
      testDirs.foreach { testDir =>
        Utils.deleteRecursively(testDir)
      }
    } finally {
      super.afterEach()
    }
  }

  protected def diskBackend: HybridStoreDiskBackend.Value

  protected def serializer: LocalStoreSerializer.Value = LocalStoreSerializer.JSON

  /** Create fake log files using the new log format used in Spark 1.3+ */
  private def newLogFiles(
      appId: String,
      appAttemptId: Option[String],
      inProgress: Boolean,
      codec: Option[String] = None): IndexedSeq[File] = {
    val ip = if (inProgress) EventLogFileWriter.IN_PROGRESS else ""
    testDirs.map { testDir =>
      val logUri = SingleEventLogFileWriter.getLogPath(testDir.toURI,
        appId, appAttemptId, codec)
      val logPath = new Path(logUri).toUri.getPath + ip
      new File(logPath)
    }
  }

  Seq(true, false).foreach { inMemory =>
    test(s"Parse application logs (inMemory = $inMemory)") {
      testAppLogParsing(inMemory)
    }
  }

  test("SPARK-52327: parse application logs with HybridStore") {
    testAppLogParsing(false, true)
  }

  test("SPARK-52327: Verify the configurable serializer for history server") {
    val conf = createTestConf()
    val serializerOfKVStore = KVUtils.serializerForHistoryServer(conf)
    assert(serializerOfKVStore.isInstanceOf[KVStoreScalaSerializer])
    if (serializer == LocalStoreSerializer.JSON) {
      assert(!serializerOfKVStore.isInstanceOf[KVStoreProtobufSerializer])
    } else {
      assert(serializerOfKVStore.isInstanceOf[KVStoreProtobufSerializer])
    }
  }

  private def testAppLogParsing(inMemory: Boolean, useHybridStore: Boolean = false): Unit = {
    val clock = new ManualClock(12345678)
    val conf = createTestConf(inMemory = inMemory, useHybridStore = useHybridStore)
    val provider = new GlobFsHistoryProvider(conf, clock)

    // Write new-style application logs.
    val newAppCompletes = newLogFiles("new1", None, inProgress = false)
    newAppCompletes.foreach { newAppComplete =>
      writeFile(newAppComplete, None,
        SparkListenerApplicationStart(newAppComplete.getName(),
          Some("new-app-complete"), 1L, "test",
          None),
        SparkListenerApplicationEnd(5L)
        )
    }

    // Write a new-style application log.
    val newAppCompressedCompletes = newLogFiles("new1compressed", None, inProgress = false,
      Some(CompressionCodec.LZF))
    newAppCompressedCompletes.foreach { newAppCompressedComplete =>
      writeFile(
        newAppCompressedComplete, Some(CompressionCodec.createCodec(conf, CompressionCodec.LZF)),
        SparkListenerApplicationStart(newAppCompressedComplete.getName(), Some("new-complete-lzf"),
          1L, "test", None),
        SparkListenerApplicationEnd(4L))
    }

    // Write an unfinished app, new-style.
    val newAppIncompletes = newLogFiles("new2", None, inProgress = true)
    newAppIncompletes.foreach { newAppIncomplete =>
      writeFile(newAppIncomplete, None,
        SparkListenerApplicationStart(newAppIncomplete.getName(),
          Some("new-incomplete"), 1L, "test",
          None)
        )
    }

    // Force a reload of data from the log directories, and check that logs are loaded.
    // Take the opportunity to check that the offset checks work as expected.
    updateAndCheck(provider) { list =>
      list.size should be (3 * numSubDirs)
      list.count(_.attempts.head.completed) should be (2 * numSubDirs)

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
            new Date(end), new Date(lastMod), duration, user, completed, SPARK_VERSION)))
      }

      // For completed files, lastUpdated would be lastModified time.
      (0 until numSubDirs).foreach { i =>
        list(2 * i) should be (makeAppInfo("new-app-complete",
          newAppCompletes(i).getName(), 1L, 5L,
          newAppCompletes(i).lastModified(), "test", true))
        list(2 * i + 1) should be (makeAppInfo("new-complete-lzf",
          newAppCompressedCompletes(i).getName(),
          1L, 4L, newAppCompressedCompletes(i).lastModified(), "test", true))
        // For Inprogress files, lastUpdated would be current loading time.
        list(i + numSubDirs * 2) should be (makeAppInfo("new-incomplete",
          newAppIncompletes(i).getName(), 1L, -1L,
          clock.getTimeMillis(), "test", false))
      }

      // Make sure the UI can be rendered.
      list.foreach { info =>
        val appUi = provider.getAppUI(info.id, None)
        appUi should not be null
        appUi should not be None
      }
    }
  }

  test("SPARK-52327 ignore files that cannot be read.") {
    // setReadable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)

    class TestGlobFsHistoryProvider extends GlobFsHistoryProvider(createTestConf()) {
      var doMergeApplicationListingCall = 0
      override private[history] def doMergeApplicationListing(
          reader: EventLogFileReader,
          lastSeen: Long,
          enableSkipToEnd: Boolean,
          lastCompactionIndex: Option[Long]): Unit = {
        super.doMergeApplicationListing(reader, lastSeen, enableSkipToEnd, lastCompactionIndex)
        doMergeApplicationListingCall += 1
      }
    }
    val provider = new TestGlobFsHistoryProvider

    val logFiles1 = newLogFiles("new1", None, inProgress = false)
    logFiles1.foreach { logFile1 =>
      writeFile(logFile1, None,
        SparkListenerApplicationStart("app1-1", Some("app1-1"), 1L, "test", None),
        SparkListenerApplicationEnd(2L)
        )
    }
    val logFiles2 = newLogFiles("new2", None, inProgress = false)
    logFiles2.foreach { logFile2 =>
      writeFile(logFile2, None,
        SparkListenerApplicationStart("app1-2", Some("app1-2"), 1L, "test", None),
        SparkListenerApplicationEnd(2L)
        )
      logFile2.setReadable(false, false)
    }

    updateAndCheck(provider) { list =>
      list.size should be (1)
    }

    provider.doMergeApplicationListingCall should be (1)
  }

  test("history file is renamed from inprogress to completed") {
    val provider = new GlobFsHistoryProvider(createTestConf())

    val logFiles1 = newLogFiles("app1", None, inProgress = true)
    logFiles1.foreach { logFile1 =>
      writeFile(logFile1, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", None),
      SparkListenerApplicationEnd(2L))
    }
    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      provider.getAttempt("app1", None).logPath should endWith(EventLogFileWriter.IN_PROGRESS)
    }

    val renamedLogFiles = newLogFiles("app1", None, inProgress = false)
    logFiles1.lazyZip(renamedLogFiles).foreach{ case (originalFile, renamedFile) =>
      originalFile.renameTo(renamedFile)
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      provider.getAttempt("app1", None).logPath should not endWith(EventLogFileWriter.IN_PROGRESS)
    }
  }

  test("SPARK-52327: Check final file if in-progress event log file does not exist") {
    withTempDir { dir =>
      val conf = createTestConf()
      conf.set(HISTORY_LOG_DIR, dir.getAbsolutePath)
      conf.set(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN, 1)
      conf.set(EVENT_LOG_COMPACTION_SCORE_THRESHOLD, 0.0d)
      val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val provider = new GlobFsHistoryProvider(conf)

      val mergeApplicationListing = PrivateMethod[Unit](Symbol("mergeApplicationListing"))

      val inProgressFile = newLogFiles("app1", None, inProgress = true)
      val logAppender1 = new LogAppender("in-progress and final event log files does not exist")
      inProgressFile.foreach { file =>
        withLogAppender(logAppender1) {
          provider invokePrivate mergeApplicationListing(
            EventLogFileReader(fs, new Path(file.toURI), None),
          System.currentTimeMillis,
          true
          )
        }
      }
      val logs1 = logAppender1.loggingEvents.map(_.getMessage.getFormattedMessage)
        .filter(_.contains("In-progress event log file does not exist: "))
      assert(logs1.size === 1)
      inProgressFile.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", None),
          SparkListenerApplicationEnd(2L))
      }
      val finalFile = newLogFiles("app1", None, inProgress = false)
      inProgressFile.lazyZip(finalFile).foreach{ case (inProgressFile, finalFile) =>
        inProgressFile.renameTo(finalFile)
      }
      val logAppender2 = new LogAppender("in-progress event log file has been renamed to final")
      inProgressFile.foreach { file =>
        withLogAppender(logAppender2) {
          provider invokePrivate mergeApplicationListing(
            EventLogFileReader(fs, new Path(file.toURI), None),
          System.currentTimeMillis,
          true
          )
        }
      }
      val logs2 = logAppender2.loggingEvents.map(_.getMessage.getFormattedMessage)
        .filter(_.contains("In-progress event log file does not exist: "))
      assert(logs2.isEmpty)
    }
  }

  test("Parse logs that application is not started") {
    val provider = new GlobFsHistoryProvider(createTestConf())

    val logFiles1 = newLogFiles("app1", None, inProgress = true)
    logFiles1.foreach { file =>
      writeFile(file, None,
        SparkListenerLogStart("1.4")
      )
    }
    updateAndCheck(provider) { list =>
      list.size should be (0)
    }
  }

  test("SPARK-52327 empty log directory") {
    val provider = new GlobFsHistoryProvider(createTestConf())

    val logFiles1 = newLogFiles("app1", None, inProgress = true)
    logFiles1.foreach{ logFile1 =>
      writeFile(logFile1, None,
      SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", None),
      SparkListenerApplicationEnd(2L))
    }
    testDirs.foreach { testDir =>
      val oldLog = new File(testDir, "old1")
      oldLog.mkdir()
    }

    provider.checkForLogs()
    val appListAfterRename = provider.getListing()
    appListAfterRename.size should be (numSubDirs)
  }

  test("apps with multiple attempts with order") {
    val provider = new GlobFsHistoryProvider(createTestConf())

    val attempt1 = newLogFiles("app1", Some("attempt1"), inProgress = true)
    attempt1.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1"))
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (numSubDirs)
    }

    val attempt2 = newLogFiles("app1", Some("attempt2"), inProgress = true)
    attempt2.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 2L, "test", Some("attempt2"))
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (2 * numSubDirs)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }

    val attempt3 = newLogFiles("app1", Some("attempt3"), inProgress = false)
    attempt3.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt3")),
        SparkListenerApplicationEnd(4L)
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (3 * numSubDirs)
      list.head.attempts.head.attemptId should be (Some("attempt3"))
    }

    val app2Attempt1 = newLogFiles("app2", Some("attempt1"), inProgress = false)
    app2Attempt1.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app2", Some("app2"), 5L, "test", Some("attempt1")),
        SparkListenerApplicationEnd(6L)
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (2 * numSubDirs)
      list.head.attempts.size should be (numSubDirs)
      list.last.attempts.size should be (3 * numSubDirs)
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
    val provider = new GlobFsHistoryProvider(conf)

    val attempt1 = newLogFiles("app1", Some("attempt1"), inProgress = true)

    val executorAddedEvents = expectedLogUrlMap.keys.zipWithIndex.map { case (execInfo, idx) =>
      SparkListenerExecutorAdded(1 + idx, s"exec$idx", execInfo)
    }.toList.sortBy(_.time)
    val allEvents = List(SparkListenerApplicationStart("app1", Some("app1"), 1L,
      "test", Some("attempt1"))) ++ executorAddedEvents ++
      (if (isCompletedApp) List(SparkListenerApplicationEnd(1000L)) else Seq())

    attempt1.foreach { file =>
      writeFile(file, None, allEvents: _*)
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (numSubDirs)

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
    val provider = new GlobFsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S.key, s"${maxAge}ms"), clock)

    val logs1 = newLogFiles("app1", Some("attempt1"), inProgress = false)
    logs1.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1")),
        SparkListenerApplicationEnd(2L)
      )
    }
    logs1.foreach { file =>
      file.setLastModified(0L)
    }

    val logs2 = newLogFiles("app1", Some("attempt2"), inProgress = false)
    logs2.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 3L, "test", Some("attempt2")),
        SparkListenerApplicationEnd(4L)
      )
    }
    logs2.foreach { file =>
      file.setLastModified(clock.getTimeMillis())
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (2 * numSubDirs)
    }

    // Move the clock forward so log1 exceeds the max age.
    clock.advance(maxAge)

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list.head.attempts.size should be (numSubDirs)
      list.head.attempts.head.attemptId should be (Some("attempt2"))
    }
    logs1.foreach { file =>
      assert(!file.exists())
    }

    // Do the same for the other log.
    clock.advance(maxAge)

    updateAndCheck(provider) { list =>
      list.size should be (0)
    }
    logs2.foreach { file =>
      assert(!file.exists())
    }
  }

  test("should not clean inprogress application with lastUpdated time less than maxTime") {
    val firstFileModifiedTime = TimeUnit.DAYS.toMillis(1)
    val secondFileModifiedTime = TimeUnit.DAYS.toMillis(6)
    val maxAge = TimeUnit.DAYS.toMillis(7)
    val clock = new ManualClock(0)
    val provider = new GlobFsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S, maxAge / 1000), clock)
    val logs = newLogFiles("inProgressApp1", None, inProgress = true)
    logs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(
          "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1"))
      )
    }
    clock.setTime(firstFileModifiedTime)
    logs.foreach { file =>
      file.setLastModified(clock.getTimeMillis())
    }
    provider.checkForLogs()
    logs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(
          "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1")),
        SparkListenerJobStart(0, 1L, Nil, null)
      )
    }
    clock.setTime(secondFileModifiedTime)
    logs.foreach { file =>
      file.setLastModified(clock.getTimeMillis())
    }
    provider.checkForLogs()
    clock.setTime(TimeUnit.DAYS.toMillis(10))
    logs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(
          "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1")),
        SparkListenerJobStart(0, 1L, Nil, null),
        SparkListenerJobEnd(0, 1L, JobSucceeded)
      )
    }
    logs.foreach { file =>
      file.setLastModified(clock.getTimeMillis())
    }
    provider.checkForLogs()
    // This should not trigger any cleanup
    updateAndCheck(provider) { list =>
      list.size should be(numSubDirs)
    }
  }

  test("log cleaner for inProgress files") {
    val firstFileModifiedTime = TimeUnit.SECONDS.toMillis(10)
    val secondFileModifiedTime = TimeUnit.SECONDS.toMillis(20)
    val maxAge = TimeUnit.SECONDS.toMillis(40)
    val clock = new ManualClock(0)
    val provider = new GlobFsHistoryProvider(
      createTestConf().set(MAX_LOG_AGE_S.key, s"${maxAge}ms"), clock)

    val logs1 = newLogFiles("inProgressApp1", None, inProgress = true)
    logs1.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(
          "inProgressApp1", Some("inProgressApp1"), 3L, "test", Some("attempt1"))
      )
    }

    clock.setTime(firstFileModifiedTime)
    provider.checkForLogs()

    val logs2 = newLogFiles("inProgressApp2", None, inProgress = true)
    logs2.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(
          "inProgressApp2", Some("inProgressApp2"), 23L, "test2", Some("attempt2"))
      )
    }

    clock.setTime(secondFileModifiedTime)
    provider.checkForLogs()

    // This should not trigger any cleanup
    updateAndCheck(provider) { list =>
      list.size should be(2 * numSubDirs)
    }

    // Should trigger cleanup for first file but not second one
    clock.setTime(firstFileModifiedTime + maxAge + 1)
    updateAndCheck(provider) { list =>
      list.size should be(numSubDirs)
    }
    logs1.foreach { file =>
      assert(!file.exists())
    }
    logs2.foreach { file =>
      assert(file.exists())
    }

    // Should cleanup the second file as well.
    clock.setTime(secondFileModifiedTime + maxAge + 1)
    updateAndCheck(provider) { list =>
      list.size should be(0)
    }
    logs1.foreach { file =>
      assert(!file.exists())
    }
    logs2.foreach { file =>
      assert(!file.exists())
    }
  }

  test("Event log copy") {
    val provider = new GlobFsHistoryProvider(createTestConf())
    val logs = (1 to 2).map { i =>
      val logs = newLogFiles("downloadApp1", Some(s"attempt$i"), inProgress = false)
      logs.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart(
            "downloadApp1", Some("downloadApp1"), 5000L * i, "test", Some(s"attempt$i")),
          SparkListenerApplicationEnd(5001L * i)
        )
      }
      logs
    }
    provider.checkForLogs()

    (1 to 2).foreach { i =>
      logs.foreach { log =>
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
            Files.asCharSource(log.find(_.getName == entry.getName).get, StandardCharsets.UTF_8)
              .read()
          actual should be (expected)
          totalEntries += 1
          entry = inputStream.getNextEntry
        }
        totalEntries should be (numSubDirs)
        inputStream.close()
      }
    }
  }

  test("driver log cleaner") {
    val firstFileModifiedTime = TimeUnit.SECONDS.toMillis(10)
    val secondFileModifiedTime = TimeUnit.SECONDS.toMillis(20)
    val maxAge = TimeUnit.SECONDS.toSeconds(40)
    val clock = new ManualClock(0)
    testDirs.foreach { testDir =>
      val testConf = new SparkConf()
      testConf.set(HISTORY_LOG_DIR, Utils.createTempDir(namePrefix = "eventLog").getAbsolutePath())
      testConf.set(DRIVER_LOG_DFS_DIR, testDir.getAbsolutePath())
      testConf.set(DRIVER_LOG_CLEANER_ENABLED, true)
      testConf.set(DRIVER_LOG_CLEANER_INTERVAL, maxAge / 4)
      testConf.set(MAX_DRIVER_LOG_AGE_S, maxAge)
      val provider = new GlobFsHistoryProvider(testConf, clock)

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
      KVUtils.viewToSeq(provider.listing.view(classOf[LogInfo])).size should be(3)

      // Should trigger cleanup for first file but not second one
      clock.setTime(firstFileModifiedTime + TimeUnit.SECONDS.toMillis(maxAge) + 1)
      provider.cleanDriverLogs()
      KVUtils.viewToSeq(provider.listing.view(classOf[LogInfo])).size should be(2)
      assert(!log1.exists())
      assert(log2.exists())
      assert(log3.exists())

      // Update the third file length while keeping the original modified time
      Files.write("Add logs to file".getBytes(), log3)
      log3.setLastModified(secondFileModifiedTime)
      // Should cleanup the second file but not the third file, as filelength changed.
      clock.setTime(secondFileModifiedTime + TimeUnit.SECONDS.toMillis(maxAge) + 1)
      provider.cleanDriverLogs()
      KVUtils.viewToSeq(provider.listing.view(classOf[LogInfo])).size should be(1)
      assert(!log1.exists())
      assert(!log2.exists())
      assert(log3.exists())

      // Should cleanup the third file as well.
      clock.setTime(secondFileModifiedTime + 2 * TimeUnit.SECONDS.toMillis(maxAge) + 2)
      provider.cleanDriverLogs()
      KVUtils.viewToSeq(provider.listing.view(classOf[LogInfo])).size should be(0)
      assert(!log3.exists())
    }
  }

  test("SPARK-52327 new logs with no app ID are ignored") {
    val provider = new GlobFsHistoryProvider(createTestConf())

    // Write a new log file without an app id, to make sure it's ignored.
    val logFiles = newLogFiles("app1", None, inProgress = true)
    logFiles.foreach { file =>
      writeFile(file, None,
        SparkListenerLogStart("1.4")
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
    }
  }

  test("provider correctly checks whether fs is in safe mode") {
    val provider = spy[GlobFsHistoryProvider](new GlobFsHistoryProvider(createTestConf()))
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
    testDirs.foreach { testDir =>
      testDir.delete()
    }
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

    // GlobFsHistoryProvider should ignore hidden files.  (It even writes out a hidden file itself
    // that should be ignored).

    // write out one totally bogus hidden file
    val hiddenGarbageFiles = testDirs.map { testDir =>
      new File(testDir, ".garbage")
    }
    hiddenGarbageFiles.foreach { file =>
      Utils.tryWithResource(new PrintWriter(file)) { out =>
        // scalastyle:off println
        out.println("GARBAGE")
        // scalastyle:on println
      }
    }

    // also write out one real event log file, but since its a hidden file, we shouldn't read it
    val tmpNewAppFiles = newLogFiles("hidden", None, inProgress = false)
    tmpNewAppFiles.foreach { file =>
      val hiddenNewAppFile = new File(file.getParentFile, "." + file.getName)
      file.renameTo(hiddenNewAppFile)
    }

    // and write one real file, which should still get picked up just fine
    val newAppCompleteFiles = newLogFiles("real-app", None, inProgress = false)
    newAppCompleteFiles.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(file.getName(), Some("new-app-complete"), 1L, "test",
          None),
        SparkListenerApplicationEnd(5L)
      )
    }

    val provider = new GlobFsHistoryProvider(createTestConf())
    updateAndCheck(provider) { list =>
      list.size should be (numSubDirs)
      list(0).name should be ("real-app")
    }
  }

  test("support history server ui admin acls") {
    def createAndCheck(conf: SparkConf, properties: (String, String)*)
      (checkFn: SecurityManager => Unit): Unit = {
      // Empty the testDirs for each test.
      testDirs.foreach { testDir =>
        if (testDir.exists() && testDir.isDirectory) {
          testDir.listFiles().foreach { f => if (f.isFile) f.delete() }
        }
      }

      var provider: GlobFsHistoryProvider = null
      try {
        provider = new GlobFsHistoryProvider(conf)
        val logs = newLogFiles("app1", Some("attempt1"), inProgress = false)
        logs.foreach { file =>
          writeFile(file, None,
            SparkListenerApplicationStart("app1", Some("app1"), System.currentTimeMillis(),
              "test", Some("attempt1")),
          SparkListenerEnvironmentUpdate(Map(
            "Spark Properties" -> properties.toSeq,
            "Hadoop Properties" -> Seq.empty,
            "JVM Information" -> Seq.empty,
            "System Properties" -> Seq.empty,
            "Metrics Properties" -> Seq.empty,
              "Classpath Entries" -> Seq.empty
            )),
            SparkListenerApplicationEnd(System.currentTimeMillis()))
        }

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
    val oldProvider = new GlobFsHistoryProvider(conf)

    val logFiles = newLogFiles("app1", None, inProgress = false)
    logFiles.foreach { file =>
      writeFile(file, None,
        SparkListenerLogStart("2.3"),
        SparkListenerApplicationStart("test", Some("test"), 1L, "test", None),
        SparkListenerApplicationEnd(5L)
      )
    }

    updateAndCheck(oldProvider) { list =>
      list.size should be (numSubDirs)
    }
    assert(oldProvider.listing.count(classOf[ApplicationInfoWrapper]) === numSubDirs)

    // Manually overwrite the version in the listing db; this should cause the new provider to
    // discard all data because the versions don't match.
    val meta = new GlobFsHistoryProviderMetadata(GlobFsHistoryProvider.CURRENT_LISTING_VERSION + 1,
      AppStatusStore.CURRENT_VERSION, conf.get(LOCAL_STORE_DIR).get)
    oldProvider.listing.setMetadata(meta)
    oldProvider.stop()

    val mismatchedVersionProvider = new GlobFsHistoryProvider(conf)
    assert(mismatchedVersionProvider.listing.count(classOf[ApplicationInfoWrapper]) === 0)
  }

  test("invalidate cached UI") {
    val provider = new GlobFsHistoryProvider(createTestConf())
    val appId = "new1"

    // Write an incomplete app log.
    val appLogs = newLogFiles(appId, None, inProgress = true)
    appLogs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(appId, Some(appId), 1L, "test", None)
      )
    }
    provider.checkForLogs()

    // Load the app UI.
    val oldUI = provider.getAppUI(appId, None)
    assert(oldUI.isDefined)
    intercept[NoSuchElementException] {
      oldUI.get.ui.store.job(0)
    }

    // Add more info to the app log, and trigger the provider to update things.
    appLogs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(appId, Some(appId), 1L, "test", None),
        SparkListenerJobStart(0, 1L, Nil, null)
      )
    }
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
      val provider = spy[GlobFsHistoryProvider](new GlobFsHistoryProvider(conf, clock))
      val appId = "new1"

      // Write logs for two app attempts.
      clock.advance(1)
      val attempts1 = newLogFiles(appId, Some("1"), inProgress = false)
      attempts1.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart(appId, Some(appId), 1L, "test", Some("1")),
          SparkListenerJobStart(0, 1L, Nil, null),
          SparkListenerApplicationEnd(5L)
        )
      }
      val attempts2 = newLogFiles(appId, Some("2"), inProgress = false)
      attempts2.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart(appId, Some(appId), 1L, "test", Some("2")),
          SparkListenerJobStart(0, 1L, Nil, null),
          SparkListenerApplicationEnd(5L)
        )
      }
      updateAndCheck(provider) { list =>
        assert(list.size === numSubDirs)
        assert(list(0).id === appId)
        assert(list(0).attempts.size === 2 * numSubDirs)
      }

      // Load the app's UI.
      val ui = provider.getAppUI(appId, Some("1"))
      assert(ui.isDefined)

      // Delete the underlying log file for attempt 1 and rescan. The UI should go away, but since
      // attempt 2 still exists, listing data should be there.
      clock.advance(1)
      attempts1.foreach { file =>
        file.delete()
      }
      updateAndCheck(provider) { list =>
        assert(list.size === numSubDirs)
        assert(list(0).id === appId)
        assert(list(0).attempts.size === numSubDirs)
      }
      assert(!ui.get.valid)
      assert(provider.getAppUI(appId, None) === None)

      // Delete the second attempt's log file. Now everything should go away.
      clock.advance(1)
      attempts2.foreach { file =>
        file.delete()
      }
      updateAndCheck(provider) { list =>
        assert(list.isEmpty)
      }
    }
  }

  test("SPARK-52327: clean up removes invalid history files") {
    val clock = new ManualClock()
    val conf = createTestConf().set(MAX_LOG_AGE_S.key, s"2d")
    val provider = new GlobFsHistoryProvider(conf, clock)

    // Create 0-byte size inprogress and complete files
    var logCount = 0
    var validLogCount = 0

    val emptyInProgress = newLogFiles("emptyInprogressLogFile", None, inProgress = true)
    emptyInProgress.foreach { file =>
      file.createNewFile()
      file.setLastModified(clock.getTimeMillis())
    }
    logCount += 1

    val slowApp = newLogFiles("slowApp", None, inProgress = true)
    slowApp.foreach { file =>
      file.createNewFile()
      file.setLastModified(clock.getTimeMillis())
    }
    logCount += 1

    val emptyFinished = newLogFiles("emptyFinishedLogFile", None, inProgress = false)
    emptyFinished.foreach { file =>
      file.createNewFile()
      file.setLastModified(clock.getTimeMillis())
    }
    logCount += 1

    // Create an incomplete log file, has an end record but no start record.
    val corrupt = newLogFiles("nonEmptyCorruptLogFile", None, inProgress = false)
    corrupt.foreach { file =>
      writeFile(file, None, SparkListenerApplicationEnd(0))
      file.setLastModified(clock.getTimeMillis())
    }
    logCount += 1

    provider.checkForLogs()
    provider.cleanLogs()

    testDirs.foreach { testDir =>
      assert(new File(testDir.toURI).listFiles().length === logCount)
    }

    // Move the clock forward 1 day and scan the files again. They should still be there.
    clock.advance(TimeUnit.DAYS.toMillis(1))
    provider.checkForLogs()
    provider.cleanLogs()
    testDirs.foreach { testDir =>
      assert(new File(testDir.toURI).listFiles().length === logCount)
    }

    // Update the slow app to contain valid info. Code should detect the change and not clean
    // it up.
    slowApp.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart(file.getName(), Some(file.getName()), 1L, "test", None))
      file.setLastModified(clock.getTimeMillis())
    }
    validLogCount += 1

    // Move the clock forward another 2 days and scan the files again. This time the cleaner should
    // pick up the invalid files and get rid of them.
    clock.advance(TimeUnit.DAYS.toMillis(2))
    provider.checkForLogs()
    provider.cleanLogs()
    testDirs.foreach{ testDir =>
      assert(new File(testDir.toURI).listFiles().length === validLogCount)
    }
  }

  test("always find end event for finished apps") {
    // Create a log file where the end event is before the configure chunk to be reparsed at
    // the end of the file. The correct listing should still be generated.
    val logs = newLogFiles("end-event-test", None, inProgress = false)
    logs.foreach { file =>
      writeFile(file, None,
        Seq(
        SparkListenerApplicationStart("end-event-test", Some("end-event-test"), 1L, "test", None),
        SparkListenerEnvironmentUpdate(Map(
          "Spark Properties" -> Seq.empty,
          "Hadoop Properties" -> Seq.empty,
          "JVM Information" -> Seq.empty,
          "System Properties" -> Seq.empty,
          "Metrics Properties" -> Seq.empty,
          "Classpath Entries" -> Seq.empty
        )),
        SparkListenerApplicationEnd(5L)
      ) ++ (1 to 1000).map { i => SparkListenerJobStart(i, i, Nil) }: _*)
    }

    val conf = createTestConf().set(END_EVENT_REPARSE_CHUNK_SIZE.key, s"1k")
    val provider = new GlobFsHistoryProvider(conf)
    updateAndCheck(provider) { list =>
      assert(list.size === numSubDirs)
      assert(list(0).attempts.size === numSubDirs)
      assert(list(0).attempts(0).completed)
    }
  }

  test("parse event logs with optimizations off") {
    val conf = createTestConf()
      .set(END_EVENT_REPARSE_CHUNK_SIZE, 0L)
      .set(FAST_IN_PROGRESS_PARSING, false)
    val provider = new GlobFsHistoryProvider(conf)

    val complete = newLogFiles("complete", None, inProgress = false)
    complete.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("complete", Some("complete"), 1L, "test", None),
        SparkListenerApplicationEnd(5L)
      )
    }

    val incomplete = newLogFiles("incomplete", None, inProgress = true)
    incomplete.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("incomplete", Some("incomplete"), 1L, "test", None)
      )
    }

    updateAndCheck(provider) { list =>
      list.size should be (2)
      list.count(_.attempts.head.completed) should be (1)
    }
  }

  test("SPARK-52327: ignore files we don't have read permission on") {
    val clock = new ManualClock(1533132471)
    val provider = new GlobFsHistoryProvider(createTestConf(), clock)
    val accessDeniedFiles = newLogFiles("accessDenied", None, inProgress = false)
    accessDeniedFiles.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("accessDenied", Some("accessDenied"), 1L, "test", None))
    }
    val accessGrantedFiles = newLogFiles("accessGranted", None, inProgress = false)
    accessGrantedFiles.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("accessGranted", Some("accessGranted"), 1L, "test", None),
        SparkListenerApplicationEnd(5L))
    }
    var isReadable = false
    val mockedFs = spy[FileSystem](provider.fs)
    doThrow(new AccessControlException("Cannot read accessDenied file")).when(mockedFs).open(
      argThat((path: Path) => path.getName.toLowerCase(Locale.ROOT) == "accessdenied" &&
        !isReadable))
    val mockedProvider = spy[GlobFsHistoryProvider](provider)
    when(mockedProvider.fs).thenReturn(mockedFs)
    updateAndCheck(mockedProvider) { list =>
      list.size should be(1)
    }
    // Doing 2 times in order to check the inaccessibleList filter too
    updateAndCheck(mockedProvider) { list =>
      list.size should be(1)
    }
    val accessDeniedPaths = accessDeniedFiles.map {accessDenied =>
      new Path(accessDenied.getPath())
    }
    accessDeniedPaths.foreach { path =>
      assert(!mockedProvider.isAccessible(path))
    }
    clock.advance(24 * 60 * 60 * 1000 + 1) // add a bit more than 1d
    isReadable = true
    mockedProvider.cleanLogs()
    accessDeniedPaths.foreach { accessDeniedPath =>
      updateAndCheck(mockedProvider) { list =>
      assert(mockedProvider.isAccessible(accessDeniedPath))
      assert(list.exists(_.name == "accessDenied"))
      assert(list.exists(_.name == "accessGranted"))
        list.size should be(2)
      }
    }
  }

  test("check in-progress event logs absolute length") {
    val path = new Path("testapp.inprogress")
    val provider = new GlobFsHistoryProvider(createTestConf())
    val mockedProvider = spy[GlobFsHistoryProvider](provider)
    val mockedFs = mock(classOf[FileSystem])
    val in = mock(classOf[FSDataInputStream])
    val dfsIn = mock(classOf[DFSInputStream])
    when(mockedProvider.fs).thenReturn(mockedFs)
    when(mockedFs.open(path)).thenReturn(in)
    when(in.getWrappedStream).thenReturn(dfsIn)
    when(dfsIn.getFileLength).thenReturn(200)

    // FileStatus.getLen is more than logInfo fileSize
    var fileStatus = new FileStatus(200, false, 0, 0, 0, path)
    when(mockedFs.getFileStatus(path)).thenReturn(fileStatus)
    var logInfo = new GlobLogInfo(path.toString, 0, GlobLogType.EventLogs, Some("appId"),
      Some("attemptId"), 100, None, None, false)
    var reader = EventLogFileReader(mockedFs, path)
    assert(reader.isDefined)
    assert(mockedProvider.shouldReloadLog(logInfo, reader.get))

    fileStatus = new FileStatus()
    fileStatus.setPath(path)
    when(mockedFs.getFileStatus(path)).thenReturn(fileStatus)
    // DFSInputStream.getFileLength is more than logInfo fileSize
    logInfo = new GlobLogInfo(path.toString, 0, GlobLogType.EventLogs, Some("appId"),
      Some("attemptId"), 100, None, None, false)
    reader = EventLogFileReader(mockedFs, path)
    assert(reader.isDefined)
    assert(mockedProvider.shouldReloadLog(logInfo, reader.get))

    // DFSInputStream.getFileLength is equal to logInfo fileSize
    logInfo = new GlobLogInfo(path.toString, 0, GlobLogType.EventLogs, Some("appId"),
      Some("attemptId"), 200, None, None, false)
    reader = EventLogFileReader(mockedFs, path)
    assert(reader.isDefined)
    assert(!mockedProvider.shouldReloadLog(logInfo, reader.get))

    // in.getWrappedStream returns other than DFSInputStream
    val bin = mock(classOf[BufferedInputStream])
    when(in.getWrappedStream).thenReturn(bin)
    reader = EventLogFileReader(mockedFs, path)
    assert(reader.isDefined)
    assert(!mockedProvider.shouldReloadLog(logInfo, reader.get))

    // fs.open throws exception
    when(mockedFs.open(path)).thenThrow(new IOException("Throwing intentionally"))
    reader = EventLogFileReader(mockedFs, path)
    assert(reader.isDefined)
    assert(!mockedProvider.shouldReloadLog(logInfo, reader.get))
  }

  test("log cleaner with the maximum number of log files") {
    val clock = new ManualClock(0)
    (5 to 0 by -1).foreach { num =>
      val logs1_1 = newLogFiles("app1", Some("attempt1"), inProgress = false)
      logs1_1.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1")),
          SparkListenerApplicationEnd(2L)
        )
        file.setLastModified(2L)
      }

      val logs2_1 = newLogFiles("app2", Some("attempt1"), inProgress = false)
      logs2_1.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app2", Some("app2"), 3L, "test", Some("attempt1")),
          SparkListenerApplicationEnd(4L)
        )
        file.setLastModified(4L)
      }

      val logs3_1 = newLogFiles("app3", Some("attempt1"), inProgress = false)
      logs3_1.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app3", Some("app3"), 5L, "test", Some("attempt1")),
          SparkListenerApplicationEnd(6L)
        )
        file.setLastModified(6L)
      }

      val logs1_2_incomplete = newLogFiles("app1", Some("attempt2"), inProgress = false)
      logs1_2_incomplete.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app1", Some("app1"), 7L, "test", Some("attempt2"))
        )
        file.setLastModified(8L)
      }

      val logs3_2 = newLogFiles("app3", Some("attempt2"), inProgress = false)
      logs3_2.foreach { file =>
        writeFile(file, None,
          SparkListenerApplicationStart("app3", Some("app3"), 9L, "test", Some("attempt2")),
          SparkListenerApplicationEnd(10L)
        )
        file.setLastModified(10L)
      }

      val provider = new GlobFsHistoryProvider(
        createTestConf().set(MAX_LOG_NUM.key, s"$num"), clock)
      updateAndCheck(provider) { list =>
        logs1_1.foreach { log =>
          assert(log.exists() == (num > 4))
        }
        logs1_2_incomplete.foreach { log =>
          assert(log.exists())  // Always exists for all configurations
        }
        logs2_1.foreach { log =>
          assert(log.exists() == (num > 3))
        }
        logs3_1.foreach { log =>
          assert(log.exists() == (num > 2))
        }
        logs3_2.foreach { log =>
          assert(log.exists() == (num > 2))
        }
      }
    }
  }

  test("backwards compatibility with LogInfo from Spark 2.4") {
    case class LogInfoV24(
         logPath: String,
         lastProcessed: Long,
         appId: Option[String],
         attemptId: Option[String],
         fileSize: Long)

    val oldObj = LogInfoV24("dummy", System.currentTimeMillis(), Some("hello"),
      Some("attempt1"), 100)

    val serializer = new KVStoreScalaSerializer()
    val serializedOldObj = serializer.serialize(oldObj)
    val deserializedOldObj = serializer.deserialize(serializedOldObj, classOf[LogInfo])
    assert(deserializedOldObj.logPath === oldObj.logPath)
    assert(deserializedOldObj.lastProcessed === oldObj.lastProcessed)
    assert(deserializedOldObj.appId === oldObj.appId)
    assert(deserializedOldObj.attemptId === oldObj.attemptId)
    assert(deserializedOldObj.fileSize === oldObj.fileSize)

    // SPARK-52327: added logType: LogType.Value - expected 'null' on old format
    assert(deserializedOldObj.logType === null)

    // SPARK-52327: added lastIndex: Option[Long], isComplete: Boolean - expected 'None' and
    // 'false' on old format. The default value for isComplete is wrong value for completed app,
    // but the value will be corrected once checkForLogs is called.
    assert(deserializedOldObj.lastIndex === None)
    assert(deserializedOldObj.isComplete === false)
  }

  test("SPARK-52327 LogInfo should be serialized/deserialized by jackson properly") {
    def assertSerDe(serializer: KVStoreScalaSerializer, info: LogInfo): Unit = {
      val infoAfterSerDe = serializer.deserialize(serializer.serialize(info), classOf[LogInfo])
      assert(infoAfterSerDe === info)
      assertOptionAfterSerde(infoAfterSerDe.lastIndex, info.lastIndex)
    }

    val serializer = new KVStoreScalaSerializer()
    val logInfoWithIndexAsNone = LogInfo("dummy", 0, LogType.EventLogs, Some("appId"),
      Some("attemptId"), 100, None, None, false)
    assertSerDe(serializer, logInfoWithIndexAsNone)

    val logInfoWithIndex = LogInfo("dummy", 0, LogType.EventLogs, Some("appId"),
      Some("attemptId"), 100, Some(3), None, false)
    assertSerDe(serializer, logInfoWithIndex)
  }

  test("SPARK-52327 AttemptInfoWrapper should be serialized/deserialized by jackson properly") {
    def assertSerDe(serializer: KVStoreScalaSerializer, attempt: AttemptInfoWrapper): Unit = {
      val attemptAfterSerDe = serializer.deserialize(serializer.serialize(attempt),
        classOf[AttemptInfoWrapper])
      assert(attemptAfterSerDe.info === attempt.info)
      // skip comparing some fields, as they've not triggered SPARK-52327
      assertOptionAfterSerde(attemptAfterSerDe.lastIndex, attempt.lastIndex)
    }

    val serializer = new KVStoreScalaSerializer()
    val appInfo = new ApplicationAttemptInfo(None, new Date(1), new Date(1), new Date(1),
      10, "spark", false, "dummy")
    val attemptInfoWithIndexAsNone = new AttemptInfoWrapper(appInfo, "dummyPath", 10, None,
      None, None, None, None)
    assertSerDe(serializer, attemptInfoWithIndexAsNone)

    val attemptInfoWithIndex = new AttemptInfoWrapper(appInfo, "dummyPath", 10, Some(1),
      None, None, None, None)
    assertSerDe(serializer, attemptInfoWithIndex)
  }

  test("SPARK-52327: clean up specified event log") {
    val clock = new ManualClock()
    val conf = createTestConf().set(MAX_LOG_AGE_S, 0L).set(CLEANER_ENABLED, true)
    val provider = new GlobFsHistoryProvider(conf, clock)

    // create an invalid application log file
    val inValidLogFiles = newLogFiles("inValidLogFile", None, inProgress = true)
    inValidLogFiles.foreach { file =>
      file.createNewFile()
      writeFile(file, None,
        SparkListenerApplicationStart(file.getName, None, 1L, "test", None))
      file.setLastModified(clock.getTimeMillis())
    }

    // create a valid application log file
    val validLogFiles = newLogFiles("validLogFile", None, inProgress = true)
    validLogFiles.foreach { file =>
      file.createNewFile()
      writeFile(file, None,
        SparkListenerApplicationStart(file.getName, Some("local_123"), 1L, "test", None))
      file.setLastModified(clock.getTimeMillis())
    }

    provider.checkForLogs()
    // The invalid application log file would be cleaned by checkAndCleanLog().
    testDirs.foreach { testDir =>
      assert(new File(testDir.toURI).listFiles().length === 1)
    }

    clock.advance(1)
    // cleanLogs() would clean the valid application log file.
    provider.cleanLogs()
    testDirs.foreach { testDir =>
      assert(new File(testDir.toURI).listFiles().length === 0)
    }
  }

  private def assertOptionAfterSerde(opt: Option[Long], expected: Option[Long]): Unit = {
    if (expected.isEmpty) {
      assert(opt.isEmpty)
    } else {
      // The issue happens only when the value in Option is being unboxed. Here we ensure unboxing
      // to Long succeeds: even though IDE suggests `.toLong` is redundant, direct comparison
      // doesn't trigger unboxing and passes even without SPARK-29755, so don't remove
      // `.toLong` below. Please refer SPARK-29755 for more details.
      assert(opt.get.toLong === expected.get.toLong)
    }
  }

  test("compact event log files") {
    def verifyEventLogFiles(
        fs: FileSystem,
        rootPath: String,
        expectedIndexForCompact: Option[Long],
        expectedIndicesForNonCompact: Seq[Long]): Unit = {
      val reader = EventLogFileReader(fs, new Path(rootPath)).get
      var logFiles = reader.listEventLogFiles

      expectedIndexForCompact.foreach { idx =>
        val headFile = logFiles.head
        assert(EventLogFileWriter.isCompacted(headFile.getPath))
        assert(idx == RollingEventLogFilesWriter.getEventLogFileIndex(headFile.getPath.getName))
        logFiles = logFiles.drop(1)
      }

      assert(logFiles.size === expectedIndicesForNonCompact.size)

      logFiles.foreach { logFile =>
        assert(RollingEventLogFilesWriter.isEventLogFile(logFile))
        assert(!EventLogFileWriter.isCompacted(logFile.getPath))
      }

      val indices = logFiles.map { logFile =>
        RollingEventLogFilesWriter.getEventLogFileIndex(logFile.getPath.getName)
      }
      assert(expectedIndicesForNonCompact === indices)
    }

    withTempDir { dir =>
      val conf = createTestConf()
      conf.set(HISTORY_LOG_DIR, dir.getAbsolutePath)
      conf.set(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN, 1)
      conf.set(EVENT_LOG_COMPACTION_SCORE_THRESHOLD, 0.0d)
      val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val provider = new GlobFsHistoryProvider(conf)

      val writer = new RollingEventLogFilesWriter("app", None, dir.toURI, conf, hadoopConf)
      writer.start()

      // writing event log file 1 - don't compact for now
      writeEventsToRollingWriter(writer, Seq(
        SparkListenerApplicationStart("app", Some("app"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)

      updateAndCheck(provider) { _ =>
        verifyEventLogFiles(fs, writer.logPath, None, Seq(1))
        val info = provider.listing.read(classOf[LogInfo], writer.logPath)
        assert(info.lastEvaluatedForCompaction === Some(1))
      }

      // writing event log file 2 - compact the event log file 1 into 1.compact
      writeEventsToRollingWriter(writer, Seq.empty, rollFile = true)
      writeEventsToRollingWriter(writer, Seq(SparkListenerUnpersistRDD(1),
        SparkListenerJobEnd(1, 1, JobSucceeded)), rollFile = false)

      updateAndCheck(provider) { _ =>
        verifyEventLogFiles(fs, writer.logPath, Some(1), Seq(2))
        val info = provider.listing.read(classOf[LogInfo], writer.logPath)
        assert(info.lastEvaluatedForCompaction === Some(2))
      }

      // writing event log file 3 - compact two files - 1.compact & 2 into one, 2.compact
      writeEventsToRollingWriter(writer, Seq.empty, rollFile = true)
      writeEventsToRollingWriter(writer, Seq(
        SparkListenerExecutorAdded(3, "exec1", new ExecutorInfo("host1", 1, Map.empty)),
        SparkListenerJobStart(2, 4, Seq.empty),
        SparkListenerJobEnd(2, 5, JobSucceeded)), rollFile = false)

      writer.stop()

      updateAndCheck(provider) { _ =>
        verifyEventLogFiles(fs, writer.logPath, Some(2), Seq(3))

        val info = provider.listing.read(classOf[LogInfo], writer.logPath)
        assert(info.lastEvaluatedForCompaction === Some(3))

        val store = new InMemoryStore
        val appStore = new AppStatusStore(store)

        val reader = EventLogFileReader(fs, new Path(writer.logPath)).get
        provider.rebuildAppStore(store, reader, 0L)

        // replayed store doesn't have any job, as events for job are removed while compacting
        intercept[NoSuchElementException] {
          appStore.job(1)
        }

        // but other events should be available even they were in original files to compact
        val appInfo = appStore.applicationInfo()
        assert(appInfo.id === "app")
        assert(appInfo.name === "app")

        // All events in retained file(s) should be available, including events which would have
        // been filtered out if compaction is applied. e.g. finished jobs, removed executors, etc.
        val exec1 = appStore.executorSummary("exec1")
        assert(exec1.hostPort === "host1")
        val job2 = appStore.job(2)
        assert(job2.status === JobExecutionStatus.SUCCEEDED)
      }
    }
  }

  test("SPARK-52327: don't let one bad rolling log folder prevent loading other applications") {
    withTempDir { dir =>
      val conf = createTestConf(true)
      conf.set(HISTORY_LOG_DIR, dir.getAbsolutePath)
      val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val provider = new GlobFsHistoryProvider(conf)

      val writer = new RollingEventLogFilesWriter("app", None, dir.toURI, conf, hadoopConf)
      writer.start()

      writeEventsToRollingWriter(writer, Seq(
        SparkListenerApplicationStart("app", Some("app"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)
      provider.checkForLogs()
      provider.cleanLogs()
      assert(dir.listFiles().length === 1)
      assert(provider.getListing().length === 1)

      // Manually delete the appstatus file to make an invalid rolling event log
      val appStatusPath = RollingEventLogFilesWriter.getAppStatusFilePath(new Path(writer.logPath),
        "app", None, true)
      fs.delete(appStatusPath, false)
      provider.checkForLogs()
      provider.cleanLogs()
      assert(provider.getListing().length === 0)

      // Create a new application
      val writer2 = new RollingEventLogFilesWriter("app2", None, dir.toURI, conf, hadoopConf)
      writer2.start()
      writeEventsToRollingWriter(writer2, Seq(
        SparkListenerApplicationStart("app2", Some("app2"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)

      // Both folders exist but only one application found
      provider.checkForLogs()
      provider.cleanLogs()
      assert(provider.getListing().length === 1)
      assert(dir.listFiles().length === 2)

      // Make sure a new provider sees the valid application
      provider.stop()
      val newProvider = new GlobFsHistoryProvider(conf)
      newProvider.checkForLogs()
      assert(newProvider.getListing().length === 1)
    }
  }

  test("SPARK-52327: Support spark.history.fs.update.batchSize") {
    withTempDir { dir =>
      val conf = createTestConf(true)
      conf.set(HISTORY_LOG_DIR, dir.getAbsolutePath)
      conf.set(UPDATE_BATCHSIZE, 1)
      val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
      val provider = new GlobFsHistoryProvider(conf)

      // Create 1st application
      val writer1 = new RollingEventLogFilesWriter("app1", None, dir.toURI, conf, hadoopConf)
      writer1.start()
      writeEventsToRollingWriter(writer1, Seq(
        SparkListenerApplicationStart("app1", Some("app1"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)
      writer1.stop()

      // Create 2nd application
      val writer2 = new RollingEventLogFilesWriter("app2", None, dir.toURI, conf, hadoopConf)
      writer2.start()
      writeEventsToRollingWriter(writer2, Seq(
        SparkListenerApplicationStart("app2", Some("app2"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)
      writer2.stop()

      // The 1st checkForLogs should scan/update app2 only since it is newer than app1
      provider.checkForLogs()
      assert(provider.getListing().length === 1)
      assert(dir.listFiles().length === 2)
      assert(provider.getListing().map(e => e.id).contains("app2"))
      assert(!provider.getListing().map(e => e.id).contains("app1"))

      // Create 3rd application
      val writer3 = new RollingEventLogFilesWriter("app3", None, dir.toURI, conf, hadoopConf)
      writer3.start()
      writeEventsToRollingWriter(writer3, Seq(
        SparkListenerApplicationStart("app3", Some("app3"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)
      writer3.stop()

      // The 2nd checkForLogs should scan/update app3 only since it is newer than app1
      provider.checkForLogs()
      assert(provider.getListing().length === 2)
      assert(dir.listFiles().length === 3)
      assert(provider.getListing().map(e => e.id).contains("app3"))
      assert(!provider.getListing().map(e => e.id).contains("app1"))

      provider.stop()
    }
  }

  test("SPARK-52327: EventLogFileReader should skip rolling event log directories with no logs") {
    withTempDir { dir =>
      val conf = createTestConf(true)
      conf.set(HISTORY_LOG_DIR, dir.getAbsolutePath)
      val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val provider = new GlobFsHistoryProvider(conf)

      val writer = new RollingEventLogFilesWriter("app", None, dir.toURI, conf, hadoopConf)
      writer.start()

      writeEventsToRollingWriter(writer, Seq(
        SparkListenerApplicationStart("app", Some("app"), 0, "user", None),
        SparkListenerJobStart(1, 0, Seq.empty)), rollFile = false)
      provider.checkForLogs()
      provider.cleanLogs()
      assert(dir.listFiles().length === 1)
      assert(provider.getListing().length === 1)

      // Manually delete event log files and create event log file reader
      val eventLogDir = dir.listFiles().head
      eventLogDir.listFiles
        .filter(f => RollingEventLogFilesWriter.isEventLogFile(f.getName))
        .foreach(f => f.delete())
      EventLogFileReader(fs, new Path(eventLogDir.getAbsolutePath)).map(_.lastIndex)
    }
  }

  test("SPARK-52327: check ui view permissions without retrieving ui") {
    val conf = createTestConf()
      .set(HISTORY_SERVER_UI_ACLS_ENABLE, true)
      .set(HISTORY_SERVER_UI_ADMIN_ACLS, Seq("user1", "user2"))
      .set(HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS, Seq("group1"))
      .set(USER_GROUPS_MAPPING, classOf[TestGroupsMappingProvider].getName)

    val provider = new GlobFsHistoryProvider(conf)
    val logs = newLogFiles("app1", Some("attempt1"), inProgress = false)
    logs.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), System.currentTimeMillis(),
          "test", Some("attempt1")),
      SparkListenerEnvironmentUpdate(Map(
        "Spark Properties" -> List((UI_VIEW_ACLS.key, "user"), (UI_VIEW_ACLS_GROUPS.key, "group")),
        "Hadoop Properties" -> Seq.empty,
        "JVM Information" -> Seq.empty,
        "System Properties" -> Seq.empty,
        "Metrics Properties" -> Seq.empty,
          "Classpath Entries" -> Seq.empty
        )),
        SparkListenerApplicationEnd(System.currentTimeMillis()))
    }

    provider.checkForLogs()

    // attempt2 doesn't exist
    intercept[NoSuchElementException] {
      provider.checkUIViewPermissions("app1", Some("attempt2"), "user1")
    }
    // app2 doesn't exist
    intercept[NoSuchElementException] {
      provider.checkUIViewPermissions("app2", Some("attempt1"), "user1")
    }

    // user1 and user2 are admins
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "user1"))
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "user2"))
    // user3 is a member of admin group "group1"
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "user3"))
    // test is the app owner
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "test"))
    // user is in the app's view acls
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "user"))
    // user5 is a member of the app's view acls group "group"
    assert(provider.checkUIViewPermissions("app1", Some("attempt1"), "user5"))

    // abc, user6, user7 don't have permissions
    assert(!provider.checkUIViewPermissions("app1", Some("attempt1"), "abc"))
    assert(!provider.checkUIViewPermissions("app1", Some("attempt1"), "user6"))
    assert(!provider.checkUIViewPermissions("app1", Some("attempt1"), "user7"))

    provider.stop()
  }

  test("SPARK-52327: Reduce the number of doMergeApplicationListing invocations") {
    class TestGlobFsHistoryProvider(conf: SparkConf, clock: Clock)
      extends GlobFsHistoryProvider(conf, clock) {
      var doMergeApplicationListingCall = 0
      override private[history] def doMergeApplicationListing(
          reader: EventLogFileReader,
          lastSeen: Long,
          enableSkipToEnd: Boolean,
          lastCompactionIndex: Option[Long]): Unit = {
        super.doMergeApplicationListing(reader, lastSeen, enableSkipToEnd, lastCompactionIndex)
        doMergeApplicationListingCall += 1
      }
    }

    val maxAge = TimeUnit.SECONDS.toMillis(10)
    val clock = new ManualClock(maxAge / 2)
    val conf = createTestConf().set(MAX_LOG_AGE_S.key, s"${maxAge}ms").set(CLEANER_ENABLED, true)
    val provider = new TestGlobFsHistoryProvider(conf, clock)

    val logs1 = newLogFiles("app1", Some("attempt1"), inProgress = false)
    logs1.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 1L, "test", Some("attempt1")),
        SparkListenerApplicationEnd(2L)
      )
    }
    logs1.foreach { file =>
      file.setLastModified(0L)
    }

    val logs2 = newLogFiles("app1", Some("attempt2"), inProgress = false)
    logs2.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app1", Some("app1"), 2L, "test", Some("attempt2")),
        SparkListenerApplicationEnd(4L)
      )
    }
    logs2.foreach { file =>
      file.setLastModified(clock.getTimeMillis())
    }

    val logs3 = newLogFiles("app2", Some("attempt1"), inProgress = false)
    logs3.foreach { file =>
      writeFile(file, None,
        SparkListenerApplicationStart("app2", Some("app1"), 3L, "test", Some("attempt1")),
        SparkListenerApplicationEnd(4L)
      )
    }
    logs3.foreach { file =>
      file.setLastModified(0L)
    }

    provider.getListing().size should be (0)

    // Move the clock forward so log1 and log3 exceed the max age.
    clock.advance(maxAge)
    // Avoid unnecessary parse, the expired log files would be cleaned by checkForLogs().
    provider.checkForLogs()

    provider.doMergeApplicationListingCall should be (1)
    provider.getListing().size should be (1)

    logs1.foreach { file =>
      assert(!file.exists())
    }
    logs2.foreach { file =>
      assert(file.exists())
    }
    logs3.foreach { file =>
      assert(!file.exists())
    }
  }

  test("SPARK-52327: GlobFsHistoryProvider start should set Hadoop CallerContext") {
    val provider = new GlobFsHistoryProvider(createTestConf())
    provider.start()

    try {
      val hadoopCallerContext = HadoopCallerContext.getCurrent()
      assert(hadoopCallerContext.getContext() === "SPARK_HISTORY")
    } finally {
      provider.stop()
    }
  }

  /**
   * Asks the provider to check for logs and calls a function to perform checks on the updated
   * app list. Example:
   *
   *     updateAndCheck(provider) { list =>
   *       // asserts
   *     }
   */
  private def updateAndCheck(provider: GlobFsHistoryProvider)
      (checkFn: Seq[ApplicationInfo] => Unit): Unit = {
    provider.checkForLogs()
    provider.cleanLogs()
    checkFn(provider.getListing().toSeq)
  }

  private def writeFile(file: File, codec: Option[CompressionCodec],
    events: SparkListenerEvent*) = {
    val fstream = new FileOutputStream(file)
    val cstream = codec.map(_.compressedContinuousOutputStream(fstream)).getOrElse(fstream)
    val bstream = new BufferedOutputStream(cstream)
    val jsonProtocol = new JsonProtocol(new SparkConf())

    val metadata = SparkListenerLogStart(org.apache.spark.SPARK_VERSION)
    val eventJsonString = jsonProtocol.sparkEventToJsonString(metadata)
    val metadataJson = eventJsonString + "\n"
    bstream.write(metadataJson.getBytes(StandardCharsets.UTF_8))

    val writer = new OutputStreamWriter(bstream, StandardCharsets.UTF_8)
    Utils.tryWithSafeFinally {
      events.foreach(e => writer.write(jsonProtocol.sparkEventToJsonString(e) + "\n"))
    } {
      writer.close()
    }
  }

  private def createEmptyFile(file: File) = {
    new FileOutputStream(file).close()
  }

  private def createTestConf(
      inMemory: Boolean = false,
      useHybridStore: Boolean = false): SparkConf = {
    val conf = new SparkConf()
      .set(HISTORY_LOG_DIR, testGlob)
      .set(FAST_IN_PROGRESS_PARSING, true)

    if (!inMemory) {
      conf.set(LOCAL_STORE_DIR, testDirs(0).getAbsolutePath())
    }
    conf.set(HYBRID_STORE_ENABLED, useHybridStore)
    conf.set(HYBRID_STORE_DISK_BACKEND.key, diskBackend.toString)
    conf.set(LOCAL_STORE_SERIALIZER.key, serializer.toString)

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
    extends GlobFsHistoryProvider(conf, clock) {

    @volatile var inSafeMode = true

    // Skip initialization so that we can manually start the safe mode check thread.
    private[history] override def initialize(): Thread = null

    private[history] override def isFsInSafeMode(): Boolean = inSafeMode

  }

}
