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
import java.util.concurrent.TimeUnit
import java.util.zip.{ZipInputStream, ZipOutputStream}

import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.json4s.jackson.JsonMethods._
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, spy, verify}
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.history.config._
import org.apache.spark.internal.Logging
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.security.GroupMappingServiceProvider
import org.apache.spark.status.AppStatusStore
import org.apache.spark.util.{Clock, JsonProtocol, ManualClock, Utils}

class FsHistoryProviderSuite extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {

  private var testDir: File = null

  before {
    testDir = Utils.createTempDir(namePrefix = s"a b%20c+d")
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
          completed: Boolean): ApplicationHistoryInfo = {
        ApplicationHistoryInfo(id, name,
          List(ApplicationAttemptInfo(None, start, end, lastMod, user, completed, "")))
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
      list.foreach { case info =>
        val appUi = provider.getAppUI(info.id, None)
        appUi should not be null
        appUi should not be None
      }
    }
  }

  test("SPARK-3697: ignore files that cannot be read.") {
    // setReadable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)

    class TestFsHistoryProvider extends FsHistoryProvider(
      createTestConf().set("spark.testing", "true")) {
      var mergeApplicationListingCall = 0
      override protected def mergeApplicationListing(fileStatus: FileStatus): Unit = {
        super.mergeApplicationListing(fileStatus)
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
      list should not be (null)
      list.size should be (1)
      list.head.attempts.size should be (3)
      list.head.attempts.head.attemptId should be (Some("attempt3"))
    }

    val app2Attempt1 = newLogFile("app2", Some("attempt1"), inProgress = false)
    writeFile(attempt1, true, None,
      SparkListenerApplicationStart("app2", Some("app2"), 5L, "test", Some("attempt1")),
      SparkListenerApplicationEnd(6L)
      )

    updateAndCheck(provider) { list =>
      list.size should be (2)
      list.head.attempts.size should be (1)
      list.last.attempts.size should be (3)
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

  test("log cleaner for inProgress files") {
    val firstFileModifiedTime = TimeUnit.SECONDS.toMillis(10)
    val secondFileModifiedTime = TimeUnit.SECONDS.toMillis(20)
    val maxAge = TimeUnit.SECONDS.toMillis(40)
    val clock = new ManualClock(0)
    val provider = new FsHistoryProvider(
      createTestConf().set("spark.history.fs.cleaner.maxAge", s"${maxAge}ms"), clock)

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
          "downloadApp1", Some("downloadApp1"), 5000 * i, "test", Some(s"attempt$i")),
        SparkListenerApplicationEnd(5001 * i)
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

      eventually(timeout(1 second), interval(10 millis)) {
        provider.getConfig().keys should not contain ("HDFS State")
      }
    } finally {
      provider.stop()
    }
  }

  test("provider reports error after FS leaves safe mode") {
    testDir.delete()
    val clock = new ManualClock()
    val provider = new SafeModeTestProvider(createTestConf(), clock)
    val errorHandler = mock(classOf[Thread.UncaughtExceptionHandler])
    val initThread = provider.startSafeModeCheckThread(Some(errorHandler))
    try {
      provider.inSafeMode = false
      clock.setTime(10000)

      eventually(timeout(1 second), interval(10 millis)) {
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
      .set("spark.history.ui.acls.enable", "true")
      .set("spark.history.ui.admin.acls", "user1,user2")
      .set("spark.history.ui.admin.acls.groups", "group1")
      .set("spark.user.groups.mapping", classOf[TestGroupsMappingProvider].getName)

    createAndCheck(conf1, ("spark.admin.acls", "user"), ("spark.admin.acls.groups", "group")) {
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
      .set("spark.history.ui.acls.enable", "true")
      .set("spark.history.ui.admin.acls", "user1,user2")
      .set("spark.history.ui.admin.acls.groups", "group1")
      .set("spark.user.groups.mapping", classOf[TestGroupsMappingProvider].getName)
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
      .set("spark.history.ui.acls.enable", "true")
      .set("spark.user.groups.mapping", classOf[TestGroupsMappingProvider].getName)
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
      SparkListenerJobStart(0, 1L, Nil, null),
      SparkListenerApplicationEnd(5L)
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

  /**
    * Validate aggressive clean up removes incomplete or corrupt history files that would
    * otherwise be missed during clean up.  Also validate no behavior change if aggressive
    * clean up is disabled.
    */
  test("SPARK-21571: aggressive clean up removes incomplete history files") {
    createCleanAndCheckIncompleteLogFiles(14, 21, true, true, true, true, true)
    createCleanAndCheckIncompleteLogFiles(14, 21, false, false, false, true, false)
    createCleanAndCheckIncompleteLogFiles(14, 7, true, false, false, false, false)
    createCleanAndCheckIncompleteLogFiles(14, 7, false, false, false, false, false)
  }

  /**
    * Create four test incomplete/corrupt history files and invoke a check and clean cycle that
    * passes followed by one occurring after the max age days rentention window and assert the
    * expected number of history files remain.
    * @param maxAgeDays maximum retention in days, used to simulate current time
    * @param lastModifiedDaysAgo last modified date for test files relative to current time
    * @param aggressiveCleanup aggressive clean up is enabled or not
    * @param expectEmptyInprogressRemoved expect an empty inprogress file to be removed
    * @param expectEmptyCorruptRemoved expect an empty corrupt complete file to be removed
    * @param expectNonEmptyInprogressRemoved expect a non-empty inprogress file to be removed
    * @param expectNonEmptyCorruptRemoved expect a non-empty corrupt complete file to be removed
    */
  private def createCleanAndCheckIncompleteLogFiles(
                                                     maxAgeDays: Long,
                                                     lastModifiedDaysAgo: Long,
                                                     aggressiveCleanup: Boolean,
                                                     expectEmptyInprogressRemoved: Boolean,
                                                     expectEmptyCorruptRemoved: Boolean,
                                                     expectNonEmptyInprogressRemoved: Boolean,
                                                     expectNonEmptyCorruptRemoved: Boolean) = {

    // Set current time as 2 * maximum retention period to allow for expired history files.
    val currentTimeMillis = MILLISECONDS.convert(maxAgeDays * 2, TimeUnit.DAYS)
    val clock = new ManualClock(currentTimeMillis)

    val lastModifiedTime = currentTimeMillis -
      MILLISECONDS.convert(lastModifiedDaysAgo, TimeUnit.DAYS)

    val provider = new FsHistoryProvider(
      createTestConf()
        .set("spark.history.fs.cleaner.aggressive", s"${aggressiveCleanup}")
        .set("spark.history.fs.cleaner.maxAge", s"${maxAgeDays}d")
        .set("spark.testing", "true"),
      clock) {
      override def getNewLastScanTime(): Long = clock.getTimeMillis
    }

    // Create history files
    // 1. 0-byte size files inprogress and corrupt complete files
    // 2. >0 byte size files inprogress and corrupt complete files

    try {
      val logfile1 = newLogFile("emptyInprogressLogFile", None, inProgress = true)
      logfile1.createNewFile
      logfile1.setLastModified(lastModifiedTime)

      val logfile2 = newLogFile("emptyCorruptLogFile", None, inProgress = false)
      logfile2.createNewFile
      logfile2.setLastModified(lastModifiedTime)

      // Create an inprogress log file, has only start record.
      val logfile3 = newLogFile("nonEmptyInprogressLogFile", None, inProgress = true)
      writeFile(logfile3, true, None, SparkListenerApplicationStart(
        "inProgress1", Some("inProgress1"), 3L, "test", Some("attempt1"))
      )
      logfile3.setLastModified(lastModifiedTime)

      // Create an incomplete log file, has an end record but no start record.
      val logfile4 = newLogFile("nonEmptyCorruptLogFile", None, inProgress = false)
      writeFile(logfile4, true, None, SparkListenerApplicationEnd(0))
      logfile4.setLastModified(lastModifiedTime)

      // Simulate checking logs 1 day after initial creation.  This is necessary because the log
      // checker will sometimes use the current time in place of last modified time the first
      // time it encounters an inprogress file to work around certain file system inconsistencies.
      // No history files should clean up in first check and clean pass.
      clock.setTime(lastModifiedTime + MILLISECONDS.convert(1, TimeUnit.DAYS))
      provider.checkForLogs
      provider.cleanLogs
      assert(new File(testDir.toURI).listFiles().size == 4)

      // Simulate checking logs at current time
      clock.setTime(currentTimeMillis)
      provider.checkForLogs
      provider.cleanLogs

      val remainingFiles = new File(testDir.toURI).listFiles.map(_.getName)
      assert(remainingFiles.contains(logfile1.getName) == !expectEmptyInprogressRemoved)
      assert(remainingFiles.contains(logfile2.getName) == !expectEmptyCorruptRemoved)
      assert(remainingFiles.contains(logfile3.getName) == !expectNonEmptyInprogressRemoved)
      assert(remainingFiles.contains(logfile4.getName) == !expectNonEmptyCorruptRemoved)
    } finally {
      new File(testDir.toURI).listFiles().map(_.delete())
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
      .set("spark.history.fs.logDirectory", testDir.getAbsolutePath())

    if (!inMemory) {
      conf.set(LOCAL_STORE_DIR, Utils.createTempDir().getAbsolutePath())
    }

    conf
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

