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
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.zip.{ZipInputStream, ZipOutputStream}

import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.json4s.jackson.JsonMethods._
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, spy, verify}
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.io._
import org.apache.spark.scheduler._
import org.apache.spark.security.GroupMappingServiceProvider
import org.apache.spark.util.{Clock, JsonProtocol, ManualClock, Utils}

class FsHistoryProviderSuite extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {

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

  test("Parse application logs") {
    val provider = new FsHistoryProvider(createTestConf())

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
          List(ApplicationAttemptInfo(None, start, end, lastMod, user, completed)))
      }

      list(0) should be (makeAppInfo("new-app-complete", newAppComplete.getName(), 1L, 5L,
        newAppComplete.lastModified(), "test", true))
      list(1) should be (makeAppInfo("new-complete-lzf", newAppCompressedComplete.getName(),
        1L, 4L, newAppCompressedComplete.lastModified(), "test", true))
      list(2) should be (makeAppInfo("new-incomplete", newAppIncomplete.getName(), 1L, -1L,
        newAppIncomplete.lastModified(), "test", false))

      // Make sure the UI can be rendered.
      list.foreach { case info =>
        val appUi = provider.getAppUI(info.id, None)
        appUi should not be null
        appUi should not be None
      }
    }
  }

  test("SPARK-3697: ignore directories that cannot be read.") {
    // setReadable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)
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

  test("Parse logs that application is not started") {
    val provider = new FsHistoryProvider((createTestConf()))

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

  private def createTestConf(): SparkConf = {
    new SparkConf().set("spark.history.fs.logDirectory", testDir.getAbsolutePath())
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

