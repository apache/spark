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

package org.apache.spark.scheduler

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException}
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.internal.config.History
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec}
import org.apache.spark.util.{JsonProtocol, JsonProtocolSuite, Utils}

/**
 * Test whether ReplayListenerBus replays events from logs correctly.
 */
class ReplayListenerSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {
  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private var testDir: File = _

  before {
    testDir = Utils.createTempDir()
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Simple replay") {
    val logFilePath = getFilePath(testDir, "events.txt")
    val fstream = fileSystem.create(logFilePath)
    val fwriter = new OutputStreamWriter(fstream, StandardCharsets.UTF_8)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    Utils.tryWithResource(new PrintWriter(fwriter)) { writer =>
      // scalastyle:off println
      writer.println(JsonProtocol.sparkEventToJsonString(applicationStart))
      writer.println(JsonProtocol.sparkEventToJsonString(applicationEnd))
      // scalastyle:on println
    }

    val conf = getLoggingConf(logFilePath)
    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventBufferingListener
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, logFilePath.toString)
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJsonString(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJsonString(applicationEnd))
  }

  test("Over-long event log lines are skipped instead of materialized") {
    val logFilePath = getFilePath(testDir, "events.txt")
    val fstream = fileSystem.create(logFilePath)
    val fwriter = new OutputStreamWriter(fstream, StandardCharsets.UTF_8)
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    val maxLineLength = 8 * 1024
    val hugeLine = "x" * (maxLineLength + 1024)
    Utils.tryWithResource(new PrintWriter(fwriter)) { writer =>
      // scalastyle:off println
      writer.println(JsonProtocol.sparkEventToJsonString(applicationStart))
      writer.println(hugeLine)
      writer.println(JsonProtocol.sparkEventToJsonString(applicationEnd))
      // scalastyle:on println
    }

    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventBufferingListener
    try {
      val replayer = new ReplayListenerBus(maxLineLength = maxLineLength)
      replayer.addListener(eventMonster)
      assert(replayer.replay(logData, logFilePath.toString))
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJsonString(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJsonString(applicationEnd))
  }

  test("Replay line limit uses UTF-8 bytes") {
    val start = SparkListenerApplicationStart("x" * 6000, None, 125L, "user", None)
    val json = JsonProtocol.sparkEventToJsonString(start)
    assert(json.getBytes(StandardCharsets.UTF_8).length < 8 * 1024)
    val listener = new EventBufferingListener
    val conf = new SparkConf(false).set(History.EVENT_LOG_MAX_LINE_LENGTH.key, "8k")
    val bus = new ReplayListenerBus(ReplayListenerBus.maxLineLength(conf))
    bus.addListener(listener)
    val input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
    assert(bus.replay(input, "ascii"))
    assert(listener.loggedEvents.toSeq == Seq(json))
  }

  test("Replay line limit handles raw UTF-8 boundaries and line endings") {
    // scalastyle:off nonascii
    val suffixes = Seq("x", "\u00e9", "\u4e2d", "\ud83d\ude00")
    // scalastyle:on nonascii
    for {
      suffix <- suffixes
      padding <- Seq(8, 8190, 8191, 8192)
      ending <- Seq("\n", "\r\n", "")
      delta <- -3 to 2
    } {
      // Put the limit inside the final code point, and split UTF-8 sequences across read buffers.
      val line = "x" * padding + suffix
      val bytes = line.getBytes(StandardCharsets.UTF_8)
      val limit = bytes.length + delta
      val seen = new ArrayBuffer[String]
      val bus = new ReplayListenerBus(limit)
      val input = new ByteArrayInputStream((line + ending).getBytes(StandardCharsets.UTF_8))
      withClue(s"codePoint=${suffix.codePointAt(0)} padding=$padding " +
          s"ending=${ending.map(_.toInt).mkString(",")} delta=$delta: ") {
        // Observe raw lines before JSON parsing, without letting Jackson escape the test input.
        assert(bus.replay(input, "utf8", eventsFilter = line => {
          seen += line
          false
        }))
        assert(seen.toSeq == (if (delta >= 0) Seq(line) else Seq.empty))
      }
    }
  }

  test("Replay line buffer stops retaining oversized content") {
    for (limit <- Seq(0, 1, 8, 1024)) {
      val buffer = new ReplayListenerBus.BoundedLineBuffer(limit)
      for (_ <- 0 until 10000) {
        buffer.append('x')
        assert(buffer.length <= limit + 1)
      }
      assert(buffer.length == limit + 1)
      assert(buffer.result().isEmpty)
    }
  }

  test("Replay line limit defaults and maximum supported configuration") {
    val conf = new SparkConf(false)
    assert(conf.get(History.EVENT_LOG_MAX_LINE_LENGTH) == 256L * 1024 * 1024)
    assert(ReplayListenerBus.maxLineLength(conf) == ReplayListenerBus.DEFAULT_MAX_LINE_LENGTH)
    for (value <- Seq("0", "-1", "2147483647", "3g")) {
      conf.set(History.EVENT_LOG_MAX_LINE_LENGTH.key, value)
      assert(ReplayListenerBus.maxLineLength(conf) == Int.MaxValue)
    }
  }

  test("Replay preserves physical line numbers after skipping long lines") {
    val end = JsonProtocol.sparkEventToJsonString(SparkListenerApplicationEnd(1000L))
    val appender = new LogAppender
    val bus = new ReplayListenerBus(1024)
    val input = new ByteArrayInputStream(
      (end + "\n" + "x" * 2048 + "\n" + "x" * 2048 + "\n{}\n")
        .getBytes(StandardCharsets.UTF_8))
    withLogAppender(appender) {
      assert(!bus.replay(input, "line-numbers"))
    }
    assert(appender.loggingEvents.exists(_.getMessage.getFormattedMessage
      .contains("Malformed line #4: {}")))
  }

  test("Replay logs physical line numbers before rethrowing JSON errors") {
    val end = JsonProtocol.sparkEventToJsonString(SparkListenerApplicationEnd(1000L))
    val mappingError =
      """{"Event":"org.apache.spark.util.TestListenerEvent","foo":"x","bar":[]}"""
    for ((malformed, errorClass) <- Seq(
        ("{bad", classOf[JsonParseException]),
        (mappingError, classOf[MismatchedInputException]))) {
      val appender = new LogAppender
      val bus = new ReplayListenerBus(1024)
      val input = new ByteArrayInputStream(
        Seq(end, "x" * 2048, "x" * 2048, malformed, end).mkString("\n")
          .getBytes(StandardCharsets.UTF_8))
      withLogAppender(appender) {
        val error = intercept[JsonProcessingException] {
          bus.replay(input, "json-errors", maybeTruncated = true)
        }
        assert(errorClass.isInstance(error))
      }
      assert(appender.loggingEvents.exists(_.getMessage.getFormattedMessage
        .contains("Exception parsing Spark event log: json-errors at line 4")))
    }
  }

  test("Replay propagates stream IO errors without reporting a stale parse line") {
    val error = new IOException("read failure")
    val input = new InputStream {
      override def read(): Int = throw error
    }
    val appender = new LogAppender
    withLogAppender(appender) {
      assert(intercept[IOException] {
        new ReplayListenerBus().replay(input, "read-error")
      } eq error)
    }
    assert(!appender.loggingEvents.exists(_.getMessage.getFormattedMessage
      .contains("Exception parsing Spark event log")))
  }

  test("Replay preserves line numbers for truncated logs and filtered iterators") {
    val end = JsonProtocol.sparkEventToJsonString(SparkListenerApplicationEnd(1000L))
    val truncated = "{\"Event\":"
    val appender = new LogAppender
    val bus = new ReplayListenerBus(1024)
    val input = new ByteArrayInputStream(
      ("x" * 2048 + "\n" + end + "\n" + truncated).getBytes(StandardCharsets.UTF_8))
    withLogAppender(appender) {
      assert(bus.replay(input, "truncated", maybeTruncated = true,
        eventsFilter = _ != end))
      assert(bus.replay(Iterator(end, end, truncated), "iterator", maybeTruncated = true,
        eventsFilter = _ != end))
    }
    val warnings = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
      .filter(_.contains("Got JsonParseException"))
    assert(warnings.size == 2)
    assert(warnings.forall(_.contains("at line 3,")))
  }

  test("Replay still rejects malformed UTF-8 in skipped lines") {
    val bytes = Array.fill[Byte](20 * 1024)('x'.toByte) ++ Array(0xff.toByte, '\n'.toByte)
    val bus = new ReplayListenerBus(1024)
    intercept[java.nio.charset.MalformedInputException] {
      bus.replay(new ByteArrayInputStream(bytes), "invalid-utf8")
    }
  }

  /**
   * Test replaying compressed spark history file that internally throws an EOFException.  To
   * avoid sensitivity to the compression specifics the test forces an EOFException to occur
   * while reading bytes from the underlying stream (such as observed in actual history files
   * in some cases) and forces specific failure handling.  This validates correctness in both
   * cases when maybeTruncated is true or false.
   */
  test("Replay compressed inprogress log file succeeding on partial read") {
    val buffered = new ByteArrayOutputStream
    val codec = new LZ4CompressionCodec(new SparkConf())
    val compstream = codec.compressedContinuousOutputStream(buffered)
    val cwriter = new OutputStreamWriter(compstream, StandardCharsets.UTF_8)
    Utils.tryWithResource(new PrintWriter(cwriter)) { writer =>

      val applicationStart = SparkListenerApplicationStart("AppStarts", None,
        125L, "Mickey", None)
      val applicationEnd = SparkListenerApplicationEnd(1000L)

      // scalastyle:off println
      writer.println(JsonProtocol.sparkEventToJsonString(applicationStart))
      writer.println(JsonProtocol.sparkEventToJsonString(applicationEnd))
      // scalastyle:on println
    }

    val logFilePath = getFilePath(testDir, "events.lz4.inprogress")
    val bytes = buffered.toByteArray
    Utils.tryWithResource(fileSystem.create(logFilePath)) { fstream =>
      fstream.write(bytes, 0, buffered.size)
    }

    // Read the compressed .inprogress file and verify only first event was parsed.
    val conf = getLoggingConf(logFilePath)
    val replayer = new ReplayListenerBus()

    val eventMonster = new EventBufferingListener
    replayer.addListener(eventMonster)

    // Verify the replay returns the events given the input maybe truncated.
    val logData = EventLogFileReader.openEventLog(logFilePath, fileSystem)
    Utils.tryWithResource(new EarlyEOFInputStream(logData, buffered.size - 10)) { failingStream =>
      replayer.replay(failingStream, logFilePath.toString, true)

      assert(eventMonster.loggedEvents.size === 1)
      assert(failingStream.didFail)
    }

    // Verify the replay throws the EOF exception since the input may not be truncated.
    val logData2 = EventLogFileReader.openEventLog(logFilePath, fileSystem)
    Utils.tryWithResource(new EarlyEOFInputStream(logData2, buffered.size - 10)) { failingStream2 =>
      intercept[EOFException] {
        replayer.replay(failingStream2, logFilePath.toString, false)
      }
    }
  }

  test("Replay incompatible event log") {
    val logFilePath = getFilePath(testDir, "incompatible.txt")
    val fstream = fileSystem.create(logFilePath)
    val fwriter = new OutputStreamWriter(fstream, StandardCharsets.UTF_8)
    val applicationStart = SparkListenerApplicationStart("Incompatible App", None,
      125L, "UserUsingIncompatibleVersion", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)
    Utils.tryWithResource(new PrintWriter(fwriter)) { writer =>
      // scalastyle:off println
      writer.println(JsonProtocol.sparkEventToJsonString(applicationStart))
      writer.println("""{"Event":"UnrecognizedEventOnlyForTest","Timestamp":1477593059313}""")
      writer.println(JsonProtocol.sparkEventToJsonString(applicationEnd))
      // scalastyle:on println
    }

    val conf = getLoggingConf(logFilePath)
    val logData = fileSystem.open(logFilePath)
    val eventMonster = new EventBufferingListener
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, logFilePath.toString)
    } finally {
      logData.close()
    }
    assert(eventMonster.loggedEvents.size === 2)
    assert(eventMonster.loggedEvents(0) === JsonProtocol.sparkEventToJsonString(applicationStart))
    assert(eventMonster.loggedEvents(1) === JsonProtocol.sparkEventToJsonString(applicationEnd))
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay") {
    testApplicationReplay()
  }

  // This assumes the correctness of EventLoggingListener
  test("End-to-end replay with compression") {
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testApplicationReplay(Some(codec))
    }
  }


  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  /**
   * Test end-to-end replaying of events.
   *
   * This test runs a few simple jobs with event logging enabled, and compares each emitted
   * event to the corresponding event replayed from the event logs. This test makes the
   * assumption that the event logging behavior is correct (tested in a separate suite).
   */
  private def testApplicationReplay(codecName: Option[String] = None): Unit = {
    val logDir = new File(testDir.getAbsolutePath, "test-replay")
    // Here, it creates `Path` from the URI instead of the absolute path for the explicit file
    // scheme so that the string representation of this `Path` has leading file scheme correctly.
    val logDirPath = new Path(logDir.toURI)
    fileSystem.mkdirs(logDirPath)

    val conf = getLoggingConf(logDirPath, codecName)
    sc = new SparkContext("local-cluster[2,1,1024]", "Test replay", conf)

    // Run a few jobs
    sc.parallelize(1 to 100, 1).count()
    sc.parallelize(1 to 100, 2).map(i => (i, i)).count()
    sc.parallelize(1 to 100, 3).map(i => (i, i)).groupByKey().count()
    sc.parallelize(1 to 100, 4).map(i => (i, i)).groupByKey().persist().count()
    sc.stop()

    // Prepare information needed for replay
    val applications = fileSystem.listStatus(logDirPath)
    assert(applications != null && applications.nonEmpty)
    val eventLog = applications.sortBy(_.getModificationTime).last
    assert(!eventLog.isDirectory)

    // Replay events
    val logData = EventLogFileReader.openEventLog(eventLog.getPath(), fileSystem)
    val eventMonster = new EventBufferingListener
    try {
      val replayer = new ReplayListenerBus()
      replayer.addListener(eventMonster)
      replayer.replay(logData, eventLog.getPath().toString)
    } finally {
      logData.close()
    }

    // Verify the same events are replayed in the same order
    assert(sc.eventLogger.isDefined)
    val originalEvents = sc.eventLogger.get.loggedEvents
      .map(JsonProtocol.sparkEventFromJson)
    val replayedEvents = eventMonster.loggedEvents
      .map(JsonProtocol.sparkEventFromJson)
    originalEvents.zip(replayedEvents).foreach { case (e1, e2) =>
      JsonProtocolSuite.assertEquals(e1, e1)
    }
  }

  private def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
  }

  /**
   * A simple listener that buffers all the events it receives.
   */
  private class EventBufferingListener extends SparkFirehoseListener {

    private[scheduler] val loggedEvents = new ArrayBuffer[String]

    override def onEvent(event: SparkListenerEvent): Unit = {
      val eventJson = JsonProtocol.sparkEventToJsonString(event)
      loggedEvents += eventJson
    }
  }

  /*
   * This is a dummy input stream that wraps another input stream but ends prematurely when
   * reading at the specified position, throwing an EOFException.
   */
  private class EarlyEOFInputStream(in: InputStream, failAtPos: Int) extends InputStream {
    private val countDown = new AtomicInteger(failAtPos)

    def didFail: Boolean = countDown.get == 0

    @throws[IOException]
    override def read(): Int = {
      if (countDown.get == 0) {
        throw new EOFException("Stream ended prematurely")
      }
      countDown.decrementAndGet()
      in.read()
    }

    override def close(): Unit = in.close()
  }
}
