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

import scala.collection.mutable
import scala.io.{Codec, Source}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper.writeEventsToRollingWriter
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.status.ListenerEventsTestHelper._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{JsonProtocol, Utils}

class EventLogFileCompactorSuite extends SparkFunSuite {
  import EventLogFileCompactorSuite._

  private val sparkConf = new SparkConf()
  private val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

  test("No event log files") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)

      assertNoCompaction(fs, Seq.empty, compactor.compact(Seq.empty),
        CompactionResultCode.NOT_ENOUGH_FILES)
    }
  }

  test("No compact file, less origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 2).map(_ => testEvent): _*)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertNoCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        CompactionResultCode.NOT_ENOUGH_FILES)
    }
  }

  test("No compact file, more origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 5).map(_ => testEvent): _*)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        expectedNumOfFilesCompacted = 2)
    }
  }

  test("compact file exists, less origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 2).map(_ => testEvent): _*)

      val fileToCompact = fileStatuses.head.getPath
      val compactedPath = new Path(fileToCompact.getParent,
        fileToCompact.getName + EventLogFileWriter.COMPACTED)
      assert(fs.rename(fileToCompact, compactedPath))

      val newFileStatuses = Seq(fs.getFileStatus(compactedPath)) ++ fileStatuses.drop(1)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertNoCompaction(fs, newFileStatuses, compactor.compact(newFileStatuses),
        CompactionResultCode.NOT_ENOUGH_FILES)
    }
  }

  test("compact file exists, number of origin files are same as max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 4).map(_ => testEvent): _*)

      val fileToCompact = fileStatuses.head.getPath
      val compactedPath = new Path(fileToCompact.getParent,
        fileToCompact.getName + EventLogFileWriter.COMPACTED)
      assert(fs.rename(fileToCompact, compactedPath))

      val newFileStatuses = Seq(fs.getFileStatus(compactedPath)) ++ fileStatuses.drop(1)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertNoCompaction(fs, newFileStatuses, compactor.compact(newFileStatuses),
        CompactionResultCode.NOT_ENOUGH_FILES)
    }
  }

  test("compact file exists, more origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 10).map(_ => testEvent): _*)

      val fileToCompact = fileStatuses.head.getPath
      val compactedPath = new Path(fileToCompact.getParent,
        fileToCompact.getName + EventLogFileWriter.COMPACTED)
      assert(fs.rename(fileToCompact, compactedPath))

      val newFileStatuses = Seq(fs.getFileStatus(compactedPath)) ++ fileStatuses.drop(1)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertCompaction(fs, newFileStatuses, compactor.compact(newFileStatuses),
        expectedNumOfFilesCompacted = 7)
    }
  }

  test("events for finished job are dropped in new compact file") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      // 1, 2 will be compacted into one file, 3~5 are dummies to ensure max files to retain
      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        Seq(
          SparkListenerExecutorAdded(0, "exec1", new ExecutorInfo("host1", 1, Map.empty)),
          SparkListenerJobStart(1, 0, Seq.empty)),
        Seq(
          SparkListenerJobEnd(1, 1, JobSucceeded),
          SparkListenerExecutorAdded(2, "exec2", new ExecutorInfo("host2", 1, Map.empty))),
        testEvent,
        testEvent,
        testEvent)

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      assertCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        expectedNumOfFilesCompacted = 2)

      val expectCompactFileBasePath = fileStatuses.take(2).last.getPath
      val compactFilePath = getCompactFilePath(expectCompactFileBasePath)
      Utils.tryWithResource(EventLogFileReader.openEventLog(compactFilePath, fs)) { is =>
        val lines = Source.fromInputStream(is)(Codec.UTF8).getLines().toList
        assert(lines.length === 2, "Compacted file should have only two events being accepted")
        lines.foreach { line =>
          val event = JsonProtocol.sparkEventFromJson(line)
          assert(!event.isInstanceOf[SparkListenerJobStart] &&
            !event.isInstanceOf[SparkListenerJobEnd])
        }
      }
    }
  }

  test("Don't compact file if score is lower than threshold") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      // job 1 having 4 tasks
      val rddsForStage1 = createRddsWithId(1 to 2)
      val stage1 = createStage(1, rddsForStage1, Nil)
      val tasks = createTasks(4, Array("exec1"), 0L).map(createTaskStartEvent(_, 1, 0))

      // job 2 having 4 tasks
      val rddsForStage2 = createRddsWithId(3 to 4)
      val stage2 = createStage(2, rddsForStage2, Nil)
      val tasks2 = createTasks(4, Array("exec1"), 0L).map(createTaskStartEvent(_, 2, 0))

      // here job 1 is finished and job 2 is still live, hence half of total tasks are considered
      // as live
      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        Seq(SparkListenerJobStart(1, 0, Seq(stage1)), SparkListenerStageSubmitted(stage1)),
        tasks,
        Seq(SparkListenerJobStart(2, 0, Seq(stage2)), SparkListenerStageSubmitted(stage2)),
        tasks2,
        Seq(SparkListenerJobEnd(1, 0, JobSucceeded)),
        testEvent,
        testEvent,
        testEvent)

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, 0.7d)
      assertNoCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        CompactionResultCode.LOW_SCORE_FOR_COMPACTION)
    }
  }

  test("rewrite files with test filters") {
    class TestEventFilter1 extends EventFilter {
      override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = {
        case _: SparkListenerApplicationEnd => true
        case _: SparkListenerBlockManagerAdded => true
        case _: SparkListenerApplicationStart => false
      }

      override def statistics(): Option[EventFilter.FilterStatistics] = None
    }

    class TestEventFilter2 extends EventFilter {
      override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = {
        case _: SparkListenerApplicationEnd => true
        case _: SparkListenerEnvironmentUpdate => true
        case _: SparkListenerNodeExcluded => true
        case _: SparkListenerBlockManagerAdded => false
        case _: SparkListenerApplicationStart => false
        case _: SparkListenerNodeUnexcluded => false
      }

      override def statistics(): Option[EventFilter.FilterStatistics] = None
    }

    def writeEventToWriter(writer: EventLogFileWriter, event: SparkListenerEvent): String = {
      val line = EventLogTestHelper.convertEvent(event)
      writer.writeEvent(line, flushLogger = true)
      line
    }

    withTempDir { tempDir =>
      val sparkConf = new SparkConf
      val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
      val fs = new Path(tempDir.getAbsolutePath).getFileSystem(hadoopConf)

      val writer = new SingleEventLogFileWriter("app", None, tempDir.toURI, sparkConf, hadoopConf)
      writer.start()

      val expectedLines = new mutable.ArrayBuffer[String]

      // filterApplicationEnd: Some(true) & Some(true) => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerApplicationEnd(0))

      // filterBlockManagerAdded: Some(true) & Some(false) => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerBlockManagerAdded(
        0, BlockManagerId("1", "host1", 1), 10))

      // filterApplicationStart: Some(false) & Some(false) => filter out
      writeEventToWriter(writer, SparkListenerApplicationStart("app", None, 0, "user", None))

      // filterNodeExcluded: None & Some(true) => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerNodeExcluded(0, "host1", 1))

      // filterNodeUnexcluded: None & Some(false) => filter out
      writeEventToWriter(writer, SparkListenerNodeUnexcluded(0, "host1"))

      // other events: None & None => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerUnpersistRDD(0))

      writer.stop()

      val filters = Seq(new TestEventFilter1, new TestEventFilter2)

      val logPath = new Path(writer.logPath)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs,
        TEST_ROLLING_MAX_FILES_TO_RETAIN, TEST_COMPACTION_SCORE_THRESHOLD)
      val newPath = compactor.rewrite(filters, Seq(fs.getFileStatus(logPath)))
      assert(new Path(newPath).getName === logPath.getName + EventLogFileWriter.COMPACTED)

      Utils.tryWithResource(EventLogFileReader.openEventLog(new Path(newPath), fs)) { is =>
        val lines = Source.fromInputStream(is)(Codec.UTF8).getLines()
        var linesLength = 0
        lines.foreach { line =>
          linesLength += 1
          assert(expectedLines.contains(line))
        }
        assert(linesLength === expectedLines.length)
      }
    }
  }

  private def assertCompaction(
      fs: FileSystem,
      originalFiles: Seq[FileStatus],
      compactRet: CompactionResult,
      expectedNumOfFilesCompacted: Int): Unit = {
    assert(CompactionResultCode.SUCCESS === compactRet.code)

    val expectRetainedFiles = originalFiles.drop(expectedNumOfFilesCompacted)
    expectRetainedFiles.foreach { status => assert(fs.exists(status.getPath)) }

    val expectRemovedFiles = originalFiles.take(expectedNumOfFilesCompacted)
    expectRemovedFiles.foreach { status => assert(!fs.exists(status.getPath)) }

    val expectCompactFileBasePath = originalFiles.take(expectedNumOfFilesCompacted).last.getPath
    val expectCompactFileIndex = RollingEventLogFilesWriter.getEventLogFileIndex(
      expectCompactFileBasePath.getName)
    assert(Some(expectCompactFileIndex) === compactRet.compactIndex)

    val expectCompactFilePath = getCompactFilePath(expectCompactFileBasePath)
    assert(fs.exists(expectCompactFilePath))
  }

  private def getCompactFilePath(expectCompactFileBasePath: Path): Path = {
    new Path(expectCompactFileBasePath.getParent,
      expectCompactFileBasePath.getName + EventLogFileWriter.COMPACTED)
  }

  private def assertNoCompaction(
      fs: FileSystem,
      originalFiles: Seq[FileStatus],
      compactRet: CompactionResult,
      expectedCompactRet: CompactionResultCode.Value): Unit = {
    assert(expectedCompactRet === compactRet.code)
    assert(None === compactRet.compactIndex)
    originalFiles.foreach { status => assert(fs.exists(status.getPath)) }
  }

  private def testEvent: Seq[SparkListenerEvent] =
    Seq(SparkListenerApplicationStart("app", Some("app"), 0, "user", None))
}

object EventLogFileCompactorSuite {
  val TEST_ROLLING_MAX_FILES_TO_RETAIN = 3

  // To simplify the tests, we set the score threshold as 0.0d.
  // Individual test can use the other value to verify the functionality.
  val TEST_COMPACTION_SCORE_THRESHOLD = 0.0d
}
