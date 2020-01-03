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

import scala.io.{Codec, Source}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.{SparkConf, SparkFunSuite, Success}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper.writeEventsToRollingWriter
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config.{EVENT_LOG_COMPACTION_SCORE_THRESHOLD, EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.status.ListenerEventsTestHelper._
import org.apache.spark.util.{JsonProtocol, Utils}

class EventLogFileCompactorSuite extends SparkFunSuite {
  private val sparkConf = testSparkConf()
  private val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

  test("No event log files") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)

      assertNoCompaction(fs, Seq.empty, compactor.compact(Seq.empty),
        CompactionResult.NOT_ENOUGH_FILES)
    }
  }

  test("No compact file, less origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 2).map(_ => testEvent): _*)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      assertNoCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        CompactionResult.NOT_ENOUGH_FILES)
    }
  }

  test("No compact file, more origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, sparkConf, hadoopConf,
        (1 to 5).map(_ => testEvent): _*)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
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
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      assertNoCompaction(fs, newFileStatuses, compactor.compact(newFileStatuses),
        CompactionResult.NOT_ENOUGH_FILES)
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
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      assertNoCompaction(fs, newFileStatuses, compactor.compact(newFileStatuses),
        CompactionResult.NOT_ENOUGH_FILES)
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
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
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

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      assertCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        expectedNumOfFilesCompacted = 2)

      val expectCompactFileBasePath = fileStatuses.take(2).last.getPath
      val compactFilePath = getCompactFilePath(expectCompactFileBasePath)
      Utils.tryWithResource(EventLogFileReader.openEventLog(compactFilePath, fs)) { is =>
        val lines = Source.fromInputStream(is)(Codec.UTF8).getLines().toList
        assert(lines.length === 2, "Compacted file should have only two events being accepted")
        lines.foreach { line =>
          val event = JsonProtocol.sparkEventFromJson(parse(line))
          assert(!event.isInstanceOf[SparkListenerJobStart] &&
            !event.isInstanceOf[SparkListenerJobEnd])
        }
      }
    }
  }

  test("Don't compact file if score is lower than threshold") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val newConf = sparkConf.set(EVENT_LOG_COMPACTION_SCORE_THRESHOLD, 0.7d)

      // only one of two tasks is finished, which would score 0.5d
      val tasks = createTasks(2, Array("exec1"), 0L).map(createTaskStartEvent(_, 1, 0))

      val fileStatuses = writeEventsToRollingWriter(fs, "app", dir, newConf, hadoopConf,
        tasks,
        Seq(SparkListenerTaskEnd(1, 0, "taskType", Success, tasks.head.taskInfo,
          new ExecutorMetrics, null)),
        testEvent,
        testEvent,
        testEvent)

      val compactor = new EventLogFileCompactor(newConf, hadoopConf, fs)
      assertNoCompaction(fs, fileStatuses, compactor.compact(fileStatuses),
        CompactionResult.LOW_SCORE_FOR_COMPACTION)
    }
  }

  private def assertCompaction(
      fs: FileSystem,
      originalFiles: Seq[FileStatus],
      compactRet: (CompactionResult.Value, Option[Long]),
      expectedNumOfFilesCompacted: Int): Unit = {
    assert(CompactionResult.SUCCESS === compactRet._1)

    val expectRetainedFiles = originalFiles.drop(expectedNumOfFilesCompacted)
    expectRetainedFiles.foreach { status => assert(fs.exists(status.getPath)) }

    val expectRemovedFiles = originalFiles.take(expectedNumOfFilesCompacted)
    expectRemovedFiles.foreach { status => assert(!fs.exists(status.getPath)) }

    val expectCompactFileBasePath = originalFiles.take(expectedNumOfFilesCompacted).last.getPath
    val expectCompactFileIndex = RollingEventLogFilesWriter.getEventLogFileIndex(
      expectCompactFileBasePath.getName)
    assert(Some(expectCompactFileIndex) === compactRet._2)

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
      compactRet: (CompactionResult.Value, Option[Long]),
      expectedCompactRet: CompactionResult.Value): Unit = {
  assert(compactRet._1 === expectedCompactRet)
    assert(None === compactRet._2)
    originalFiles.foreach { status => assert(fs.exists(status.getPath)) }
  }

  private def testEvent: Seq[SparkListenerEvent] =
    Seq(SparkListenerApplicationStart("app", Some("app"), 0, "user", None))

  private def testSparkConf(): SparkConf = {
    new SparkConf()
      .set(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN, 3)
      // to simplify the tests, we set the score threshold as 0.0d
      // individual test can override the value to verify the functionality
      .set(EVENT_LOG_COMPACTION_SCORE_THRESHOLD, 0.0d)
  }
}
