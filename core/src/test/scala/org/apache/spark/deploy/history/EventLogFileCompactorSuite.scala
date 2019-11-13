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

import java.io.File

import org.apache.hadoop.fs.{FileStatus, Path}
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN
import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol

class EventLogFileCompactorSuite extends SparkFunSuite {
  private val sparkConf = testSparkConf()
  private val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

  test("No event log files") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      assert(Seq.empty[FileStatus] === compactor.compact(Seq.empty))
    }
  }

  test("No compact file, less origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val logPath1 = writeDummyEventLogFile(dir, 1)
      val logPath2 = writeDummyEventLogFile(dir, 2)

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      val fileStatuses = Seq(logPath1, logPath2).map { p => fs.getFileStatus(new Path(p))  }
      val filesToRead = compactor.compact(fileStatuses)

      assert(filesToRead.map(_.getPath) === fileStatuses.map(_.getPath))
    }
  }

  test("No compact file, more origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val logPaths = (1 to 5).map { idx => writeDummyEventLogFile(dir, idx) }

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      val fileStatuses = logPaths.map { p => fs.getFileStatus(new Path(p))  }
      val filesToRead = compactor.compact(fileStatuses)

      // 3 (max file to retain) + 1 (compacted file)
      assert(filesToRead.length === 4)
      val originalFilesToRead = filesToRead.takeRight(3)
      val originFileToCompact = fileStatuses.takeRight(4).head.getPath
      val compactFilePath = filesToRead.head.getPath

      assert(compactFilePath.getName === originFileToCompact.getName + EventLogFileWriter.COMPACTED)
      assert(originalFilesToRead.map(_.getPath) === fileStatuses.takeRight(3).map(_.getPath))

      // compacted files will be removed
      fileStatuses.take(2).foreach { status => assert(!fs.exists(status.getPath)) }
      filesToRead.foreach { status => assert(fs.exists(status.getPath)) }
    }
  }

  test("compact file exists, less origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val logPaths = (1 to 5).map { idx => writeDummyEventLogFile(dir, idx) }

      val fileToCompact = logPaths(2)
      val compactedPath = new Path(fileToCompact + EventLogFileWriter.COMPACTED)
      assert(fs.rename(new Path(fileToCompact), compactedPath))

      val newLogPaths = logPaths.take(2) ++ Seq(compactedPath.toString) ++
        logPaths.takeRight(2)

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      val fileStatuses = newLogPaths.map { p => fs.getFileStatus(new Path(p))  }
      val filesToRead = compactor.compact(fileStatuses)

      // filesToRead will start from rightmost compact file, but no new compact file
      assert(filesToRead.map(_.getPath) === fileStatuses.takeRight(3).map(_.getPath))
    }
  }

  test("compact file exists, more origin files available than max files to retain") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val logPaths = (1 to 10).map { idx => writeDummyEventLogFile(dir, idx) }

      val fileToCompact = logPaths(2)
      val compactedPath = new Path(fileToCompact + EventLogFileWriter.COMPACTED)
      assert(fs.rename(new Path(fileToCompact), compactedPath))

      val newLogPaths = logPaths.take(2) ++ Seq(compactedPath.toString) ++
        logPaths.takeRight(7)

      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs)
      val fileStatuses = newLogPaths.map { p => fs.getFileStatus(new Path(p))  }
      val filesToRead = compactor.compact(fileStatuses)

      // 3 (max file to retain) + 1 (compacted file)
      assert(filesToRead.length === 4)
      val originalFilesToRead = filesToRead.takeRight(3)
      val originFileToCompact = fileStatuses.takeRight(4).head.getPath
      val compactFilePath = filesToRead.head.getPath

      assert(compactFilePath.getName === originFileToCompact.getName + EventLogFileWriter.COMPACTED)
      assert(originalFilesToRead.map(_.getPath) === fileStatuses.takeRight(3).map(_.getPath))

      // compacted files will be removed - we don't check files "before" the rightmost compact file
      fileStatuses.drop(2).dropRight(3).foreach { status => assert(!fs.exists(status.getPath)) }
      filesToRead.foreach { status => assert(fs.exists(status.getPath)) }
    }
  }

  // TODO: Would we want to verify the result of compact file here? Because we already have it in
  //   FilteredEventLogFileRewriterSuite, and adding tests everywhere seem to be redundant.

  private def writeDummyEventLogFile(dir: File, idx: Int): String = {
    // to simplify the code, we don't concern about file name being matched with the naming rule
    // of event log file
    val writer = new SingleEventLogFileWriter(s"app$idx", None, dir.toURI, sparkConf, hadoopConf)
    writer.start()
    writer.writeEvent(convertEvent(
      SparkListenerApplicationStart("app", Some("app"), 0, "user", None)), flushLogger = true)
    writer.stop()
    writer.logPath
  }

  private def convertEvent(event: SparkListenerEvent): String = {
    compact(render(JsonProtocol.sparkEventToJson(event)))
  }

  private def testSparkConf(): SparkConf = {
    new SparkConf().set(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN, 3)
  }
}
