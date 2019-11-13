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

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{JsonProtocol, Utils}

class FilteredEventLogFileRewriterSuite extends SparkFunSuite {
  test("rewrite files with test filters") {
    def writeEventToWriter(writer: EventLogFileWriter, event: SparkListenerEvent): String = {
      val line = convertEvent(event)
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

      // filterBlockManagerAdded: Some(true) & Some(false) => filter out
      writeEventToWriter(writer, SparkListenerBlockManagerAdded(0, BlockManagerId("1", "host1", 1),
        10))

      // filterApplicationStart: Some(false) & Some(false) => filter out
      writeEventToWriter(writer, SparkListenerApplicationStart("app", None, 0, "user", None))

      // filterNodeBlacklisted: None & Some(true) => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerNodeBlacklisted(0, "host1", 1))

      // filterNodeUnblacklisted: None & Some(false) => filter out
      writeEventToWriter(writer, SparkListenerNodeUnblacklisted(0, "host1"))

      // other events: None & None => filter in
      expectedLines += writeEventToWriter(writer, SparkListenerUnpersistRDD(0))

      writer.stop()

      val filters = Seq(new TestEventFilter1, new TestEventFilter2)

      val rewriter = new FilteredEventLogFileRewriter(sparkConf, hadoopConf, fs, filters)
      val logPath = new Path(writer.logPath)
      val newPath = rewriter.rewrite(Seq(fs.getFileStatus(logPath)))
      assert(new Path(newPath).getName === logPath.getName + EventLogFileWriter.COMPACTED)

      Utils.tryWithResource(EventLogFileReader.openEventLog(new Path(newPath), fs)) { is =>
        val lines = Source.fromInputStream(is)(Codec.UTF8).getLines()
        var linesLength = 0
        lines.foreach { line =>
          linesLength += 1
          assert(expectedLines.contains(line))
        }
        assert(linesLength === expectedLines.length, "The number of lines for rewritten file " +
          s"is not expected: expected ${expectedLines.length} / actual $linesLength")
      }
    }
  }

  private def convertEvent(event: SparkListenerEvent): String = {
    compact(render(JsonProtocol.sparkEventToJson(event)))
  }
}

class TestEventFilter1 extends EventFilter {
  override def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = {
    Some(true)
  }

  override def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = {
    Some(true)
  }

  override def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = {
    Some(false)
  }
}

class TestEventFilter2 extends EventFilter {
  override def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = {
    Some(true)
  }

  override def filterEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Option[Boolean] = {
    Some(true)
  }

  override def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = {
    Some(false)
  }

  override def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = {
    Some(false)
  }

  override def filterNodeBlacklisted(event: SparkListenerNodeBlacklisted): Option[Boolean] = {
    Some(true)
  }

  override def filterNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Option[Boolean] = {
    Some(false)
  }
}
