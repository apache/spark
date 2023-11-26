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

package org.apache.spark.sql.streaming

import java.io.File

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset, SerializedOffset, Source, StreamingExecutionRelation}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class TriggerAvailableNowSuite extends FileStreamSourceTest {

  import testImplicits._

  abstract class TestDataFrameProvider {
    @volatile var currentOffset = 0L

    def toDF: DataFrame

    def incrementAvailableOffset(numNewRows: Int): Unit

    def sourceName: String
  }

  class TestSource extends TestDataFrameProvider with Source {
    override def getOffset: Option[Offset] = {
      if (currentOffset <= 0) None else Some(LongOffset(currentOffset))
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      if (currentOffset == 0) currentOffset = getOffsetValue(end)
      val plan = Range(
        start.map(getOffsetValue).getOrElse(0L) + 1L, getOffsetValue(end) + 1L, 1, None,
        isStreaming = true)
      Dataset.ofRows(spark, plan)
    }

    override def incrementAvailableOffset(numNewRows: Int): Unit = {
      currentOffset += numNewRows
    }

    override def toDF: DataFrame =
      Dataset.ofRows(spark, StreamingExecutionRelation(this, spark))

    override def schema: StructType = new StructType().add("value", LongType)

    override def stop(): Unit = {}

    private def getOffsetValue(offset: Offset): Long = {
      offset match {
        case s: SerializedOffset => LongOffset(s).offset
        case l: LongOffset => l.offset
        case _ => throw new IllegalArgumentException("incorrect offset type: " + offset)
      }
    }

    override def sourceName: String = this.getClass.getName
  }

  class TestSourceWithAdmissionControl extends TestSource with SupportsAdmissionControl {
    override def getDefaultReadLimit: ReadLimit = ReadLimit.maxRows(1)  // this will be overridden

    override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
      val currentOffset = getOffset
      assert(currentOffset.nonEmpty,
        "the latestOffset should be called after incrementAvailableOffset")
      currentOffset.get
    }
  }

  class TestMicroBatchStream extends TestDataFrameProvider {
    private lazy val memoryStream = MemoryStream[Long](0, spark.sqlContext)

    override def toDF: DataFrame = memoryStream.toDF()

    override def incrementAvailableOffset(numNewRows: Int): Unit = {
      for (_ <- 1 to numNewRows) {
        currentOffset += 1
        memoryStream.addData(currentOffset)
      }
    }

    // remove the trailing `$` in the class name
    override def sourceName: String = MemoryStream.getClass.getSimpleName.dropRight(1)
  }

  Seq(
    new TestSource,
    new TestSourceWithAdmissionControl,
    new TestMicroBatchStream
  ).foreach { testSource =>
    test(s"TriggerAvailableNow for multiple sources with ${testSource.getClass}") {
      withTempDirs { (src, target) =>
        val checkpoint = new File(target, "chk").getCanonicalPath
        val targetDir = new File(target, "data").getCanonicalPath
        var lastFileModTime: Option[Long] = None

        /** Create a text file with a single data item */
        def createFile(data: Int): File = {
          val file = stringToFile(new File(src, s"$data.txt"), data.toString)
          if (lastFileModTime.nonEmpty) file.setLastModified(lastFileModTime.get + 1000)
          lastFileModTime = Some(file.lastModified)
          file
        }

        // Set up a query to read text files one at a time
        val df1 = spark
          .readStream
          .option("maxFilesPerTrigger", 1)
          .text(src.getCanonicalPath)

        val df2 = testSource.toDF.selectExpr("cast(value as string)")

        def startQuery(): StreamingQuery = {
          df1.union(df2).writeStream
            .format("parquet")
            .trigger(Trigger.AvailableNow)
            .option("checkpointLocation", checkpoint)
            .start(targetDir)
        }

        testSource.incrementAvailableOffset(3)
        createFile(7)
        createFile(8)
        createFile(9)

        val q = startQuery()

        try {
          assert(q.awaitTermination(streamingTimeout.toMillis))
          // only one batch has data in both sources, thus counted, see SPARK-24050
          assert(q.recentProgress.count(_.numInputRows != 0) == 1)
          q.recentProgress.foreach { p =>
            assert(p.sources.exists(_.description.startsWith(testSource.sourceName)))
          }
          checkAnswer(sql(s"SELECT * from parquet.`$targetDir`"),
            Seq(1, 2, 3, 7, 8, 9).map(_.toString).toDF())
        } finally {
          q.stop()
        }

        testSource.incrementAvailableOffset(3)
        createFile(10)
        createFile(11)
        createFile(12)

        // run a second query
        val q2 = startQuery()
        try {
          assert(q2.awaitTermination(streamingTimeout.toMillis))
          // only one batch has data in both sources, thus counted, see SPARK-24050
          assert(q2.recentProgress.count(_.numInputRows != 0) == 1)
          q2.recentProgress.foreach { p =>
            assert(p.sources.exists(_.description.startsWith(testSource.sourceName)))
          }
          checkAnswer(sql(s"SELECT * from parquet.`$targetDir`"), (1 to 12).map(_.toString).toDF())
        } finally {
          q2.stop()
        }
      }
    }
  }

  Seq(
    new TestSource,
    new TestSourceWithAdmissionControl,
    new TestMicroBatchStream
  ).foreach { testSource =>
    test(s"TriggerAvailableNow for single source with ${testSource.getClass}") {
      val tableName = "trigger_available_now_test_table"
      withTable(tableName) {
        val df = testSource.toDF

        def startQuery(): StreamingQuery = {
          df.writeStream
            .format("memory")
            .queryName(tableName)
            .trigger(Trigger.AvailableNow)
            .start()
        }

        testSource.incrementAvailableOffset(3)

        val q = startQuery()

        try {
          assert(q.awaitTermination(streamingTimeout.toMillis))
          assert(q.recentProgress.count(_.numInputRows != 0) == 1)
          q.recentProgress.foreach { p =>
            assert(p.sources.exists(_.description.startsWith(testSource.sourceName)))
          }
          checkAnswer(spark.table(tableName), (1 to 3).toDF())
        } finally {
          q.stop()
        }

        testSource.incrementAvailableOffset(3)

        // run a second query
        val q2 = startQuery()
        try {
          assert(q2.awaitTermination(streamingTimeout.toMillis))
          assert(q2.recentProgress.count(_.numInputRows != 0) == 1)
          q2.recentProgress.foreach { p =>
            assert(p.sources.exists(_.description.startsWith(testSource.sourceName)))
          }
          checkAnswer(spark.table(tableName), (1 to 6).toDF())
        } finally {
          q2.stop()
        }
      }
    }
  }
}
