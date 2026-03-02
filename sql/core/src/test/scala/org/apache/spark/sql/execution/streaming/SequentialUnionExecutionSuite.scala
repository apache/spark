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

package org.apache.spark.sql.execution.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamTest, Trigger}

/**
 * Test suite for [[SequentialUnionExecution]], which executes streaming queries
 * containing SequentialStreamingUnion nodes.
 */
class SequentialUnionExecutionSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("SequentialUnionExecution - basic execution with two sources") {
    withTempDir { checkpointDir =>
      val input1 = new MemoryStream[Int](id = 0, spark)
      val input2 = new MemoryStream[Int](id = 1, spark)

      val df1 = input1.toDF().withColumn("source", lit("source1"))
      val df2 = input2.toDF().withColumn("source", lit("source2"))

      // Create a sequential union
      val query = df1.followedBy(df2)

      testStream(query)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        AddData(input1, 1, 2, 3),
        CheckNewAnswer((1, "source1"), (2, "source1"), (3, "source1")),
        AddData(input1, 4, 5),
        CheckNewAnswer((4, "source1"), (5, "source1")),
        StopStream
      )
    }
  }

  test("SequentialUnionExecution - works with Trigger.ProcessingTime") {
    withTempDir { checkpointDir =>
      val input1 = new MemoryStream[Int](id = 0, spark)
      val input2 = new MemoryStream[Int](id = 1, spark)

      val df1 = input1.toDF().withColumn("source", lit("A"))
      val df2 = input2.toDF().withColumn("source", lit("B"))

      val sequential = df1.followedBy(df2)

      testStream(sequential)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath,
          trigger = Trigger.ProcessingTime("1 second")),
        // Add data to first source (active child)
        AddData(input1, 1, 2, 3),
        CheckNewAnswer((1, "A"), (2, "A"), (3, "A")),
        // Add more data - auto-transition logic is enabled but won't trigger
        // with MemoryStream as it's not truly exhausted
        AddData(input1, 4, 5),
        CheckNewAnswer((4, "A"), (5, "A")),
        StopStream
      )
    }
  }

  test("SequentialUnionExecution - file sources with AvailableNow") {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { checkpointDir =>
          // Write data to file sources
          Seq("src1-a", "src1-b", "src1-c").toDF("value")
            .write.mode("overwrite").json(dir1.getCanonicalPath)
          Seq("src2-d", "src2-e", "src2-f").toDF("value")
            .write.mode("overwrite").json(dir2.getCanonicalPath)

          // Create file streams
          val df1 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir1.getCanonicalPath)
            .withColumn("source", lit("source1"))

          val df2 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir2.getCanonicalPath)
            .withColumn("source", lit("source2"))

          // Create sequential union
          val sequential = df1.followedBy(df2)

          // Start the query with Trigger.AvailableNow
          val query = sequential.writeStream
            .format("memory")
            .queryName("filetest")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.AvailableNow)
            .start()

          try {
            println("[TEST] About to call processAllAvailable()")
            query.processAllAvailable()
            println("[TEST] processAllAvailable() completed")
            query.stop()
          } finally {
            if (query.isActive) {
              query.stop()
            }
          }
        }
      }
    }
  }

  test("SequentialUnionExecution - file sources with ProcessingTime trigger") {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { checkpointDir =>
          // Write data to file sources
          Seq("data1", "data2", "data3").toDF("value")
            .write.mode("overwrite").json(dir1.getCanonicalPath)
          Seq("data4", "data5", "data6").toDF("value")
            .write.mode("overwrite").json(dir2.getCanonicalPath)

          // Create file streams
          val df1 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir1.getCanonicalPath)
            .withColumn("source", lit("A"))

          val df2 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir2.getCanonicalPath)
            .withColumn("source", lit("B"))

          // Create sequential union
          val sequential = df1.followedBy(df2)

          // Start with ProcessingTime trigger (scenario from handoff doc)
          val query = sequential.writeStream
            .format("memory")
            .queryName("filetest_processingtime")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.ProcessingTime("100 milliseconds"))
            .start()

          try {
            // scalastyle:off println
            println("[TEST] About to call processAllAvailable() with ProcessingTime trigger")
            // scalastyle:on println
            query.processAllAvailable()
            // scalastyle:off println
            println("[TEST] processAllAvailable() completed successfully")
            // scalastyle:on println
            query.stop()
          } finally {
            if (query.isActive) {
              query.stop()
            }
          }
        }
      }
    }
  }

  test("SequentialUnionExecution - auto-transition between file sources") {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { checkpointDir =>
          // Write small files to ensure first source exhausts quickly
          Seq("source-1-row1").toDF("value")
            .write.mode("overwrite").json(dir1.getCanonicalPath)
          Seq("source-2-row1", "source-2-row2", "source-2-row3").toDF("value")
            .write.mode("overwrite").json(dir2.getCanonicalPath)

          // Create file streams
          val df1 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir1.getCanonicalPath)
            .withColumn("source", lit("source1"))

          val df2 = spark.readStream
            .format("json")
            .schema("value STRING")
            .load(dir2.getCanonicalPath)
            .withColumn("source", lit("source2"))

          // Create sequential union
          val sequential = df1.followedBy(df2)

          // Use AvailableNow to ensure completion
          val query = sequential.writeStream
            .format("memory")
            .queryName("filetest_transition")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.AvailableNow)
            .start()

          try {
            // scalastyle:off println
            println("[TEST] Testing auto-transition from source1 to source2")
            // scalastyle:on println
            query.processAllAvailable()
            // scalastyle:off println
            println("[TEST] Auto-transition test completed")
            // scalastyle:on println

            // Verify we got data from both sources
            val results = spark.sql("SELECT * FROM filetest_transition").collect()
            val source1Count = results.count(_.getString(1) == "source1")
            val source2Count = results.count(_.getString(1) == "source2")

            // scalastyle:off println
            println(s"[TEST] Results: source1=$source1Count rows, source2=$source2Count rows")
            // scalastyle:on println

            assert(source1Count > 0, "Should have data from source1")
            assert(source2Count > 0, "Should have data from source2")

            query.stop()
          } finally {
            if (query.isActive) {
              query.stop()
            }
          }
        }
      }
    }
  }
}
