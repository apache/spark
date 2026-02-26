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
import org.apache.spark.sql.streaming.StreamTest

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

  test("SequentialUnionExecution - first child receives data before second") {
    withTempDir { checkpointDir =>
      val input1 = new MemoryStream[Int](id = 0, spark)
      val input2 = new MemoryStream[Int](id = 1, spark)

      val df1 = input1.toDF().withColumn("source", lit("A"))
      val df2 = input2.toDF().withColumn("source", lit("B"))

      val sequential = df1.followedBy(df2)

      // Start the query like a real customer would
      val query = sequential.writeStream
        .format("memory")
        .queryName("sequentialTest")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start()

      try {
        // Add data to both sources upfront
        input1.addData(1, 2, 3)
        input2.addData(10, 20)

        // Process all available data
        query.processAllAvailable()

        // Should see all data from input1 (active child), but none from input2
        val results = spark.sql("SELECT * FROM sequentialTest ORDER BY value").collect()
        val resultsStr = results.map(r => s"(${r.getInt(0)},${r.getString(1)})").mkString(", ")

        // Should have exactly 3 rows from input1
        assert(results.length == 3,
          s"Expected 3 rows from input1, got ${results.length}. Rows: $resultsStr")

        // Verify all rows are from source A (input1), not source B (input2)
        val sources = results.map(_.getString(1)).toSet
        assert(sources == Set("A"),
          s"Expected only source A, but got sources: ${sources.mkString(", ")}. Rows: $resultsStr")

        // Verify the actual values from input1
        val values = results.map(_.getInt(0)).sorted
        assert(values.toSeq == Seq(1, 2, 3),
          s"Expected values [1,2,3] from input1, got: ${values.mkString(", ")}")

      } finally {
        query.stop()
      }
    }
  }
}
