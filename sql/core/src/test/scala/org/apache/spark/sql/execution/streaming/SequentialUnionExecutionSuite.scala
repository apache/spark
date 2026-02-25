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

      val query = df1.followedBy(df2)

      testStream(query)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        // Add data to both sources
        AddData(input1, 1, 2),
        AddData(input2, 10, 20),
        // Should only see data from source1 (active child)
        CheckNewAnswer((1, "A"), (2, "A")),
        // Add more data to source1
        AddData(input1, 3),
        CheckNewAnswer((3, "A")),
        StopStream
      )
    }
  }
}
