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

package org.apache.spark.sql.execution.streaming.sources

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

case class KV(key: Int, value: Long)

class ForeachBatchSinkSuite extends StreamTest {
  import testImplicits._

  test("foreachBatch with non-stateful query") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS.map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer = (ds: Dataset[Int], batchId: Long) => tester.record(batchId, ds.map(_ + 1))

    import tester._
    testWriter(ds, writer)(
      check(in = 1, 2, 3)(out = 3, 4, 5), // out = in + 2 (i.e. 1 in query, 1 in writer)
      check(in = 5, 6, 7)(out = 7, 8, 9))
  }

  test("foreachBatch with stateful query in update mode") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF()
      .select($"value" % 2 as "key")
      .groupBy("key")
      .agg(count("*") as "value")
      .toDF.as[KV]

    val tester = new ForeachBatchTester[KV](mem)
    val writer = (batchDS: Dataset[KV], batchId: Long) => tester.record(batchId, batchDS)

    import tester._
    testWriter(ds, writer, outputMode = OutputMode.Update)(
      check(in = 0)(out = (0, 1L)),
      check(in = 1)(out = (1, 1L)),
      check(in = 2, 3)(out = (0, 2L), (1, 2L)))
  }

  test("foreachBatch with stateful query in complete mode") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF()
      .select($"value" % 2 as "key")
      .groupBy("key")
      .agg(count("*") as "value")
      .toDF.as[KV]

    val tester = new ForeachBatchTester[KV](mem)
    val writer = (batchDS: Dataset[KV], batchId: Long) => tester.record(batchId, batchDS)

    import tester._
    testWriter(ds, writer, outputMode = OutputMode.Complete)(
      check(in = 0)(out = (0, 1L)),
      check(in = 1)(out = (0, 1L), (1, 1L)),
      check(in = 2)(out = (0, 2L), (1, 1L)))
  }

  test("foreachBatchSink does not affect metric generation") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS.map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer = (ds: Dataset[Int], batchId: Long) => tester.record(batchId, ds.map(_ + 1))

    import tester._
    testWriter(ds, writer)(
      check(in = 1, 2, 3)(out = 3, 4, 5),
      checkMetrics)
  }

  test("throws errors in invalid situations") {
    val ds = MemoryStream[Int].toDS
    val ex1 = intercept[IllegalArgumentException] {
      ds.writeStream.foreachBatch(null.asInstanceOf[(Dataset[Int], Long) => Unit]).start()
    }
    assert(ex1.getMessage.contains("foreachBatch function cannot be null"))
    val ex2 = intercept[AnalysisException] {
      ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => {})
        .trigger(Trigger.Continuous("1 second")).start()
    }
    assert(ex2.getMessage.contains("'foreachBatch' is not supported with continuous trigger"))
    val ex3 = intercept[AnalysisException] {
      ds.writeStream.foreachBatch((_: Dataset[Int], _: Long) => {}).partitionBy("value").start()
    }
    assert(ex3.getMessage.contains("'foreachBatch' does not support partitioning"))
  }

  // ============== Helper classes and methods =================

  private class ForeachBatchTester[T: Encoder](memoryStream: MemoryStream[Int]) {
    trait Test
    private case class Check(in: Seq[Int], out: Seq[T]) extends Test
    private case object CheckMetrics extends Test

    private val recordedOutput = new mutable.HashMap[Long, Seq[T]]

    def testWriter(
        ds: Dataset[T],
        outputBatchWriter: (Dataset[T], Long) => Unit,
        outputMode: OutputMode = OutputMode.Append())(tests: Test*): Unit = {
      try {
        var expectedBatchId = -1
        val query = ds.writeStream.outputMode(outputMode).foreachBatch(outputBatchWriter).start()

        tests.foreach {
          case Check(in, out) =>
            expectedBatchId += 1
            memoryStream.addData(in)
            query.processAllAvailable()
            assert(recordedOutput.contains(expectedBatchId))
            val ds: Dataset[T] = spark.createDataset[T](recordedOutput(expectedBatchId))
            checkDataset[T](ds, out: _*)
          case CheckMetrics =>
            assert(query.recentProgress.exists(_.numInputRows > 0))
        }
      } finally {
        sqlContext.streams.active.foreach(_.stop())
      }
    }

    def check(in: Int*)(out: T*): Test = Check(in, out)
    def checkMetrics: Test = CheckMetrics
    def record(batchId: Long, ds: Dataset[T]): Unit = recordedOutput.put(batchId, ds.collect())
    implicit def conv(x: (Int, Long)): KV = KV(x._1, x._2)
  }
}
