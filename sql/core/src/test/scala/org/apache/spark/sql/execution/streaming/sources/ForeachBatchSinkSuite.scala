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
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.{LogicalRDD, SerializeFromObjectExec}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
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

  test("foreachBatch with non-stateful query - untyped Dataset") {
    val mem = MemoryStream[Int]
    val ds = mem.toDF.selectExpr("value + 1 as value")

    val tester = new ForeachBatchTester[Row](mem)(RowEncoder.apply(ds.schema))
    val writer = (df: DataFrame, batchId: Long) =>
      tester.record(batchId, df.selectExpr("value + 1"))

    import tester._
    testWriter(ds, writer)(
      // out = in + 2 (i.e. 1 in query, 1 in writer)
      check(in = 1, 2, 3)(out = Row(3), Row(4), Row(5)),
      check(in = 5, 6, 7)(out = Row(7), Row(8), Row(9)))
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

  test("foreachBatch with batch specific operations") {
    val mem = MemoryStream[Int]
    val ds = mem.toDS.map(_ + 1)

    val tester = new ForeachBatchTester[Int](mem)
    val writer: (Dataset[Int], Long) => Unit = { case (df, batchId) =>
      df.persist()

      val newDF = df
        .map(_ + 1)
        .repartition(1)
        .sort(Column("value").desc)
      tester.record(batchId, newDF)

      // just run another simple query against cached DF to confirm they don't conflict each other
      val curValues = df.collect()
      val newValues = df.map(_ + 2).collect()
      assert(curValues.map(_ + 2) === newValues)

      df.unpersist()
    }

    import tester._
    testWriter(ds, writer)(
      // out = in + 2 (i.e. 1 in query, 1 in writer), with sorted
      check(in = 1, 2, 3)(out = 5, 4, 3),
      check(in = 5, 6, 7)(out = 9, 8, 7))
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

  test("foreachBatch should not introduce object serialization") {
    def assertPlan[T](stream: MemoryStream[Int], ds: Dataset[T]): Unit = {
      var planAsserted = false

      val writer: (Dataset[T], Long) => Unit = { case (df, _) =>
        assert(!df.queryExecution.executedPlan.exists { p =>
          p.isInstanceOf[SerializeFromObjectExec]
        }, "Untyped Dataset should not introduce serialization on object!")
        planAsserted = true
      }

      stream.addData(1, 2, 3, 4, 5)

      val query = ds.writeStream.trigger(Trigger.AvailableNow()).foreachBatch(writer).start()
      query.awaitTermination()

      assert(planAsserted, "ForeachBatch writer should be called!")
    }

    // typed
    val mem = MemoryStream[Int]
    val ds = mem.toDS.map(_ + 1)
    assertPlan(mem, ds)

    // untyped
    val mem2 = MemoryStream[Int]
    val dsUntyped = mem2.toDF().selectExpr("value + 1 as value")
    assertPlan(mem2, dsUntyped)
  }

  test("Leaf node of Dataset in foreachBatch should carry over origin logical plan") {
    def assertPlan[T](stream: MemoryStream[Int], ds: Dataset[T]): Unit = {
      var planAsserted = false

      val writer: (Dataset[T], Long) => Unit = { case (df, _) =>
        df.logicalPlan.collectLeaves().head match {
          case l: LogicalRDD =>
            assert(l.originLogicalPlan.nonEmpty, "Origin logical plan should be available in " +
              "LogicalRDD")
            l.originLogicalPlan.get.collectLeaves().head match {
              case _: StreamingDataSourceV2Relation => // pass
              case p =>
                fail("Expect StreamingDataSourceV2Relation in the leaf node of origin " +
                  s"logical plan! Actual: $p")
            }

          case p =>
            fail(s"Expect LogicalRDD in the leaf node of Dataset! Actual: $p")
        }
        planAsserted = true
      }

      stream.addData(1, 2, 3, 4, 5)

      val query = ds.writeStream.trigger(Trigger.AvailableNow()).foreachBatch(writer).start()
      query.awaitTermination()

      assert(planAsserted, "ForeachBatch writer should be called!")
    }

    // typed
    val mem = MemoryStream[Int]
    val ds = mem.toDS.map(_ + 1)
    assertPlan(mem, ds)

    // untyped
    val mem2 = MemoryStream[Int]
    val dsUntyped = mem2.toDF().selectExpr("value + 1 as value")
    assertPlan(mem2, dsUntyped)
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
