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

package org.apache.spark.sql.connector

import org.apache.spark.{SparkArithmeticException, SparkException, SparkFunSuite, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, Divide, EqualTo, EvalMode, GreaterThan, Literal, NumericEvalContext, Remainder}
import org.apache.spark.sql.connector.expressions.filter.StorageFilterNotAppliedException
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.connector.{StorageFilterImpl, StorageFilterRequestImpl}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Tests the Catalyst-backed [[StorageFilterImpl]] a connector's reader evaluates. */
class StorageFilterImplSuite extends SparkFunSuite {

  private val k = AttributeReference("k", LongType)()
  private val v = AttributeReference("v", LongType)()

  // `k % 2 = 0` over a scan whose output has `v` before `k`.
  private def evenKeys: StorageFilterImpl =
    StorageFilterImpl(EqualTo(Remainder(k, Literal(2L)), Literal(0L)), Seq(v, k))

  // `1 / k > 0`, which raises an error at `k = 0` under ANSI.
  private def positiveInverse: StorageFilterImpl = StorageFilterImpl(
    GreaterThan(
      Divide(Literal(1.0), Cast(k, DoubleType), NumericEvalContext(EvalMode.ANSI)),
      Literal(0.0)),
    Seq(k))

  private def withBatchOf(keys: Long*)(body: ColumnarBatch => Unit): Unit = {
    val vector = new OnHeapColumnVector(keys.length, LongType)
    keys.zipWithIndex.foreach { case (key, i) => vector.putLong(i, key) }
    val batch = new ColumnarBatch(Array(vector), keys.length)
    try body(batch) finally batch.close()
  }

  test("the references are the columns the filter reads") {
    assert(evenKeys.references.map(_.toString).toSeq == Seq("k"))
  }

  test("a filter over two columns takes them in the scan output's order") {
    val filter =
      StorageFilterImpl(And(EqualTo(k, Literal(1L)), EqualTo(v, Literal(10L))), Seq(v, k))
    assert(filter.references.map(_.toString).toSeq == Seq("v", "k"))
    assert(filter.eval(InternalRow(10L, 1L)), "the row holds v, then k")
    assert(!filter.eval(InternalRow(1L, 10L)))
  }

  test("a row of another width than the references fails, rather than being misread") {
    // `evenKeys` reads `k` alone, so a row holding `v, k` would put `v` where `k` is read.
    val e = intercept[SparkException](evenKeys.eval(InternalRow(3L, 4L)))
    assert(e.getCondition == "INTERNAL_ERROR", s"got ${e.getCondition}")
  }

  test("a filter over a column outside the scan output is an internal error") {
    val e = intercept[SparkException](StorageFilterImpl(EqualTo(k, Literal(1L)), Seq(v)))
    assert(e.getCondition == "INTERNAL_ERROR", s"got ${e.getCondition}")
  }

  test("select writes the kept rows' positions in ascending order") {
    withBatchOf(3L, 4L, 5L, 6L, 8L) { batch =>
      val selected = new Array[Int](batch.numRows)
      val count = evenKeys.select(batch, selected)
      assert(selected.take(count).toSeq == Seq(1, 3, 4))
    }
  }

  test("an error evaluating the filter answers that it is not applied") {
    val filter = positiveInverse
    assert(filter.eval(InternalRow(2L)))
    val e = intercept[StorageFilterNotAppliedException](filter.eval(InternalRow(0L)))
    assert(e.getCondition == "STORAGE_FILTER_NOT_APPLIED", s"got ${e.getCondition}")
    assert(e.getCause.isInstanceOf[SparkArithmeticException], s"got ${e.getCause}")
    withBatchOf(1L, 0L) { batch =>
      intercept[StorageFilterNotAppliedException](filter.select(batch, new Array[Int](2)))
    }
  }

  test("a reader factory without the storage-filter methods cannot create a filtering reader") {
    val factory = new PartitionReaderFactory {
      override def createReader(partition: InputPartition): PartitionReader[InternalRow] = null
    }
    val request = StorageFilterRequestImpl(evenKeys, appendsCheckedColumn = false)
    Seq(
      () => factory.createReader(StorageFilterTestPartition, request),
      () => factory.createColumnarReader(StorageFilterTestPartition, request)
    ).foreach { create =>
      val e = intercept[SparkUnsupportedOperationException](create())
      assert(e.getCondition == "DATA_SOURCE_STORAGE_FILTER_READER_NOT_SUPPORTED",
        s"got ${e.getCondition}")
    }
  }

  test("an error during a task kill propagates as it is") {
    val context = TaskContext.empty()
    context.markInterrupted("test")
    TaskContext.withTaskContext(context) {
      intercept[SparkArithmeticException](positiveInverse.eval(InternalRow(0L)))
    }
  }
}
