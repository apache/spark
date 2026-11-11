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

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkException, SparkFunSuite, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, Divide, EqualTo, EvalMode, Expression, GreaterThan, Literal, NumericEvalContext, Remainder, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, StorageFilterNotAppliedException}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.connector.{StorageFilterImpl, StorageFilterRequestImpl}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, LongType}
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

  test("a reader that reorders the references changes the layout of no other reader") {
    val filter =
      StorageFilterImpl(And(EqualTo(k, Literal(1L)), EqualTo(v, Literal(10L))), Seq(v, k))
    val references = filter.references()
    references.sortInPlaceBy(_.toString)
    assert(filter.references().map(_.toString).toSeq == Seq("v", "k"))
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
    checkError(exception = e, condition = "STORAGE_FILTER_NOT_APPLIED", sqlState = Some("KD000"))
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
      () => factory.createReader(RangeInputPartition(0, 1), request),
      () => factory.createColumnarReader(RangeInputPartition(0, 1), request)
    ).foreach { create =>
      checkError(
        exception = intercept[SparkUnsupportedOperationException](create()),
        condition = "DATA_SOURCE_STORAGE_FILTER_READER_NOT_SUPPORTED",
        sqlState = Some("0A000"),
        parameters = Map("class" -> factory.getClass.getName))
    }
  }

  // The file-source reader's rule, see `StorageFilterErrors`, which its own suite covers in full.
  test("a fatal error in the cause chain propagates as itself") {
    val fatal = new OutOfMemoryError("deep")
    val error = new RuntimeException("wraps", fatal)
    val filter = StorageFilterImpl(ThrowingPredicate(k, error), Seq(k))
    val e = intercept[OutOfMemoryError](filter.eval(InternalRow(1L)))
    assert(e eq fatal, s"got $e")
  }

  test("an InternalError propagates in an UnknownError, so it cannot pass for a corrupt file") {
    val internal = new InternalError("fatal")
    val filter = StorageFilterImpl(ThrowingPredicate(k, internal), Seq(k))
    val e = intercept[UnknownError](filter.eval(InternalRow(1L)))
    assert(e.getCause eq internal, s"got ${e.getCause}")
  }

  test("an error during a task kill propagates in a checked SparkException") {
    val context = TaskContext.empty()
    context.markInterrupted("test")
    TaskContext.withTaskContext(context) {
      val e = intercept[SparkException](positiveInverse.eval(InternalRow(0L)))
      assert(e.getCause.isInstanceOf[SparkArithmeticException], s"got ${e.getCause}")
    }
  }

  test("a LinkageError answers that the filter is not applied") {
    val error = new LinkageError("not fatal to the executor")
    val filter = StorageFilterImpl(ThrowingPredicate(k, error), Seq(k))
    val e = intercept[StorageFilterNotAppliedException](filter.eval(InternalRow(1L)))
    assert(e.getCause eq error, s"got ${e.getCause}")
  }

  test("an evaluated filter releases its expression, so a later copy cannot be evaluated") {
    // The readers of a task share the instance their task deserialized, and only the first
    // evaluation builds the predicate. A copy of an evaluated instance has nothing to build from.
    val filter = evenKeys
    assert(filter.eval(InternalRow(4L)))
    assert(!filter.eval(InternalRow(3L)), "the predicate built first is used again")
    val copy = new JavaSerializer(new SparkConf()).newInstance()
    val copied = copy.deserialize[StorageFilterImpl](copy.serialize(filter))
    val e = intercept[SparkException](copied.eval(InternalRow(4L)))
    assert(e.getCondition == "INTERNAL_ERROR", s"got ${e.getCondition}")
    assert(e.getMessage.contains("StorageFilter(k) was copied"), s"got ${e.getMessage}")
  }
}

/** A predicate over `child` that throws `error`, for the paths an error takes out of a filter. */
case class ThrowingPredicate(child: Expression, error: Throwable)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = BooleanType
  override protected def nullSafeEval(input: Any): Any = throw error
  override protected def withNewChildInternal(newChild: Expression): ThrowingPredicate =
    copy(child = newChild)
}
