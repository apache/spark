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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * The Collect aggregate function collects all seen expression values into a list of values.
 *
 * We have to store all the collected elements in memory, and that too many elements can cause GC
 * paused and eventually OutOfMemory Errors.
 */
abstract class Collect[T] extends TypedImperativeAggregate[T] {

  val child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  // Both `CollectList` and `CollectSet` are non-deterministic since their results depend on the
  // actual order of input rows.
  override def deterministic: Boolean = false

  protected def _serialize(obj: Iterable[Any]): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10)  // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    try {
      val projection = UnsafeProjection.create(Array[DataType](child.dataType))
      obj.foreach { value =>
        val row = InternalRow.apply(value)
        val unsafeRow = projection.apply(row)
        out.writeInt(unsafeRow.getSizeInBytes)
        unsafeRow.writeToStream(out, buffer)
      }
      out.writeInt(-1)
      out.flush()

      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }

  protected def _deserialize(bytes: Array[Byte], updater: (Any) => Unit): Unit = {
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)
    try {
      // Read unsafeRow size and content in bytes.
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(2)
        row.pointTo(bs, sizeOfNextRow)

        val value = row.get(0, child.dataType)
        updater(value)
        sizeOfNextRow = ins.readInt()
      }
    } finally {
      ins.close()
      bis.close()
    }
  }
}

/**
 * Collect a list of elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.")
case class CollectList(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[ArrayBuffer[Any]] {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): CollectList =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): ArrayBuffer[Any] = new ArrayBuffer[Any]()

  override def prettyName: String = "collect_list"

  override def update(buffer: ArrayBuffer[Any], input: InternalRow): Unit = {
    val value = child.eval(input).asInstanceOf[Any]

    // Do not allow null values. We follow the semantics of Hive's collect_list/collect_set here.
    // See: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator
    if (value != null) {
      buffer += value
    }
  }

  override def merge(buffer: ArrayBuffer[Any], other: ArrayBuffer[Any]): Unit = {
    buffer ++= other
  }

  override def eval(buffer: ArrayBuffer[Any]): Any = {
    generateOutput(buffer)
  }

  private def generateOutput(results: ArrayBuffer[Any]): Any = {
    if (results.isEmpty) {
      null
    } else {
      new GenericArrayData(results.toArray)
    }
  }

  override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = _serialize(obj)
  override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
    val buffer = new ArrayBuffer[Any]()
    val updater: (Any) => Unit = (value) => buffer += value
    _deserialize(bytes, updater)
    buffer
  }
}

/**
 * Collect a set of unique elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.")
case class CollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[HashSet[Any]] {

  def this(child: Expression) = this(child, 0, 0)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.dataType.existsRecursively(_.isInstanceOf[MapType])) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure("collect_set() cannot have map type data")
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_set"

  override def createAggregationBuffer(): HashSet[Any] = new HashSet[Any]()

  override def update(buffer: HashSet[Any], input: InternalRow): Unit = {
    val value = child.eval(input).asInstanceOf[Any]

    // Do not allow null values. We follow the semantics of Hive's collect_list/collect_set here.
    // See: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator
    if (value != null) {
      buffer += value
    }
  }

  override def merge(buffer: HashSet[Any], other: HashSet[Any]): Unit = {
    buffer ++= other
  }

  override def eval(buffer: HashSet[Any]): Any = {
    generateOutput(buffer)
  }

  private def generateOutput(results: HashSet[Any]): Any = {
    if (results.isEmpty) {
      null
    } else {
      new GenericArrayData(results.toArray)
    }
  }

  override def serialize(obj: HashSet[Any]): Array[Byte] = _serialize(obj)
  override def deserialize(bytes: Array[Byte]): HashSet[Any] = {
    val buffer = new HashSet[Any]()
    val updater: (Any) => Unit = (value) => buffer += value
    _deserialize(bytes, updater)
    buffer
  }
}
