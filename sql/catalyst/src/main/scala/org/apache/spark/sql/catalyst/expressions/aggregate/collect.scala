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

import scala.collection.generic.Growable
import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * A base class for collect_list and collect_set aggregate functions.
 *
 * We have to store all the collected elements in memory, and so notice that too many elements
 * can cause GC paused and eventually OutOfMemory Errors.
 */
abstract class Collect[T <: Growable[Any] with Iterable[Any]] extends TypedImperativeAggregate[T] {

  val child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  // Both `CollectList` and `CollectSet` are non-deterministic since their results depend on the
  // actual order of input rows.
  override def deterministic: Boolean = false

  override def update(buffer: T, input: InternalRow): T = {
    val value = child.eval(input)

    // Do not allow null values. We follow the semantics of Hive's collect_list/collect_set here.
    // See: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator
    if (value != null) {
      buffer += value
    }
    buffer
  }

  override def merge(buffer: T, other: T): T = {
    buffer ++= other
  }

  override def eval(buffer: T): Any = {
    new GenericArrayData(buffer.toArray)
  }

  private lazy val projection = UnsafeProjection.create(
    Array[DataType](ArrayType(elementType = child.dataType, containsNull = false)))
  private lazy val row = new UnsafeRow(1)

  override def serialize(obj: T): Array[Byte] = {
    val array = new GenericArrayData(obj.toArray)
    projection.apply(InternalRow.apply(array)).getBytes()
  }

  override def deserialize(bytes: Array[Byte]): T = {
    val buffer = createAggregationBuffer()
    row.pointTo(bytes, bytes.length)
    row.getArray(0).foreach(child.dataType, (_, x: Any) => buffer += x)
    buffer
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
    inputAggBufferOffset: Int = 0) extends Collect[mutable.ArrayBuffer[Any]] {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def prettyName: String = "collect_list"
}

/**
 * Collect a set of unique elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.")
case class CollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[mutable.HashSet[Any]] {

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

  override def createAggregationBuffer(): mutable.HashSet[Any] = mutable.HashSet.empty
}
