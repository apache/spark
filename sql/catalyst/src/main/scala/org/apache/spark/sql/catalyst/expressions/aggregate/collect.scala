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
 * The Collect aggregate function collects all seen expression values into a list of values.
 *
 * The operator is bound to the slower sort based aggregation path because the number of
 * elements (and their memory usage) can not be determined in advance. This also means that the
 * collected elements are stored on heap, and that too many elements can cause GC pauses and
 * eventually Out of Memory Errors.
 */
abstract class Collect extends ImperativeAggregate {

  val child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def supportsPartial: Boolean = false

  override def aggBufferAttributes: Seq[AttributeReference] = Nil

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = Nil

  // Both `CollectList` and `CollectSet` are non-deterministic since their results depend on the
  // actual order of input rows.
  override def deterministic: Boolean = false

  protected[this] val buffer: Growable[Any] with Iterable[Any]

  override def initialize(b: MutableRow): Unit = {
    buffer.clear()
  }

  override def update(b: MutableRow, input: InternalRow): Unit = {
    val value = child.eval(input)
    if (value != null) {
      buffer += value
    }
  }

  override def merge(buffer: MutableRow, input: InternalRow): Unit = {
    sys.error("Collect cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(buffer.toArray)
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
    inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_list"

  override protected[this] val buffer: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty
}

/**
 * Collect a list of unique elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.")
case class CollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect {

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

  override protected[this] val buffer: mutable.HashSet[Any] = mutable.HashSet.empty
}
