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

package org.apache.spark.sql.execution.stat

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions, Column}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.classic.ExpressionUtils.expression
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object FrequentItems extends Logging {

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * <a href="https://doi.org/10.1145/762471.762473">here</a>, proposed by Karp, Schenker,
   * and Papadimitriou.
   * The `support` should be greater than 1e-4.
   * For Internal use only.
   *
   * @param df The input DataFrame
   * @param cols the names of the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def singlePassFreqItems(
      df: DataFrame,
      cols: Seq[String],
      support: Double): DataFrame = {
    require(support >= 1e-4 && support <= 1.0, s"Support must be in [1e-4, 1], but got $support.")

    // number of max items to keep counts for
    val sizeOfMap = (1 / support).toInt

    val frequentItemCols = cols.map { col =>
      Column(new CollectFrequentItems(expression(functions.col(col)), sizeOfMap))
        .as(s"${col}_freqItems")
    }

    df.select(frequentItemCols: _*)
  }
}

case class CollectFrequentItems(
    child: Expression,
    size: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[mutable.Map[Any, Long]]
  with UnaryLike[Expression] {
  require(size > 0)

  def this(child: Expression, size: Int) = this(child, size, 0, 0)

  // Returns empty array for empty inputs
  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = child.nullable)

  override def prettyName: String = "collect_frequent_items"

  override def createAggregationBuffer(): mutable.Map[Any, Long] =
    mutable.Map.empty[Any, Long]

  private def add(map: mutable.Map[Any, Long], key: Any, count: Long): mutable.Map[Any, Long] = {
    if (map.contains(key)) {
      map(key) += count
    } else {
      if (map.size < size) {
        map += key -> count
      } else {
        val minCount = if (map.values.isEmpty) 0 else map.values.min
        val remainder = count - minCount
        if (remainder >= 0) {
          map += key -> count // something will get kicked out, so we can add this
          map.filterInPlace((k, v) => v > minCount)
          map.mapValuesInPlace((k, v) => v - minCount)
        } else {
          map.mapValuesInPlace((k, v) => v - count)
        }
      }
    }
    map
  }

  override def update(
      buffer: mutable.Map[Any, Long],
      input: InternalRow): mutable.Map[Any, Long] = {
    val key = child.eval(input)
    if (key != null) {
      this.add(buffer, InternalRow.copyValue(key), 1L)
    } else {
      this.add(buffer, key, 1L)
    }
  }

  override def merge(
      buffer: mutable.Map[Any, Long],
      input: mutable.Map[Any, Long]): mutable.Map[Any, Long] = {
    val otherIter = input.iterator
    while (otherIter.hasNext) {
      val (key, count) = otherIter.next()
      add(buffer, key, count)
    }
    buffer
  }

  override def eval(buffer: mutable.Map[Any, Long]): Any =
    new GenericArrayData(buffer.keys.toArray)

  private lazy val projection =
    UnsafeProjection.create(Array[DataType](child.dataType, LongType))

  override def serialize(map: mutable.Map[Any, Long]): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10) // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    Utils.tryWithSafeFinally {
      // Write pairs in counts map to byte buffer.
      map.foreach { case (key, count) =>
        val row = InternalRow.apply(key, count)
        val unsafeRow = projection.apply(row)
        out.writeInt(unsafeRow.getSizeInBytes)
        unsafeRow.writeToStream(out, buffer)
      }
      out.writeInt(-1)
      out.flush()

      bos.toByteArray
    } {
      out.close()
      bos.close()
    }
  }

  override def deserialize(bytes: Array[Byte]): mutable.Map[Any, Long] = {
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)
    Utils.tryWithSafeFinally {
      val map = mutable.Map.empty[Any, Long]
      // Read unsafeRow size and content in bytes.
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(2)
        row.pointTo(bs, sizeOfNextRow)
        // Insert the pairs into counts map.
        val key = row.get(0, child.dataType)
        val count = row.get(1, LongType).asInstanceOf[Long]
        map.update(key, count)
        sizeOfNextRow = ins.readInt()
      }

      map
    } {
      ins.close()
      bis.close()
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
