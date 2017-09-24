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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


/**
 * An interpreted row ordering comparator.
 */
class InterpretedOrdering(orders: Seq[SortOrder]) extends Ordering[InternalRow] {

  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BindReferences.bindReference(_, inputSchema)))

  @transient private[this] lazy val orderings = orders.toIndexedSeq.map { order =>
    val ordering = TypeUtils.getInterpretedOrdering(order.dataType)
    if (order.direction == Ascending) {
        ordering
    } else {
      ordering.reverse
    }
  }

  def compare(a: InternalRow, b: InternalRow): Int = {
    var i = 0
    val size = orders.size
    while (i < size) {
      val order = orders(i)
      val left = order.child.eval(a)
      val right = order.child.eval(b)

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.nullOrdering == NullsFirst) -1 else 1
      } else if (right == null) {
        return if (order.nullOrdering == NullsFirst) 1 else -1
      } else {
        val comparison = orderings(i).compare(left, right)
        if (comparison != 0) {
          return comparison
        }
      }
      i += 1
    }
    0
  }
}

object InterpretedOrdering {

  /**
   * Creates a [[InterpretedOrdering]] for the given schema, in natural ascending order.
   */
  def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}
