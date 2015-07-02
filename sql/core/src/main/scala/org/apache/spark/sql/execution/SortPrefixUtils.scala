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


package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, IntegerType}
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, PrefixComparator}


object SortPrefixUtils {

  /**
   * A dummy prefix comparator which always claims that prefixes are equal. This is used in cases
   * where we don't know how to generate or compare prefixes for a SortOrder.
   */
  private object NoOpPrefixComparator extends PrefixComparator {
    override def compare(prefix1: Long, prefix2: Long): Int = 0
  }

  def getPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    sortOrder.dataType match {
      case IntegerType => PrefixComparators.INTEGER
      case LongType => PrefixComparators.LONG
      case FloatType => PrefixComparators.FLOAT
      case DoubleType => PrefixComparators.DOUBLE
      case _ => NoOpPrefixComparator
    }
  }

  def getPrefixComputer(sortOrder: SortOrder): InternalRow => Long = {
    sortOrder.dataType match {
      case IntegerType => (row: InternalRow) =>
        PrefixComparators.INTEGER.computePrefix(sortOrder.child.eval(row).asInstanceOf[Int])
      case LongType => (row: InternalRow) => sortOrder.child.eval(row).asInstanceOf[Long]
      case FloatType => (row: InternalRow) =>
        PrefixComparators.FLOAT.computePrefix(sortOrder.child.eval(row).asInstanceOf[Float])
      case DoubleType => (row: InternalRow) =>
        PrefixComparators.DOUBLE.computePrefix(sortOrder.child.eval(row).asInstanceOf[Double])
      case _ => (row: InternalRow) => 0L
    }
  }
}
