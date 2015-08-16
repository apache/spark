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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
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
      case StringType =>
        if (sortOrder.isAscending) PrefixComparators.STRING else PrefixComparators.STRING_DESC
      case BinaryType =>
        if (sortOrder.isAscending) PrefixComparators.BINARY else PrefixComparators.BINARY_DESC
      case BooleanType | ByteType | ShortType | IntegerType | LongType | DateType | TimestampType =>
        if (sortOrder.isAscending) PrefixComparators.LONG else PrefixComparators.LONG_DESC
      case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
        if (sortOrder.isAscending) PrefixComparators.LONG else PrefixComparators.LONG_DESC
      case FloatType | DoubleType =>
        if (sortOrder.isAscending) PrefixComparators.DOUBLE else PrefixComparators.DOUBLE_DESC
      case dt: DecimalType =>
        if (sortOrder.isAscending) PrefixComparators.DOUBLE else PrefixComparators.DOUBLE_DESC
      case _ => NoOpPrefixComparator
    }
  }

  /**
   * Creates the prefix comparator for the first field in the given schema, in ascending order.
   */
  def getPrefixComparator(schema: StructType): PrefixComparator = {
    if (schema.nonEmpty) {
      val field = schema.head
      getPrefixComparator(SortOrder(BoundReference(0, field.dataType, field.nullable), Ascending))
    } else {
      new PrefixComparator {
        override def compare(prefix1: Long, prefix2: Long): Int = 0
      }
    }
  }

  /**
   * Creates the prefix computer for the first field in the given schema, in ascending order.
   */
  def createPrefixGenerator(schema: StructType): UnsafeExternalRowSorter.PrefixComputer = {
    if (schema.nonEmpty) {
      val boundReference = BoundReference(0, schema.head.dataType, nullable = true)
      val prefixProjection = UnsafeProjection.create(
        SortPrefix(SortOrder(boundReference, Ascending)))
      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = {
          prefixProjection.apply(row).getLong(0)
        }
      }
    } else {
      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = 0
      }
    }
  }
}
