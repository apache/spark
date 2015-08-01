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
      case StringType if sortOrder.isAscending => PrefixComparators.STRING
      case StringType if !sortOrder.isAscending => PrefixComparators.STRING_DESC
      case BooleanType | ByteType | ShortType | IntegerType | LongType if sortOrder.isAscending =>
        PrefixComparators.LONG
      case BooleanType | ByteType | ShortType | IntegerType | LongType if !sortOrder.isAscending =>
        PrefixComparators.LONG_DESC
      case FloatType | DoubleType if sortOrder.isAscending => PrefixComparators.DOUBLE
      case FloatType | DoubleType if !sortOrder.isAscending => PrefixComparators.DOUBLE_DESC
      case _ => NoOpPrefixComparator
    }
  }

  def getPrefixComparator(schema: StructType): PrefixComparator = {
    val field = schema.head
    getPrefixComparator(SortOrder(BoundReference(0, field.dataType, field.nullable), Ascending))
  }

  def createPrefixGenerator(schema: StructType): UnsafeExternalRowSorter.PrefixComputer = {
    val boundReference = BoundReference(0, schema.head.dataType, nullable = true)
    val prefixProjection = UnsafeProjection.create(SortPrefix(SortOrder(boundReference, Ascending)))
    new UnsafeExternalRowSorter.PrefixComputer {
      override def computePrefix(row: InternalRow): Long = {
        prefixProjection.apply(row).getLong(0)
      }
    }
  }
}
