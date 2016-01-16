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

package org.apache.spark.sql.execution.vectorized

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.unsafe.Platform

class ColumnarBatchSuite extends SparkFunSuite {
  test("Null Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val reference = mutable.ArrayBuffer.empty[Boolean]

      val column = ColumnVector.allocate(1024, IntegerType, memMode)
      var idx = 0
      assert(column.anyNullsSet() == false)

      column.putNotNull(idx)
      reference += false
      idx += 1
      assert(column.anyNullsSet() == false)

      column.putNull(idx)
      reference += true
      idx += 1
      assert(column.anyNullsSet() == true)
      assert(column.numNulls() == 1)

      column.putNulls(idx, 3)
      reference += true
      reference += true
      reference += true
      idx += 3
      assert(column.anyNullsSet() == true)

      column.putNotNulls(idx, 4)
      reference += false
      reference += false
      reference += false
      reference += false
      idx += 4
      assert(column.anyNullsSet() == true)
      assert(column.numNulls() == 4)

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getIsNull(v._2))
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.nullsNativeAddress()
          assert(v._1 == (Platform.getByte(null, addr + v._2) == 1), "index=" + v._2)
        }
      }
      column.close
    }}
  }

  test("Int Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Int]

      val column = ColumnVector.allocate(1024, IntegerType, memMode)
      var idx = 0

      val values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).toArray
      column.putInts(idx, 2, values, 0)
      reference += 1
      reference += 2
      idx += 2

      column.putInts(idx, 3, values, 2)
      reference += 3
      reference += 4
      reference += 5
      idx += 3

      val littleEndian = new Array[Byte](8)
      littleEndian(0) = 7
      littleEndian(1) = 1
      littleEndian(4) = 6
      littleEndian(6) = 1

      column.putIntsLittleEndian(idx, 1, littleEndian, 4)
      column.putIntsLittleEndian(idx + 1, 1, littleEndian, 0)
      reference += 6 + (1 << 16)
      reference += 7 + (1 << 8)
      idx += 2

      column.putIntsLittleEndian(idx, 2, littleEndian, 0)
      reference += 7 + (1 << 8)
      reference += 6 + (1 << 16)
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextInt()
          column.putInt(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          column.putInts(idx, n, n + 1)
          var i = 0
          while (i < n) {
            reference += (n + 1)
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getInt(v._2), "Seed = " + seed + " Mem Mode=" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getInt(null, addr + 4 * v._2))
        }
      }
      column.close
    }}
  }

  test("Double APIs") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Double]

      val column = ColumnVector.allocate(1024, DoubleType, memMode)
      var idx = 0

      val values = (1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil).toArray
      column.putDoubles(idx, 2, values, 0)
      reference += 1.0
      reference += 2.0
      idx += 2

      column.putDoubles(idx, 3, values, 2)
      reference += 3.0
      reference += 4.0
      reference += 5.0
      idx += 3

      val buffer = new Array[Byte](16)
      Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET, 2.234)
      Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET + 8, 1.123)

      column.putDoubles(idx, 1, buffer, 8)
      column.putDoubles(idx + 1, 1, buffer, 0)
      reference += 1.123
      reference += 2.234
      idx += 2

      column.putDoubles(idx, 2, buffer, 0)
      reference += 2.234
      reference += 1.123
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextDouble()
          column.putDouble(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          val v = random.nextDouble()
          column.putDoubles(idx, n, v)
          var i = 0
          while (i < n) {
            reference += v
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getDouble(v._2), "Seed = " + seed + " MemMode=" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getDouble(null, addr + 8 * v._2))
        }
      }
      column.close
    }}
  }

  test("ColumnarBatch basic") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val schema = new StructType()
        .add("intCol", IntegerType)
        .add("doubleCol", DoubleType)
        .add("intCol2", IntegerType)

      val batch = ColumnarBatch.allocate(schema, memMode)
      assert(batch.numCols() == 3)
      assert(batch.numRows() == 0)
      assert(batch.numValidRows() == 0)
      assert(batch.capacity() > 0)
      assert(batch.rowIterator().hasNext == false)

      // Add a row [1, 1.1, NULL]
      batch.column(0).putInt(0, 1)
      batch.column(1).putDouble(0, 1.1)
      batch.column(2).putNull(0)
      batch.setNumRows(1)

      // Verify the results of the row.
      assert(batch.numCols() == 3)
      assert(batch.numRows() == 1)
      assert(batch.numValidRows() == 1)
      assert(batch.rowIterator().hasNext == true)
      assert(batch.rowIterator().hasNext == true)

      assert(batch.column(0).getInt(0) == 1)
      assert(batch.column(0).getIsNull(0) == false)
      assert(batch.column(1).getDouble(0) == 1.1)
      assert(batch.column(1).getIsNull(0) == false)
      assert(batch.column(2).getIsNull(0) == true)

      // Verify the iterator works correctly.
      val it = batch.rowIterator()
      assert(it.hasNext())
      val row = it.next()
      assert(row.getInt(0) == 1)
      assert(row.isNullAt(0) == false)
      assert(row.getDouble(1) == 1.1)
      assert(row.isNullAt(1) == false)
      assert(row.isNullAt(2) == true)
      assert(it.hasNext == false)
      assert(it.hasNext == false)

      // Filter out the row.
      row.markFiltered()
      assert(batch.numRows() == 1)
      assert(batch.numValidRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Reset and add 3 throws
      batch.reset()
      assert(batch.numRows() == 0)
      assert(batch.numValidRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Add rows [NULL, 2.2, 2], [3, NULL, 3], [4, 4.4, 4]
      batch.column(0).putNull(0)
      batch.column(1).putDouble(0, 2.2)
      batch.column(2).putInt(0, 2)

      batch.column(0).putInt(1, 3)
      batch.column(1).putNull(1)
      batch.column(2).putInt(1, 3)

      batch.column(0).putInt(2, 4)
      batch.column(1).putDouble(2, 4.4)
      batch.column(2).putInt(2, 4)
      batch.setNumRows(3)

      def rowEquals(x: InternalRow, y: Row): Unit = {
        assert(x.isNullAt(0) == y.isNullAt(0))
        if (!x.isNullAt(0)) assert(x.getInt(0) == y.getInt(0))

        assert(x.isNullAt(1) == y.isNullAt(1))
        if (!x.isNullAt(1)) assert(x.getDouble(1) == y.getDouble(1))

        assert(x.isNullAt(2) == y.isNullAt(2))
        if (!x.isNullAt(2)) assert(x.getInt(2) == y.getInt(2))
      }
      // Verify
      assert(batch.numRows() == 3)
      assert(batch.numValidRows() == 3)
      val it2 = batch.rowIterator()
      rowEquals(it2.next(), Row(null, 2.2, 2))
      rowEquals(it2.next(), Row(3, null, 3))
      rowEquals(it2.next(), Row(4, 4.4, 4))
      assert(!it.hasNext)

      // Filter out some rows and verify
      batch.markFiltered(1)
      assert(batch.numValidRows() == 2)
      val it3 = batch.rowIterator()
      rowEquals(it3.next(), Row(null, 2.2, 2))
      rowEquals(it3.next(), Row(4, 4.4, 4))
      assert(!it.hasNext)

      batch.markFiltered(2)
      assert(batch.numValidRows() == 1)
      val it4 = batch.rowIterator()
      rowEquals(it4.next(), Row(null, 2.2, 2))

      batch.close
    }}
  }
}
