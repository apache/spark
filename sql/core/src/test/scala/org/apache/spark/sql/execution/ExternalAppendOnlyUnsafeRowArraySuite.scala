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

import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class ExternalAppendOnlyUnsafeRowArraySuite extends SparkFunSuite with LocalSparkContext {
  private val random = new java.util.Random()
  private var taskContext: TaskContext = _

  override def afterAll(): Unit = TaskContext.unset()

  private def withExternalArray(spillThreshold: Int)
                               (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
    sc = new SparkContext("local", "test", new SparkConf(false))

    taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)

    val array = new ExternalAppendOnlyUnsafeRowArray(
      taskContext.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      taskContext,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      spillThreshold)
    try f(array) finally {
      array.clear()
    }
  }

  private def insertRow(array: ExternalAppendOnlyUnsafeRowArray): Long = {
    val valueInserted = random.nextLong()

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](64), 16)
    row.setLong(0, valueInserted)
    array.add(row)
    valueInserted
  }

  private def checkIfValueExists(iterator: Iterator[UnsafeRow], expectedValue: Long): Unit = {
    assert(iterator.hasNext)
    val actualRow = iterator.next()
    assert(actualRow.getLong(0) == expectedValue)
    assert(actualRow.getSizeInBytes == 16)
  }

  private def validateData(
      array: ExternalAppendOnlyUnsafeRowArray,
      expectedValues: ArrayBuffer[Long]): Iterator[UnsafeRow] = {
    val iterator = array.generateIterator()
    for (value <- expectedValues) {
      checkIfValueExists(iterator, value)
    }

    assert(!iterator.hasNext)
    iterator
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int): ArrayBuffer[Long] = {
    val populatedValues = new ArrayBuffer[Long]
    populateRows(array, numRowsToBePopulated, populatedValues)
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int,
      populatedValues: ArrayBuffer[Long]): ArrayBuffer[Long] = {
    for (_ <- 0 until numRowsToBePopulated) {
      populatedValues.append(insertRow(array))
    }
    populatedValues
  }

  private def getNumBytesSpilled: Long = {
    TaskContext.get().taskMetrics().memoryBytesSpilled
  }

  private def assertNoSpill(): Unit = {
    assert(getNumBytesSpilled == 0)
  }

  private def assertSpill(): Unit = {
    assert(getNumBytesSpilled > 0)
  }

  test("insert rows less than the spillThreshold") {
    val spillThreshold = 100
    withExternalArray(spillThreshold) { array =>
      assert(array.isEmpty)

      val expectedValues = populateRows(array, 1)
      assert(!array.isEmpty)
      assert(array.length == 1)

      val iterator1 = validateData(array, expectedValues)

      // Add more rows (but not too many to trigger switch to [[UnsafeExternalSorter]])
      // Verify that NO spill has happened
      populateRows(array, spillThreshold - 1, expectedValues)
      assert(array.length == spillThreshold)
      assertNoSpill()

      val iterator2 = validateData(array, expectedValues)

      assert(!iterator1.hasNext)
      assert(!iterator2.hasNext)
    }
  }

  test("insert rows more than the spillThreshold to force spill") {
    val spillThreshold = 100
    withExternalArray(spillThreshold) { array =>
      val numValuesInserted = 20 * spillThreshold

      assert(array.isEmpty)
      val expectedValues = populateRows(array, 1)
      assert(array.length == 1)

      val iterator1 = validateData(array, expectedValues)

      // Populate more rows to trigger spill. Verify that spill has happened
      populateRows(array, numValuesInserted - 1, expectedValues)
      assert(array.length == numValuesInserted)
      assertSpill()

      val iterator2 = validateData(array, expectedValues)
      assert(!iterator2.hasNext)

      assert(!iterator1.hasNext)
      intercept[ConcurrentModificationException](iterator1.next())
    }
  }

  test("iterator on an empty array should be empty") {
    withExternalArray(spillThreshold = 10) { array =>
      val iterator = array.generateIterator()
      assert(array.isEmpty)
      assert(array.length == 0)
      assert(!iterator.hasNext)
    }
  }

  test("generate iterator with negative start index") {
    withExternalArray(spillThreshold = 2) { array =>
      val exception =
        intercept[ArrayIndexOutOfBoundsException](array.generateIterator(startIndex = -10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array")
      )
    }
  }

  test("generate iterator with start index exceeding array's size (without spill)") {
    val spillThreshold = 2
    withExternalArray(spillThreshold) { array =>
      populateRows(array, spillThreshold / 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))
      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }

  test("generate iterator with start index exceeding array's size (with spill)") {
    val spillThreshold = 2
    withExternalArray(spillThreshold) { array =>
      populateRows(array, spillThreshold * 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }

  test("generate iterator with custom start index (without spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      val expectedValues = populateRows(array, spillThreshold)
      val startIndex = spillThreshold / 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }

  test("generate iterator with custom start index (with spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      val expectedValues = populateRows(array, spillThreshold * 10)
      val startIndex = spillThreshold * 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }

  test("test iterator invalidation (without spill)") {
    withExternalArray(spillThreshold = 10) { array =>
      // insert 2 rows, iterate until the first row
      populateRows(array, 2)

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }

  test("test iterator invalidation (with spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      // Populate enough rows so that spill has happens
      populateRows(array, spillThreshold * 2)
      assertSpill()

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }

  test("clear on an empty the array") {
    withExternalArray(spillThreshold = 2) { array =>
      val iterator = array.generateIterator()
      assert(!iterator.hasNext)

      // multiple clear'ing should not have an side-effect
      array.clear()
      array.clear()
      array.clear()
      assert(array.isEmpty)
      assert(array.length == 0)

      // Clearing an empty array should also invalidate any old iterators
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }

  test("clear array (without spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      // Populate rows ... but not enough to trigger spill
      populateRows(array, spillThreshold / 2)
      assertNoSpill()

      // Clear the array
      array.clear()
      assert(array.isEmpty)

      // Re-populate few rows so that there is no spill
      // Verify the data. Verify that there was no spill
      val expectedValues = populateRows(array, spillThreshold / 3)
      validateData(array, expectedValues)
      assertNoSpill()

      // Populate more rows .. enough to not trigger a spill.
      // Verify the data. Verify that there was no spill
      populateRows(array, spillThreshold / 3, expectedValues)
      validateData(array, expectedValues)
      assertNoSpill()
    }
  }

  test("clear array (with spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      // Populate enough rows to trigger spill
      populateRows(array, spillThreshold * 2)
      val bytesSpilled = getNumBytesSpilled
      assert(bytesSpilled > 0)

      // Clear the array
      array.clear()
      assert(array.isEmpty)

      // Re-populate the array ... but NOT upto the point that there is spill.
      // Verify data. Verify that there was NO "extra" spill
      val expectedValues = populateRows(array, spillThreshold / 2)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled == bytesSpilled)

      // Populate more rows to trigger spill
      // Verify the data. Verify that there was "extra" spill
      populateRows(array, spillThreshold * 2, expectedValues)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled > bytesSpilled)
    }
  }
}
