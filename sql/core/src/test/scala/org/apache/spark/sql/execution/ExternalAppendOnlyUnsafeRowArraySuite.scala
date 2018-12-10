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

  override def afterAll(): Unit = try {
    TaskContext.unset()
  } finally {
    super.afterAll()
  }

  private def withExternalArray(inMemoryThreshold: Int, spillThreshold: Int, asQueue: Boolean)
                               (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
    sc = new SparkContext("local", "test", new SparkConf(false))

    taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)

    val array = new ExternalAppendOnlyUnsafeRowArray(
      taskContext.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      taskContext,
      asQueue,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      inMemoryThreshold,
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

  val asQueueVals = Array(false, true)

  asQueueVals.foreach(q => {
    test(s"insert rows less than the inMemoryThreshold $q") {
      val (inMemoryThreshold, spillThreshold) = (100, 50)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        assert(array.isEmpty)

        val expectedValues = populateRows(array, 1)
        assert(!array.isEmpty)
        assert(array.length == 1)

        val iterator1 = validateData(array, expectedValues)

        // Add more rows (but not too many to trigger switch to [[UnsafeExternalSorter]])
        // Verify that NO spill has happened
        populateRows(array, inMemoryThreshold - 1, expectedValues)
        assert(array.length == inMemoryThreshold)
        assertNoSpill()

        val iterator2 = validateData(array, expectedValues)

        assert(!iterator1.hasNext)
        assert(!iterator2.hasNext)
      }
    }})

  asQueueVals.foreach(q => {
    test(s"insert rows more than the inMemoryThreshold but less than spillThreshold $q") {
      val (inMemoryThreshold, spillThreshold) = (10, 50)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        assert(array.isEmpty)
        val expectedValues = populateRows(array, inMemoryThreshold - 1)
        assert(array.length == (inMemoryThreshold - 1))
        val iterator1 = validateData(array, expectedValues)
        assertNoSpill()

        // Add more rows to trigger switch to [[UnsafeExternalSorter]] but not too many to cause a
        // spill to happen. Verify that NO spill has happened
        populateRows(array, spillThreshold - expectedValues.length - 1, expectedValues)
        assert(array.length == spillThreshold - 1)
        assertNoSpill()

        val iterator2 = validateData(array, expectedValues)
        assert(!iterator2.hasNext)

        assert(!iterator1.hasNext)
        intercept[ConcurrentModificationException](iterator1.next())
      }
    }})

  asQueueVals.foreach(q => {
    test(s"insert rows enough to force spill $q") {
      val (inMemoryThreshold, spillThreshold) = (20, 10)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        assert(array.isEmpty)
        val expectedValues = populateRows(array, inMemoryThreshold - 1)
        assert(array.length == (inMemoryThreshold - 1))
        val iterator1 = validateData(array, expectedValues)
        assertNoSpill()

        // Add more rows to trigger switch to [[UnsafeExternalSorter]] and cause a spill to happen.
        // Verify that spill has happened
        populateRows(array, 2, expectedValues)
        assert(array.length == inMemoryThreshold + 1)
        assertSpill()

        val iterator2 = validateData(array, expectedValues)
        assert(!iterator2.hasNext)

        assert(!iterator1.hasNext)
        intercept[ConcurrentModificationException](iterator1.next())
      }
    }})

  asQueueVals.foreach(q => {
    test(s"iterator on an empty array should be empty $q") {
      withExternalArray(inMemoryThreshold = 4, spillThreshold = 10, q) { array =>
        val iterator = array.generateIterator()
        assert(array.isEmpty)
        assert(array.length == 0)
        assert(!iterator.hasNext)
      }
    }})

  asQueueVals.foreach(q => {
    test(s"generate iterator with negative start index $q") {
      withExternalArray(inMemoryThreshold = 100, spillThreshold = 56, q) { array =>
        val exception =
          intercept[ArrayIndexOutOfBoundsException](array.generateIterator(startIndex = -10))

        assert(exception.getMessage.contains(
          "Invalid `startIndex` provided for generating iterator over the array")
        )
      }
    }})

  asQueueVals.foreach(q => {
    test(s"generate iterator with start index exceeding array's size (without spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (20, 100)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        populateRows(array, spillThreshold / 2)

        val exception =
          intercept[ArrayIndexOutOfBoundsException](
            array.generateIterator(startIndex = spillThreshold * 10))
        assert(exception.getMessage.contains(
          "Invalid `startIndex` provided for generating iterator over the array"))
      }
    }})

  asQueueVals.foreach(q => {
    test(s"generate iterator with start index exceeding array's size (with spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (20, 100)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        populateRows(array, spillThreshold * 2)

        val exception =
          intercept[ArrayIndexOutOfBoundsException](
            array.generateIterator(startIndex = spillThreshold * 10))

        assert(exception.getMessage.contains(
          "Invalid `startIndex` provided for generating iterator over the array"))
      }
    }})

  asQueueVals.foreach(q => {
    test(s"generate iterator with custom start index (without spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (20, 100)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        val expectedValues = populateRows(array, inMemoryThreshold)
        val startIndex = inMemoryThreshold / 2
        val iterator = array.generateIterator(startIndex = startIndex)
        for (i <- startIndex until expectedValues.length) {
          checkIfValueExists(iterator, expectedValues(i))
        }
      }
    }})

  asQueueVals.foreach(q => {
    test(s"generate iterator with custom start index (with spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (20, 100)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        val expectedValues = populateRows(array, spillThreshold * 10)
        val startIndex = spillThreshold * 2
        val iterator = array.generateIterator(startIndex = startIndex)
        for (i <- startIndex until expectedValues.length) {
          checkIfValueExists(iterator, expectedValues(i))
        }
      }
    }})

  asQueueVals.foreach(q => {
    test(s"test iterator invalidation (without spill) $q") {
      withExternalArray(inMemoryThreshold = 10, spillThreshold = 100, q) { array =>
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
    }})

  test(s"test dequeue with spill") {
    withExternalArray(inMemoryThreshold = 2, spillThreshold = 3, true) { array =>
      // insert 2 rows, iterate until the first row
      populateRows(array, 5)
      assertSpill()

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      val first = iterator.next()

      val first2 = array.dequeue().get
      assert(first.equals(first2))
      val second = array.dequeue().get
      assert(!second.equals(first2))

      val third = array.peek().get
      val third2 = array.dequeue().get
      assert(third.equals(third2))

      assert(array.length == 2)

      array.dequeue()

      populateRows(array, 10)

      array.dequeue()
      array.dequeue()

      assert(array.length == 9)
    }
  }

  asQueueVals.foreach(q => {
    test(s"test iterator invalidation (with spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (2, 10)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        // Populate enough rows so that spill happens
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
    }})

  asQueueVals.foreach(q => {
    test(s"clear on an empty the array $q") {
      withExternalArray(inMemoryThreshold = 2, spillThreshold = 3, q) { array =>
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
    }})

  asQueueVals.foreach(q => {
    test(s"clear array (without spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (10, 100)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
        // Populate rows ... but not enough to trigger spill
        populateRows(array, inMemoryThreshold / 2)
        assertNoSpill()

        // Clear the array
        array.clear()
        assert(array.isEmpty)

        // Re-populate few rows so that there is no spill
        // Verify the data. Verify that there was no spill
        val expectedValues = populateRows(array, inMemoryThreshold / 2)
        validateData(array, expectedValues)
        assertNoSpill()

        // Populate more rows .. enough to not trigger a spill.
        // Verify the data. Verify that there was no spill
        populateRows(array, inMemoryThreshold / 2, expectedValues)
        validateData(array, expectedValues)
        assertNoSpill()
      }
    }})

  asQueueVals.foreach(q => {
    test(s"clear array (with spill) $q") {
      val (inMemoryThreshold, spillThreshold) = (10, 20)
      withExternalArray(inMemoryThreshold, spillThreshold, q) { array =>
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
    }})
}
