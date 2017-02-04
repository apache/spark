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

  private def createSparkConf(): SparkConf = {
    val conf = new SparkConf(false)
    // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
    // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
    conf.set("spark.serializer.objectStreamReset", "1")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    conf
  }

  private def withExternalArray(spillThreshold: Int)
                               (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
    sc = new SparkContext("local", "test", createSparkConf())
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    val array = new ExternalAppendOnlyUnsafeRowArray(spillThreshold)

    try f(array) finally {
      array.clear()
      sc.stop()
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

  private def checkIfValueExits(iterator: Iterator[UnsafeRow], expectedValue: Long): Unit = {
    assert(iterator.hasNext)
    val actualRow = iterator.next()
    assert(actualRow.getLong(0) == expectedValue)
    assert(actualRow.getSizeInBytes == 16)
  }

  test("insert rows less than the spillThreshold") {
    val spillThreshold = 100
    withExternalArray(spillThreshold) { array =>
      assert(array.isEmpty)

      val expectedValues = new ArrayBuffer[Long]
      expectedValues.append(insertRow(array))
      assert(!array.isEmpty)
      assert(array.length == 1)

      val iterator1 = array.generateIterator()
      checkIfValueExits(iterator1, expectedValues.head)
      assert(array.length == 1)
      assert(!iterator1.hasNext)

      // Add more rows (but not too many to trigger switch to [[UnsafeExternalSorter]])
      while (expectedValues.length < spillThreshold) {
        expectedValues.append(insertRow(array))
      }
      assert(array.length == spillThreshold)

      // Verify that NO spill has happened
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled == 0)

      val iterator2 = array.generateIterator()
      for (expectedValue <- expectedValues) {
        checkIfValueExits(iterator2, expectedValue)
      }

      intercept[ConcurrentModificationException](iterator1.hasNext)
      assert(!iterator2.hasNext)
    }
  }

  test("insert rows more than the spillThreshold to force spill") {
    val spillThreshold = 100
    withExternalArray(spillThreshold) { array =>
      assert(array.isEmpty)

      val numValuesInserted = 20 * spillThreshold
      val expectedValues = new ArrayBuffer[Long]

      expectedValues.append(insertRow(array))
      assert(!array.isEmpty)
      assert(array.length == 1)

      val iterator1 = array.generateIterator()
      assert(iterator1.hasNext)
      checkIfValueExits(iterator1, expectedValues.head)
      assert(!iterator1.hasNext)

      while (expectedValues.length < numValuesInserted) {
        expectedValues.append(insertRow(array))
      }
      assert(array.length == numValuesInserted)

      // Verify that spill has happened
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled > 0)

      val iterator2 = array.generateIterator()
      for (value <- expectedValues) {
        checkIfValueExits(iterator2, value)
      }

      assert(!iterator2.hasNext)
      intercept[ConcurrentModificationException](iterator1.hasNext)
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

  test("test iterator invalidation (without spill)") {
    withExternalArray(spillThreshold = 10) { array =>
      // insert 2 rows, iterate until the first row
      insertRow(array)
      insertRow(array)

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      insertRow(array)
      intercept[ConcurrentModificationException](iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      intercept[ConcurrentModificationException](iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }

  test("test iterator invalidation (with spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      for (_ <- 0 until (spillThreshold * 2)) {
        insertRow(array)
      }
      // Verify that spill has happened
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled > 0)

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      insertRow(array)
      intercept[ConcurrentModificationException](iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      intercept[ConcurrentModificationException](iterator.hasNext)
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
      intercept[ConcurrentModificationException](iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }

  test("clear array (without spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      for (_ <- 0 until (spillThreshold / 2)) {
        insertRow(array)
      }
      // Verify that NO spill has happened
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled == 0)

      // Clear the array
      array.clear()
      assert(array.isEmpty)
      assert(array.length == 0)

      // Re-populate few rows so that there is no spill
      val expectedValues = new ArrayBuffer[Long]
      for (_ <- 0 until (spillThreshold / 3)) {
        expectedValues.append(insertRow(array))
      }
      // Verify the data. Verify that there was no spill
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled == 0)
      var iterator = array.generateIterator()
      for (value <- expectedValues) {
        checkIfValueExits(iterator, value)
      }

      // Populate more rows .. enough to not trigger a spill
      for (_ <- 0 until (spillThreshold / 3)) {
        expectedValues.append(insertRow(array))
      }
      // Verify the data. Verify that there was no spill
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled == 0)
      iterator = array.generateIterator()
      for (value <- expectedValues) {
        checkIfValueExits(iterator, value)
      }
    }
  }

  test("clear array (with spill)") {
    val spillThreshold = 10
    withExternalArray(spillThreshold) { array =>
      for (_ <- 0 until (spillThreshold * 2)) {
        insertRow(array)
      }
      // Verify that spill has happened
      val bytesSpilled = TaskContext.get().taskMetrics().memoryBytesSpilled
      assert(bytesSpilled > 0)

      // Clear the array
      array.clear()
      assert(array.isEmpty)
      assert(array.length == 0)

      // Re-populate the array ... but not upto the point that there is spill
      val expectedValues = new ArrayBuffer[Long]
      for (_ <- 0 until (spillThreshold / 2)) {
        expectedValues.append(insertRow(array))
      }
      // Verify the data. Verify that there was no "extra" spill
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled == bytesSpilled)
      var iterator = array.generateIterator()
      for (value <- expectedValues) {
        checkIfValueExits(iterator, value)
      }

      // Populate more rows to trigger spill
      for (_ <- 0 until (spillThreshold * 2)) {
        expectedValues.append(insertRow(array))
      }
      // Verify the data. Verify that there was "extra" spill
      assert(TaskContext.get().taskMetrics().memoryBytesSpilled > bytesSpilled)
      iterator = array.generateIterator()
      for (value <- expectedValues) {
        checkIfValueExits(iterator, value)
      }
    }
  }
}
