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

package org.apache.spark.sql.execution.aggregate

import java.util.Properties

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.unsafe.KVIterator

class SortBasedAggregationStoreSuite  extends SparkFunSuite with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
    sc = new SparkContext("local[2, 4]", "test", conf)
    val taskManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    TaskContext.setTaskContext(
      new TaskContextImpl(0, 0, 0, 0, 0, 1, taskManager, new Properties, null))
  }

  override def afterAll(): Unit = try {
    TaskContext.unset()
  } finally {
    super.afterAll()
  }

  private val rand = new java.util.Random()

  // In this test, the aggregator is XOR checksum.
  test("merge input kv iterator and aggregation buffer iterator") {

    val inputSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType)))
    val groupingSchema = StructType(Seq(StructField("b", IntegerType)))

    // Schema: a: Int, b: Int
    val inputRow: UnsafeRow = createUnsafeRow(2)

    // Schema: group: Int
    val group: UnsafeRow = createUnsafeRow(1)

    val expected = new mutable.HashMap[Int, Int]()
    val hashMap = new ObjectAggregationMap
    (0 to 5000).foreach { _ =>
      randomKV(inputRow, group)

      // XOR aggregate on first column of input row
      expected.put(group.getInt(0), expected.getOrElse(group.getInt(0), 0) ^ inputRow.getInt(0))
      if (hashMap.getAggregationBuffer(group) == null) {
        hashMap.putAggregationBuffer(group.copy, createNewAggregationBuffer())
      }
      updateInputRow(hashMap.getAggregationBuffer(group), inputRow)
    }

    val store = new SortBasedAggregator(
      createSortedAggBufferIterator(hashMap),
      inputSchema,
      groupingSchema,
      updateInputRow,
      mergeAggBuffer,
      createNewAggregationBuffer)

    (5000 to 100000).foreach { _ =>
      randomKV(inputRow, group)
      // XOR aggregate on first column of input row
      expected.put(group.getInt(0), expected.getOrElse(group.getInt(0), 0) ^ inputRow.getInt(0))
      store.addInput(group, inputRow)
    }

    val iter = store.destructiveIterator()
    while(iter.hasNext) {
      val agg = iter.next()
      assert(agg.aggregationBuffer.getInt(0) == expected(agg.groupingKey.getInt(0)))
    }
  }

  private def createNewAggregationBuffer(): InternalRow = {
    val buffer = createUnsafeRow(1)
    buffer.setInt(0, 0)
    buffer
  }

  private def updateInputRow: (InternalRow, InternalRow) => Unit = {
    (buffer: InternalRow, input: InternalRow) => {
      buffer.setInt(0, buffer.getInt(0) ^ input.getInt(0))
    }
  }

  private def mergeAggBuffer: (InternalRow, InternalRow) => Unit = updateInputRow

  private def createUnsafeRow(numOfField: Int): UnsafeRow = {
    val buffer: Array[Byte] = new Array(1024)
    val row: UnsafeRow = new UnsafeRow(numOfField)
    row.pointTo(buffer, 1024)
    row
  }

  private def randomKV(inputRow: UnsafeRow, group: UnsafeRow): Unit = {
    inputRow.setInt(0, rand.nextInt(100000))
    inputRow.setInt(1, rand.nextInt(10000))
    group.setInt(0, inputRow.getInt(1) % 100)
  }

  def createSortedAggBufferIterator(
      hashMap: ObjectAggregationMap): KVIterator[UnsafeRow, UnsafeRow] = {

    val sortedIterator = hashMap.iterator.toList.sortBy(_.groupingKey.getInt(0)).iterator
    new KVIterator[UnsafeRow, UnsafeRow] {
      var key: UnsafeRow = null
      var value: UnsafeRow = null
      override def next: Boolean = {
        if (sortedIterator.hasNext) {
          val kv = sortedIterator.next()
          key = kv.groupingKey
          value = kv.aggregationBuffer.asInstanceOf[UnsafeRow]
          true
        } else {
          false
        }
      }
      override def getKey(): UnsafeRow = key
      override def getValue(): UnsafeRow = value
      override def close(): Unit = ()
    }
  }
}
