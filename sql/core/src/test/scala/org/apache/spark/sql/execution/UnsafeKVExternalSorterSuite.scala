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

import scala.util.Random

import org.apache.spark._
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, RowOrdering, UnsafeProjection}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}

/**
 * Test suite for [[UnsafeKVExternalSorter]], with randomly generated test data.
 */
class UnsafeKVExternalSorterSuite extends SparkFunSuite {

  private val keyTypes = Seq(IntegerType, FloatType, DoubleType, StringType)
  private val valueTypes = Seq(IntegerType, FloatType, DoubleType, StringType)

  testKVSorter(new StructType, new StructType, spill = true)
  testKVSorter(new StructType().add("c1", IntegerType), new StructType, spill = true)
  testKVSorter(new StructType, new StructType().add("c1", IntegerType), spill = true)

  private val rand = new Random(42)
  for (i <- 0 until 6) {
    val keySchema = RandomDataGenerator.randomSchema(rand.nextInt(10) + 1, keyTypes)
    val valueSchema = RandomDataGenerator.randomSchema(rand.nextInt(10) + 1, valueTypes)
    testKVSorter(keySchema, valueSchema, spill = i > 3)
  }

  /**
   * Create a test case using randomly generated data for the given key and value schema.
   *
   * The approach works as follows:
   *
   * - Create input by randomly generating data based on the given schema
   * - Run [[UnsafeKVExternalSorter]] on the generated data
   * - Collect the output from the sorter, and make sure the keys are sorted in ascending order
   * - Sort the input by both key and value, and sort the sorter output also by both key and value.
   *   Compare the sorted input and sorted output together to make sure all the key/values match.
   *
   * If spill is set to true, the sorter will spill probabilistically roughly every 100 records.
   */
  private def testKVSorter(keySchema: StructType, valueSchema: StructType, spill: Boolean): Unit = {

    val keySchemaStr = keySchema.map(_.dataType.simpleString).mkString("[", ",", "]")
    val valueSchemaStr = valueSchema.map(_.dataType.simpleString).mkString("[", ",", "]")

    test(s"kv sorting key schema $keySchemaStr and value schema $valueSchemaStr") {
      // Calling this make sure we have block manager and everything else setup.
      TestSQLContext

      val taskMemMgr = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))
      val shuffleMemMgr = new TestShuffleMemoryManager
      TaskContext.setTaskContext(new TaskContextImpl(
        stageId = 0,
        partitionId = 0,
        taskAttemptId = 98456,
        attemptNumber = 0,
        taskMemoryManager = taskMemMgr,
        metricsSystem = null))

      // Create the data converters
      val kExternalConverter = CatalystTypeConverters.createToCatalystConverter(keySchema)
      val vExternalConverter = CatalystTypeConverters.createToCatalystConverter(valueSchema)
      val kConverter = UnsafeProjection.create(keySchema)
      val vConverter = UnsafeProjection.create(valueSchema)

      val keyDataGen = RandomDataGenerator.forType(keySchema, nullable = false).get
      val valueDataGen = RandomDataGenerator.forType(valueSchema, nullable = false).get

      val input = Seq.fill(1024) {
        val k = kConverter(kExternalConverter.apply(keyDataGen.apply()).asInstanceOf[InternalRow])
        val v = vConverter(vExternalConverter.apply(valueDataGen.apply()).asInstanceOf[InternalRow])
        (k.asInstanceOf[InternalRow].copy(), v.asInstanceOf[InternalRow].copy())
      }

      val sorter = new UnsafeKVExternalSorter(
        keySchema, valueSchema, SparkEnv.get.blockManager, shuffleMemMgr, 16 * 1024 * 1024)

      // Insert generated keys and values into the sorter
      input.foreach { case (k, v) =>
        sorter.insertKV(k.asInstanceOf[UnsafeRow], v.asInstanceOf[UnsafeRow])
        // 1% chance we will spill
        if (rand.nextDouble() < 0.01 && spill) {
          shuffleMemMgr.markAsOutOfMemory()
          sorter.closeCurrentPage()
        }
      }

      // Collect the sorted output
      val out = new scala.collection.mutable.ArrayBuffer[(InternalRow, InternalRow)]
      val iter = sorter.sortedIterator()
      while (iter.next()) {
        out += Tuple2(iter.getKey.copy(), iter.getValue.copy())
      }

      val keyOrdering = RowOrdering.forSchema(keySchema.map(_.dataType))
      val valueOrdering = RowOrdering.forSchema(valueSchema.map(_.dataType))
      val kvOrdering = new Ordering[(InternalRow, InternalRow)] {
        override def compare(x: (InternalRow, InternalRow), y: (InternalRow, InternalRow)): Int = {
          keyOrdering.compare(x._1, y._1) match {
            case 0 => valueOrdering.compare(x._2, y._2)
            case cmp => cmp
          }
        }
      }

      // Testing to make sure output from the sorter is sorted by key
      var prevK: InternalRow = null
      out.zipWithIndex.foreach { case ((k, v), i) =>
        if (prevK != null) {
          assert(keyOrdering.compare(prevK, k) <= 0,
            s"""
               |key is not in sorted order:
               |previous key: $prevK
               |current key : $k
               """.stripMargin)
        }
        prevK = k
      }

      // Testing to make sure the key/value in output matches input
      assert(out.sorted(kvOrdering) === input.sorted(kvOrdering))

      // Make sure there is no memory leak
      val leakedUnsafeMemory: Long = taskMemMgr.cleanUpAllAllocatedMemory
      if (shuffleMemMgr != null) {
        val leakedShuffleMemory: Long = shuffleMemMgr.getMemoryConsumptionForThisTask()
        assert(0L === leakedShuffleMemory)
      }
      assert(0 === leakedUnsafeMemory)
      TaskContext.unset()
    }
  }
}
