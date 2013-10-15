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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.EasyMockSugar

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockManager, RDDBlockId, StorageLevel}

// TODO: Test the CacheManager's thread-safety aspects
class CacheManagerSuite extends FunSuite with BeforeAndAfter with EasyMockSugar {
  var sc : SparkContext = _
  var blockManager: BlockManager = _
  var cacheManager: CacheManager = _
  var split: Partition = _
  /** An RDD which returns the values [1, 2, 3, 4]. */
  var rdd: RDD[Int] = _

  before {
    sc = new SparkContext("local", "test")
    blockManager = mock[BlockManager]
    cacheManager = new CacheManager(blockManager)
    split = new Partition { override def index: Int = 0 }
    rdd = new RDD[Int](sc, Nil) {
      override def getPartitions: Array[Partition] = Array(split)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Partition, context: TaskContext) = Array(1, 2, 3, 4).iterator
    }
  }

  after {
    sc.stop()
  }

  test("get uncached rdd") {
    expecting {
      blockManager.get(RDDBlockId(0, 0)).andReturn(None)
      blockManager.put(RDDBlockId(0, 0), ArrayBuffer[Any](1, 2, 3, 4), StorageLevel.MEMORY_ONLY,
        true).andReturn(0)
    }

    whenExecuting(blockManager) {
      val context = new TaskContext(0, 0, 0, interrupted = false, runningLocally = false,
        taskMetrics = null)
      val value = cacheManager.getOrCompute(rdd, split, context, StorageLevel.MEMORY_ONLY)
      assert(value.toList === List(1, 2, 3, 4))
    }
  }

  test("get cached rdd") {
    expecting {
      blockManager.get(RDDBlockId(0, 0)).andReturn(Some(ArrayBuffer(5, 6, 7).iterator))
    }

    whenExecuting(blockManager) {
      val context = new TaskContext(0, 0, 0, interrupted = false, runningLocally = false,
        taskMetrics = null)
      val value = cacheManager.getOrCompute(rdd, split, context, StorageLevel.MEMORY_ONLY)
      assert(value.toList === List(5, 6, 7))
    }
  }

  test("get uncached local rdd") {
    expecting {
      // Local computation should not persist the resulting value, so don't expect a put().
      blockManager.get(RDDBlockId(0, 0)).andReturn(None)
    }

    whenExecuting(blockManager) {
      val context = new TaskContext(0, 0, 0, runningLocally = true, interrupted = false,
        taskMetrics = null)
      val value = cacheManager.getOrCompute(rdd, split, context, StorageLevel.MEMORY_ONLY)
      assert(value.toList === List(1, 2, 3, 4))
    }
  }
}
