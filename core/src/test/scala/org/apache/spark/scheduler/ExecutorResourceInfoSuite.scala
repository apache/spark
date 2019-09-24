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

package org.apache.spark.scheduler

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.resource.ResourceUtils.GPU

class ExecutorResourceInfoSuite extends SparkFunSuite {

  test("Track Executor Resource information") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, ArrayBuffer("0", "1", "2", "3"))
    assert(info.availableAddrs.sorted sameElements Seq("0", "1", "2", "3"))
    assert(info.assignedAddrs.isEmpty)

    // Acquire addresses
    info.acquire(Seq("0", "1"))
    assert(info.availableAddrs.sorted sameElements Seq("2", "3"))
    assert(info.assignedAddrs.sorted sameElements Seq("0", "1"))

    // release addresses
    info.release(Array("0", "1"))
    assert(info.availableAddrs.sorted sameElements Seq("0", "1", "2", "3"))
    assert(info.assignedAddrs.isEmpty)
  }

  test("Don't allow acquire address that is not available") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, ArrayBuffer("0", "1", "2", "3"))
    // Acquire some addresses.
    info.acquire(Seq("0", "1"))
    assert(!info.availableAddrs.contains("1"))
    // Acquire an address that is not available
    val e = intercept[SparkException] {
      info.acquire(Array("1"))
    }
    assert(e.getMessage.contains("Try to acquire an address that is not available."))
  }

  test("Don't allow acquire address that doesn't exist") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, ArrayBuffer("0", "1", "2", "3"))
    assert(!info.availableAddrs.contains("4"))
    // Acquire an address that doesn't exist
    val e = intercept[SparkException] {
      info.acquire(Array("4"))
    }
    assert(e.getMessage.contains("Try to acquire an address that doesn't exist."))
  }

  test("Don't allow release address that is not assigned") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, ArrayBuffer("0", "1", "2", "3"))
    // Acquire addresses
    info.acquire(Array("0", "1"))
    assert(!info.assignedAddrs.contains("2"))
    // Release an address that is not assigned
    val e = intercept[SparkException] {
      info.release(Array("2"))
    }
    assert(e.getMessage.contains("Try to release an address that is not assigned."))
  }

  test("Don't allow release address that doesn't exist") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, ArrayBuffer("0", "1", "2", "3"))
    assert(!info.assignedAddrs.contains("4"))
    // Release an address that doesn't exist
    val e = intercept[SparkException] {
      info.release(Array("4"))
    }
    assert(e.getMessage.contains("Try to release an address that doesn't exist."))
  }
}
