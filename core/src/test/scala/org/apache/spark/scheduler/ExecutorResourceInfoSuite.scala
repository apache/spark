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

import org.apache.spark.ResourceName.GPU
import org.apache.spark.SparkFunSuite

class ExecutorResourceInfoSuite extends SparkFunSuite {

  test("Track Executor Resource information") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    assert(info.idleAddresses sameElements Seq("0", "1", "2", "3"))
    assert(info.allocatedAddresses.isEmpty)
    assert(info.reservedAddresses.isEmpty)
    assert(info.getNumOfIdleResources() == 4)

    // Acquire addresses
    info.acquireAddresses(2)
    assert(info.idleAddresses sameElements Seq("2", "3"))
    assert(info.allocatedAddresses.isEmpty)
    assert(info.reservedAddresses sameElements Seq("0", "1"))
    assert(info.getNumOfIdleResources() == 2)

    // Assign addresses
    info.assignAddresses(Array("0", "1"))
    assert(info.idleAddresses sameElements Seq("2", "3"))
    assert(info.allocatedAddresses sameElements Seq("0", "1"))
    assert(info.reservedAddresses.isEmpty)
    assert(info.getNumOfIdleResources() == 2)

    // release addresses
    info.releaseAddresses(Array("0", "1"))
    assert(info.idleAddresses sameElements Seq("2", "3", "0", "1"))
    assert(info.allocatedAddresses.isEmpty)
    assert(info.reservedAddresses.isEmpty)
    assert(info.getNumOfIdleResources() == 4)
  }

  test("Don't allow acquire more addresses than available") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    val e = intercept[AssertionError] {
      info.acquireAddresses(5)
    }
    assert(e.getMessage.contains("Required to take more addresses than available."))
  }

  test("Don't allow assign address that is not reserved") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    // Acquire addresses.
    info.acquireAddresses(2)
    assert(!info.reservedAddresses.contains("2"))
    // Assign an address that is not reserved
    val e = intercept[AssertionError] {
      info.assignAddresses(Array("2"))
    }
    assert(e.getMessage.contains("Try to assign address that is not reserved."))
  }

  test("Don't allow release address that is not reserved or allocated") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    // Acquire addresses.
    info.acquireAddresses(2)
    assert(info.reservedAddresses sameElements Seq("0", "1"))
    // Assign addresses
    info.assignAddresses(Array("0", "1"))
    assert(!info.allocatedAddresses.contains("2"))
    // Release an address that is not allocated
    val e = intercept[AssertionError] {
      info.releaseAddresses(Array("2"))
    }
    assert(e.getMessage.contains("Try to release address that is not reserved or allocated."))
  }
}
