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
import scala.language.implicitConversions

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.resource.ResourceAmountUtils
import org.apache.spark.resource.ResourceUtils.GPU

class ExecutorResourceInfoSuite extends SparkFunSuite with ExecutorResourceUtils {

  implicit def convertMapLongToDouble(resources: Map[String, Long]): Map[String, Double] = {
    resources.map { case (k, v) => k -> ResourceAmountUtils.toFractionalResource(v) }
  }

  implicit def convertMapDoubleToLong(resources: Map[String, Double]): Map[String, Long] = {
    resources.map { case (k, v) => k -> ResourceAmountUtils.toInternalResource(v) }
  }

  test("Track Executor Resource information") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    assert(info.availableAddrs.sorted sameElements Seq("0", "1", "2", "3"))
    assert(info.assignedAddrs.isEmpty)

    val reqResource = Seq("0", "1").map(addrs => addrs -> 1.0).toMap
    // Acquire addresses
    info.acquire(reqResource)
    assert(info.availableAddrs.sorted sameElements Seq("2", "3"))
    assert(info.assignedAddrs.sorted sameElements Seq("0", "1"))

    // release addresses
    info.release(reqResource)
    assert(info.availableAddrs.sorted sameElements Seq("0", "1", "2", "3"))
    assert(info.assignedAddrs.isEmpty)
  }

  test("Don't allow acquire address that is not available") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    // Acquire some addresses.
    val reqResource = Seq("0", "1").map(addrs => addrs -> 1.0).toMap
    info.acquire(reqResource)
    assert(!info.availableAddrs.contains("1"))
    // Acquire an address that is not available
    val e = intercept[SparkException] {
      info.acquire(convertMapDoubleToLong(Map("1" -> 1.0)))
    }
    assert(e.getMessage.contains("Try to acquire gpu address 1 amount: 1.0, but only 0.0 left."))
  }

  test("Don't allow acquire address that doesn't exist") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    assert(!info.availableAddrs.contains("4"))
    // Acquire an address that doesn't exist
    val e = intercept[SparkException] {
      info.acquire(convertMapDoubleToLong(Map("4" -> 1.0)))
    }
    assert(e.getMessage.contains("Try to acquire an address that doesn't exist."))
  }

  test("Don't allow release address that is not assigned") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    // Acquire addresses
    val reqResource = Seq("0", "1").map(addrs => addrs -> 1.0).toMap
    info.acquire(reqResource)
    assert(!info.assignedAddrs.contains("2"))
    // Release an address that is not assigned
    val e = intercept[SparkException] {
      info.release(convertMapDoubleToLong(Map("2" -> 1.0)))
    }
    assert(e.getMessage.contains("Try to release gpu address 2 amount: 1.0. " +
      "But the total amount: 2.0 after release should be <= 1"))
  }

  test("Don't allow release address that doesn't exist") {
    // Init Executor Resource.
    val info = new ExecutorResourceInfo(GPU, Seq("0", "1", "2", "3"))
    assert(!info.assignedAddrs.contains("4"))
    // Release an address that doesn't exist
    val e = intercept[SparkException] {
      info.release(convertMapDoubleToLong(Map("4" -> 1.0)))
    }
    assert(e.getMessage.contains("Try to release an address that doesn't exist."))
  }

  test("Ensure that we can acquire the same fractions of a resource from an executor") {
    val slotSeq = Seq(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
    val addresses = ArrayBuffer("0", "1", "2", "3")
    slotSeq.foreach { slots =>
      val taskAmount = 1.0 / slots
      val info = new ExecutorResourceInfo(GPU, addresses.toSeq)
      for (_ <- 0 until slots) {
        addresses.foreach(addr => info.acquire(convertMapDoubleToLong(Map(addr -> taskAmount))))
      }

      // All addresses has been assigned
      assert(info.resourcesAmounts.values.toSeq.toSet.size == 1)
      // The left amount of any address should < taskAmount
      assert(ResourceAmountUtils.toFractionalResource(info.resourcesAmounts("0")) < taskAmount)

      addresses.foreach { addr =>
        assertThrows[SparkException] {
          info.acquire(convertMapDoubleToLong(Map(addr -> taskAmount)))
        }
      }
    }
  }

  test("assign/release resource for different task requirements") {
    val execInfo = new ExecutorResourceInfo("gpu", Seq("0", "1", "2", "3"))

    def testAllocation(taskAddressAmount: Map[String, Double],
                       expectedLeftRes: Map[String, Double]
                      ): Unit = {
      execInfo.acquire(taskAddressAmount)
      val leftRes = execInfo.resourcesAmounts
      assert(compareMaps(leftRes, expectedLeftRes))
    }

    def testRelease(releasedRes: Map[String, Double],
                    expectedLeftRes: Map[String, Double]
                   ): Unit = {
      execInfo.release(releasedRes)
      val leftRes = execInfo.resourcesAmounts
      assert(compareMaps(leftRes, expectedLeftRes))
    }

    testAllocation(taskAddressAmount = Map("0" -> 0.2),
      expectedLeftRes = Map("0" -> 0.8, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("0" -> 0.2),
      expectedLeftRes = Map("0" -> 0.6, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("1" -> 1.0, "2" -> 1.0),
      expectedLeftRes = Map("0" -> 0.6, "1" -> 0.0, "2" -> 0.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 0.1, "2" -> 0.8),
      expectedLeftRes = Map("0" -> 0.7, "1" -> 0.0, "2" -> 0.8, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("0" -> 0.50002),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.8, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("3" -> 1.0),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.8, "3" -> 0.0))

    testAllocation(taskAddressAmount = Map("2" -> 0.2),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.6, "3" -> 0.0))

    testRelease(releasedRes = Map("0" -> 0.80002, "1" -> 1.0, "2" -> 0.4, "3" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("0" -> 1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("0" -> 1.0, "1" -> 1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 0.0, "2" -> 1.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 1.0, "1" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testAllocation(taskAddressAmount = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 0.0, "2" -> 0.0, "3" -> 0.0))

    testRelease(releasedRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))
  }
}
