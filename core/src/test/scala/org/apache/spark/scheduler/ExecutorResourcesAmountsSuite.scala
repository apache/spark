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
import org.apache.spark.resource.{ResourceAmountUtils, ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.resource.ResourceUtils.GPU

class ExecutorResourcesAmountsSuite extends SparkFunSuite with ExecutorResourceUtils {

  implicit def toFractionalResource(resources: Map[String, Long]): Map[String, Double] =
    resources.map { case (k, v) => k -> ResourceAmountUtils.toFractionalResource(v) }

  implicit def toInternalResource(resources: Map[String, Double]): Map[String, Long] =
    resources.map { case (k, v) => k -> ResourceAmountUtils.toInternalResource(v) }

  implicit def toInternalResourceMap(resources: Map[String, Map[String, Double]]):
      Map[String, Map[String, Long]] =
    resources.map { case (resName, addressesAmountMap) =>
      resName -> addressesAmountMap.map { case (k, v) =>
        k -> ResourceAmountUtils.toInternalResource(v) }
  }

  test("assign to rp without task resources requirement") {
    val executorsInfo = Map(
      "gpu" -> new ExecutorResourceInfo("gpu", Seq("2", "4", "6")),
      "fpga" -> new ExecutorResourceInfo("fpga", Seq("aa", "bb"))
    )
    val availableExecResAmounts = ExecutorResourcesAmounts(executorsInfo)
    assert(availableExecResAmounts.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val treqs = new TaskResourceRequests().cpus(1)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    // assign nothing to rp without resource profile
    val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    assert(assigned.isDefined)
    assigned.foreach(resource => assert(resource.isEmpty))
  }

  test("Convert ExecutorResourceInfos to ExecutorResourcesAmounts") {
    val executorsInfo = Map(
      "gpu" -> new ExecutorResourceInfo("gpu", Seq("2", "4", "6")),
      "fpga" -> new ExecutorResourceInfo("fpga", Seq("aa", "bb"))
    )
    // default resources amounts of executors info
    executorsInfo.foreach { case (rName, rInfo) =>
      if (rName == "gpu") {
        assert(compareMaps(rInfo.resourcesAmounts, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(rInfo.resourcesAmounts, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    val availableExecResAmounts = ExecutorResourcesAmounts(executorsInfo)
    assert(availableExecResAmounts.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    // Update executors info
    // executors info shouldn't be changed.
    executorsInfo.foreach { case (rName, rInfo) =>
      if (rName == "gpu") {
        rInfo.acquire(toInternalResource(Map("2" -> 0.4, "6" -> 0.6)))
      } else {
        rInfo.acquire(toInternalResource(Map("aa" -> 0.2, "bb" -> 0.7)))
      }
    }

    executorsInfo.foreach { case (rName, rInfo) =>
      if (rName == "gpu") {
        assert(compareMaps(rInfo.resourcesAmounts, Map("2" -> 0.6, "4" -> 1.0, "6" -> 0.4)))
      } else {
        assert(compareMaps(rInfo.resourcesAmounts, Map("aa" -> 0.8, "bb" -> 0.3)))
      }
    }

    val availableExecResAmounts1 = ExecutorResourcesAmounts(executorsInfo)
    assert(availableExecResAmounts1.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val availableRes1 = availableExecResAmounts1.availableResources
    availableRes1.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 0.6, "4" -> 1.0, "6" -> 0.4)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 0.8, "bb" -> 0.3)))
      }
    }

  }

  test("ExecutorResourcesAmounts shouldn't change ExecutorResourceInfo") {
    val executorsInfo = Map(
      "gpu" -> new ExecutorResourceInfo("gpu", Seq("2", "4", "6")),
      "fpga" -> new ExecutorResourceInfo("fpga", Seq("aa", "bb"))
    )

    // default resources amounts of executors info
    executorsInfo.foreach { case (rName, rInfo) =>
      if (rName == "gpu") {
        assert(compareMaps(rInfo.resourcesAmounts, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(rInfo.resourcesAmounts, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    val availableExecResAmounts = ExecutorResourcesAmounts(executorsInfo)
    assert(availableExecResAmounts.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val gpuTaskAmount = 0.1
    val treqs = new TaskResourceRequests().resource("gpu", gpuTaskAmount)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    // taskMount = 0.1 < 1.0 which can be assigned.
    val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    // update the value
    availableExecResAmounts.acquire(assigned.get)

    val availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount,
          Map("2" -> (1.0 - gpuTaskAmount), "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    // executors info shouldn't be changed.
    executorsInfo.foreach { case (rName, rInfo) =>
      if (rName == "gpu") {
        assert(compareMaps(rInfo.resourcesAmounts, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(rInfo.resourcesAmounts, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }
  }

  test("executor resources are not matching to the task requirement") {
    val totalRes = Map("gpu" -> Map("2" -> 0.4))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    val gpuTaskAmount = 0.6
    val treqs = new TaskResourceRequests()
      .resource("gpu", gpuTaskAmount)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    assert(assigned.isEmpty)
  }

  test("part of executor resources are not matching to the task requirement") {
    val totalRes = Map("gpu" -> Map("2" -> 0.4), "fpga" -> Map("aa" -> 0.8))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    // normal allocation
    val gpuTaskAmount = 0.3
    val fpgaTaskAmount = 0.8
    val treqs = new TaskResourceRequests()
      .resource("gpu", gpuTaskAmount)
      .resource("fpga", fpgaTaskAmount)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    var assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    assert(assigned.isDefined)
    assigned.foreach(resource => assert(resource.nonEmpty))

    val treqs1 = new TaskResourceRequests()
      .resource("gpu", gpuTaskAmount)
      .resource("fpga", 0.9) // couldn't allocate fpga
    val rp1 = new ResourceProfileBuilder().require(treqs1).build()

    assigned = availableExecResAmounts.assignAddressesCustomResources(rp1)
    assert(assigned.isEmpty)
  }

  test("the total amount after release should be <= 1.0") {
    val totalRes = Map("gpu" -> Map("2" -> 0.4))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    val e = intercept[SparkException] {
      availableExecResAmounts.release(toInternalResourceMap(Map("gpu" -> Map("2" -> 0.7))))
    }
    assert(e.getMessage.contains("after releasing gpu address 2 should be <= 1.0"))

    availableExecResAmounts.release(toInternalResourceMap(Map("gpu" -> Map("2" -> 0.6))))
    assert(compareMaps(availableExecResAmounts.availableResources("gpu"), Map("2" -> 1.0)))
  }

  test("the total amount after acquire should be >= 0") {
    val totalRes = Map("gpu" -> Map("2" -> 0.4))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    val e = intercept[SparkException] {
      availableExecResAmounts.acquire(toInternalResourceMap(Map("gpu" -> Map("2" -> 0.6))))
    }
    assert(e.getMessage.contains("after acquiring gpu address 2 should be >= 0"))

    availableExecResAmounts.acquire(toInternalResourceMap(Map("gpu" -> Map("2" -> 0.4))))
    assert(compareMaps(availableExecResAmounts.availableResources("gpu"), Map("2" -> 0.0)))
  }

  test("Ensure that we can acquire the same fractions of a resource") {
    val slotSeq = Seq(31235, 1024, 512, 256, 128, 64, 32, 16, 12, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
    val addresses = ArrayBuffer("0", "1", "2", "3")
    val info = new ExecutorResourceInfo(GPU, addresses.toSeq)

    slotSeq.foreach { slots =>
      val taskAmount = 1.0 / slots
      val availableExecResAmounts = ExecutorResourcesAmounts(Map(GPU -> info))
      for (_ <- 0 until slots) {
        addresses.foreach(addr =>
          availableExecResAmounts.acquire(
            toInternalResourceMap(Map(GPU -> Map(addr -> taskAmount)))))
      }

      assert(availableExecResAmounts.availableResources.size === 1)
      // All addresses has been assigned
      assert(availableExecResAmounts.availableResources(GPU).values.toSeq.toSet.size === 1)
      // The left amount of any address should < taskAmount
      assert(availableExecResAmounts.availableResources(GPU)("0") < taskAmount)

      addresses.foreach { addr =>
        assertThrows[SparkException] {
          availableExecResAmounts.acquire(
            toInternalResourceMap(Map(GPU -> Map(addr -> taskAmount))))
        }
      }
    }
  }

  test("assign acquire release on single task resource request") {
    val executorsInfo = Map(
      "gpu" -> new ExecutorResourceInfo("gpu", Seq("2", "4", "6")),
      "fpga" -> new ExecutorResourceInfo("fpga", Seq("aa", "bb"))
    )

    val availableExecResAmounts = ExecutorResourcesAmounts(executorsInfo)

    assert(availableExecResAmounts.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val gpuTaskAmount = 0.1
    val treqs = new TaskResourceRequests().resource("gpu", gpuTaskAmount)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    // taskMount = 0.1 < 1.0 which can be assigned.
    val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    assert(assigned.isDefined)
    assigned.foreach { resource =>
      assert(resource.size === 1)
      assert(resource.keys.toSeq === Seq("gpu"))
      assert(resource("gpu").size === 1)
      assert(resource("gpu").keys.toSeq === Seq("2"))
      assert(ResourceAmountUtils.toFractionalResource(resource("gpu")("2")) === gpuTaskAmount)
    }

    // assign will not update the real value.
    var availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    // acquire will updates the value
    availableExecResAmounts.acquire(assigned.get)

    // after acquire
    availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map(
          "2" -> (1.0 - gpuTaskAmount),
          "4" -> 1.0,
          "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    // release
    availableExecResAmounts.release(assigned.get)

    availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }
  }

  test("assign acquire release on multiple task resources request") {
    val executorsInfo = Map(
      "gpu" -> new ExecutorResourceInfo("gpu", Seq("2", "4", "6")),
      "fpga" -> new ExecutorResourceInfo("fpga", Seq("aa", "bb"))
    )

    val availableExecResAmounts = ExecutorResourcesAmounts(executorsInfo)

    assert(availableExecResAmounts.resourceAddressAmount === Map("gpu" -> 3, "fpga" -> 2))

    val gpuTaskAmount = 0.1
    val fpgaTaskAmount = 0.3
    val treqs = new TaskResourceRequests()
      .resource("gpu", gpuTaskAmount)
      .resource("fpga", fpgaTaskAmount)
    val rp = new ResourceProfileBuilder().require(treqs).build()

    // taskMount = 0.1 < 1.0 which can be assigned.
    val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
    assert(assigned.isDefined)
    assigned.foreach { resourceAmounts =>
      assert(resourceAmounts.size === 2)
      assert(resourceAmounts.keys.toSeq.sorted === Seq("gpu", "fpga").sorted)

      assert(resourceAmounts("gpu").size === 1)
      assert(resourceAmounts("gpu").keys.toSeq === Seq("2"))
      assert(ResourceAmountUtils.toFractionalResource(resourceAmounts("gpu")("2")) ===
        gpuTaskAmount)

      assert(resourceAmounts("fpga").size === 1)
      assert(resourceAmounts("fpga").keys.toSeq === Seq("aa"))
      assert(ResourceAmountUtils.toFractionalResource(resourceAmounts("fpga")("aa")) ===
        fpgaTaskAmount)
    }

    // assign will not update the real value.
    var availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }

    // acquire will updates the value
    availableExecResAmounts.acquire(assigned.get)

    // after acquire
    availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount,
          Map("2" -> (1.0 - gpuTaskAmount), "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> (1.0 - fpgaTaskAmount), "bb" -> 1.0)))
      }
    }

    // release
    availableExecResAmounts.release(assigned.get)

    availableRes = availableExecResAmounts.availableResources
    availableRes.foreach { case (rName, addressesAmount) =>
      if (rName == "gpu") {
        assert(compareMaps(addressesAmount, Map("2" -> 1.0, "4" -> 1.0, "6" -> 1.0)))
      } else {
        assert(compareMaps(addressesAmount, Map("aa" -> 1.0, "bb" -> 1.0)))
      }
    }
  }

  test("assign/release resource for different task requirements") {
    val totalRes = Map("gpu" -> Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    def testAllocation(taskAmount: Double,
                       expectedAssignedAddress: Array[String],
                       expectedAssignedAmount: Array[Double],
                       expectedLeftRes: Map[String, Double]
                      ): Unit = {
      val treqs = new TaskResourceRequests().resource("gpu", taskAmount)
      val rp = new ResourceProfileBuilder().require(treqs).build()
      val assigned = availableExecResAmounts.assignAddressesCustomResources(rp)
      assert(assigned.isDefined)
      assigned.foreach { resources =>
        assert(
          resources("gpu").values.toArray.sorted.map(ResourceAmountUtils.toFractionalResource)
          === expectedAssignedAmount.sorted)

        availableExecResAmounts.acquire(resources)

        val leftRes = availableExecResAmounts.availableResources
        assert(leftRes.size == 1)
        assert(leftRes.keys.toSeq.head == "gpu")
        assert(compareMaps(leftRes("gpu"), expectedLeftRes))
      }
    }

    def testRelease(releasedRes: Map[String, Double],
                    expectedLeftRes: Map[String, Double]
                   ): Unit = {
      availableExecResAmounts.release(Map("gpu" -> releasedRes))

      val leftRes = availableExecResAmounts.availableResources
      assert(leftRes.size == 1)
      assert(leftRes.keys.toSeq.head == "gpu")
      assert(compareMaps(leftRes("gpu"), expectedLeftRes))
    }

    // request 0.2 gpu, ExecutorResourcesAmounts should assign "0",
    testAllocation(taskAmount = 0.2,
      expectedAssignedAddress = Array("0"),
      expectedAssignedAmount = Array(0.2),
      expectedLeftRes = Map("0" -> 0.8, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    // request 0.2 gpu again, ExecutorResourcesAmounts should assign "0",
    testAllocation(taskAmount = 0.2,
      expectedAssignedAddress = Array("0"),
      expectedAssignedAmount = Array(0.2),
      expectedLeftRes = Map("0" -> 0.6, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    // request 2 gpus, ExecutorResourcesAmounts should assign "1" and "2",
    testAllocation(taskAmount = 2,
      expectedAssignedAddress = Array("1", "2"),
      expectedAssignedAmount = Array(1.0, 1.0),
      expectedLeftRes = Map("0" -> 0.6, "1" -> 0.0, "2" -> 0.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 0.1, "2" -> 0.8),
      expectedLeftRes = Map("0" -> 0.7, "1" -> 0.0, "2" -> 0.8, "3" -> 1.0))

    // request 0.50002 gpu, ExecutorResourcesAmounts should assign "0",
    testAllocation(taskAmount = 0.50002,
      expectedAssignedAddress = Array("0"),
      expectedAssignedAmount = Array(0.50002),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.8, "3" -> 1.0))

    // request 1 gpu, ExecutorResourcesAmounts should assign "3",
    testAllocation(taskAmount = 1.0,
      expectedAssignedAddress = Array("3"),
      expectedAssignedAmount = Array(1.0),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.8, "3" -> 0.0))

    // request 0.2 gpu, ExecutorResourcesAmounts should assign "2",
    testAllocation(taskAmount = 0.2,
      expectedAssignedAddress = Array("2"),
      expectedAssignedAmount = Array(0.2),
      expectedLeftRes = Map("0" -> 0.19998, "1" -> 0.0, "2" -> 0.6, "3" -> 0.0))

    testRelease(releasedRes = Map("0" -> 0.80002, "1" -> 1.0, "2" -> 0.4, "3" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    // request 1 gpus, ExecutorResourcesAmounts should assign "0"
    testAllocation(taskAmount = 1.0,
      expectedAssignedAddress = Array("0"),
      expectedAssignedAmount = Array(1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    // request 2 gpus, ExecutorResourcesAmounts should assign "0", "1"
    testAllocation(taskAmount = 2.0,
      expectedAssignedAddress = Array("0", "1"),
      expectedAssignedAmount = Array(1.0, 1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 0.0, "2" -> 1.0, "3" -> 1.0))

    testRelease(releasedRes = Map("0" -> 1.0, "1" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))

    // request 4 gpus, ExecutorResourcesAmounts should assign "0", "1", "2", "3"
    testAllocation(taskAmount = 4.0,
      expectedAssignedAddress = Array("0", "1", "2", "3"),
      expectedAssignedAmount = Array(1.0, 1.0, 1.0, 1.0),
      expectedLeftRes = Map("0" -> 0.0, "1" -> 0.0, "2" -> 0.0, "3" -> 0.0))

    testRelease(releasedRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0),
      expectedLeftRes = Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))
  }

  test("Don't allow acquire resource or address that is not available") {
    // Init Executor Resource.
    val totalRes = Map("gpu" -> Map("0" -> 1.0, "1" -> 1.0, "2" -> 1.0, "3" -> 1.0))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    // Acquire an address from a resource that doesn't exist
    val e = intercept[SparkException] {
      availableExecResAmounts.acquire(toInternalResourceMap(Map("fpga" -> Map("1" -> 1.0))))
    }
    assert(e.getMessage.contains("Try to acquire an address from fpga that doesn't exist"))

    // Acquire an address that is not available
    val e1 = intercept[SparkException] {
      availableExecResAmounts.acquire(toInternalResourceMap(Map("gpu" -> Map("6" -> 1.0))))
    }
    assert(e1.getMessage.contains("Try to acquire an address that doesn't exist"))
  }

  test("Don't allow release resource or address that is not available") {
    // Init Executor Resource.
    val totalRes = Map("gpu" -> Map("0" -> 0.5, "1" -> 0.5, "2" -> 0.5, "3" -> 0.5))
    val availableExecResAmounts = new ExecutorResourcesAmounts(totalRes)

    // Acquire an address from a resource that doesn't exist
    val e = intercept[SparkException] {
      availableExecResAmounts.release(toInternalResourceMap(Map("fpga" -> Map("1" -> 0.1))))
    }
    assert(e.getMessage.contains("Try to release an address from fpga that doesn't exist"))

    // Acquire an address that is not available
    val e1 = intercept[SparkException] {
      availableExecResAmounts.release(toInternalResourceMap(Map("gpu" -> Map("6" -> 0.1))))
    }
    assert(e1.getMessage.contains("Try to release an address that is not assigned"))
  }

}
