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

package org.apache.spark.deploy.yarn

import java.io.File
import java.nio.charset.StandardCharsets

import scala.language.postfixOps

import com.google.common.io.Files
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration
import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.tags.ExtendedYarnTest

/**
 * Integration test for the dynamic set spark.dynamicAllocation.maxExecutors
 * depends on queue's maxResources use a mini Yarn cluster.
 */
@ExtendedYarnTest
class DynamicSetMaxExecutorsSuite extends BaseYarnClusterSuite {

  // coresTotal = cpuCores * numNodeManagers = 80
  numNodeManagers = 10
  val cpuCores = 8

  val queueNameRA = "ra"
  val queueNameRB = "rb"
  val queueNameA1 = "a1"
  val queueNameA2 = "a2"
  val ra = CapacitySchedulerConfiguration.ROOT + "." + queueNameRA
  val rb = CapacitySchedulerConfiguration.ROOT + "." + queueNameRB
  val a1 = ra + "." + queueNameA1
  val a2 = ra + "." + queueNameA2

  val aCapacity = 40F
  val aMaximumCapacity = 60F
  val bCapacity = 60F
  val bMaximumCapacity = 100F
  val a1Capacity = 30F
  val a1MaximumCapacity = 70F
  val a2Capacity = 70F
  val dynamicAllocationEnabled = "spark.dynamicAllocation.enabled=true" +
    ",spark.shuffle.service.enabled=true"
  val dynamicAllocationDisabled = "spark.dynamicAllocation.enabled=false" +
    ",spark.shuffle.service.enabled=false"

  override def newYarnConfig(): CapacitySchedulerConfiguration = {

    val yarnConf = new CapacitySchedulerConfiguration()

    // Define top-level queues
    yarnConf.setQueues(CapacitySchedulerConfiguration.ROOT, Array(queueNameRA, queueNameRB))
    yarnConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100)
    yarnConf.setCapacity(ra, aCapacity)
    yarnConf.setMaximumCapacity(ra, aMaximumCapacity)
    yarnConf.setCapacity(rb, bCapacity)
    yarnConf.setMaximumCapacity(rb, bMaximumCapacity)

    // Define 2nd-level queues
    yarnConf.setQueues(ra, Array(queueNameA1, queueNameA2))
    yarnConf.setCapacity(a1, a1Capacity)
    yarnConf.setMaximumCapacity(a1, a1MaximumCapacity)
    yarnConf.setCapacity(a2, a2Capacity)
    yarnConf.set("yarn.nodemanager.resource.cpu-vcores", cpuCores.toString)
    yarnConf
  }

  test(s"run Spark on YARN with dynamicAllocation enabled and ${ queueNameA1 } queue") {
    // a1's executors: 80 * 0.6 * 0.7 = 33
    setMaxExecutors(33, queueNameA1, dynamicAllocationEnabled)
  }

  test(s"run Spark on YARN with dynamicAllocation enabled and ${ queueNameA2 } queue") {
    // a2's executors: 80 * 0.6 * 1 = 48
    setMaxExecutors(48, queueNameA2, dynamicAllocationEnabled)
  }

  test(s"run Spark on YARN with dynamicAllocation enabled and ${ queueNameRB } queue") {
    // b's executors: 80 * 1 = 80
    setMaxExecutors(80, queueNameRB, dynamicAllocationEnabled)
  }

  test(s"run Spark on YARN with dynamicAllocation enabled and ${ queueNameA1 } queue and " +
    s"user set maxExecutors") {
    val executors = 12
    setMaxExecutors(executors, queueNameA1,
      s"${ dynamicAllocationEnabled },${DYN_ALLOCATION_MAX_EXECUTORS.key}=${ executors }")
  }

  test(s"run Spark on YARN with dynamicAllocation disabled and ${ queueNameA1 } queue") {
    setMaxExecutors(DYN_ALLOCATION_MAX_EXECUTORS.defaultValue.get, queueNameA1,
      dynamicAllocationDisabled)
  }

  test(s"run Spark on YARN with dynamicAllocation disabled and ${ queueNameA1 } queue and " +
    s"user set maxExecutors") {
    val executors = 12
    setMaxExecutors(executors, queueNameA1,
      s"${ dynamicAllocationEnabled },${DYN_ALLOCATION_MAX_EXECUTORS.key}=${ executors }")
  }

  test(s"run Spark on YARN with dynamicAllocation enabled and ${ queueNameRB } queue and " +
    s"user set spark.executor.cores") {
    // b's executors = 80 * 1 / 3 = 26
    setMaxExecutors(26, queueNameRB, s"${ dynamicAllocationEnabled },${EXECUTOR_CORES.key}=3")
  }

  private def setMaxExecutors(expectedExecutors: Int,
                              queueName: String,
                              extArgMaps: String): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(true,
      mainClassName(SetMaxExecutors.getClass),
      appArgs = Seq(result.getAbsolutePath, queueName, extArgMaps))
    checkResult(finalState, result, expectedExecutors.toString)
  }

}

private object SetMaxExecutors extends Logging with Matchers {
  def main(args: Array[String]): Unit = {

    var result = Int.MaxValue.toString
    val status = new File(args(0))
    val queueName = args(1)
    val extArgMaps = args(2)

    var sc: SparkContext = null
    try {
      val conf = new SparkConf()
        .setAppName(s"DynamicSetMaxExecutors-${ queueName }-${ extArgMaps }")
        .set(QUEUE_NAME, queueName)

      extArgMaps.split(",").foreach{ kv =>
        val confKVs = kv.split("=")
        conf.set(confKVs(0), confKVs(1))
      }

      sc = new SparkContext(conf)

      result = sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS).toString
    } finally {
      Files.write(result, status, StandardCharsets.UTF_8)
      sc.stop()
    }
  }
}
