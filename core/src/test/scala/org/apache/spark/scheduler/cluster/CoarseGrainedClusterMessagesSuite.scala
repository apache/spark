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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkFunSuite, TaskState}
import org.apache.spark.resource.CpuAmount
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StatusUpdate
import org.apache.spark.serializer.JavaSerializer

class CoarseGrainedClusterMessagesSuite extends SparkFunSuite {

  private val ser = new JavaSerializer(new SparkConf(false)).newInstance()

  private def roundTrip(su: StatusUpdate): StatusUpdate =
    ser.deserialize[StatusUpdate](ser.serialize(su))

  private def statusUpdate(
      state: TaskState.TaskState = TaskState.RUNNING,
      data: ByteBuffer = ByteBuffer.allocate(0),
      taskCpus: BigDecimal = CpuAmount.normalize(BigDecimal(1)),
      resources: Map[String, Map[String, Long]] = Map.empty): StatusUpdate =
    StatusUpdate("exec-17", 1234567L, state, data, taskCpus, resources)

  test("StatusUpdate round-trips all fields") {
    val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5))
    val resources = Map(
      "gpu" -> Map("0" -> 1L, "1" -> 2L),
      "fpga" -> Map("addr-a" -> 7L))
    val cpus = CpuAmount.normalize(BigDecimal("0.5"))
    val su = statusUpdate(TaskState.FINISHED, data, cpus, resources)
    val rt = roundTrip(su)

    assert(rt.executorId === su.executorId)
    assert(rt.taskId === su.taskId)
    assert(rt.state === su.state)
    assert(rt.taskCpus === cpus)
    assert(rt.taskCpus.scale === cpus.scale)
    assert(rt.data.value === su.data.value)
    assert(rt.resources === resources)
  }

  test("StatusUpdate round-trips every TaskState") {
    TaskState.values.foreach { s =>
      assert(roundTrip(statusUpdate(state = s)).state === s)
    }
  }

  test("SPARK-58192: fractional taskCpus round-trips exactly") {
    Seq("1", "0.5", "0.333333333", "2.25").foreach { amount =>
      val cpus = CpuAmount.normalize(BigDecimal(amount))
      val rt = roundTrip(statusUpdate(taskCpus = cpus))
      assert(rt.taskCpus === cpus, s"value for $amount")
      assert(rt.taskCpus.scale === cpus.scale, s"scale for $amount")
    }
  }

  test("StatusUpdate round-trips with empty data and resources") {
    val rt = roundTrip(statusUpdate())
    assert(rt.data.value.remaining() === 0)
    assert(rt.resources.isEmpty)
  }

  test("StatusUpdate is compact after manual serialization") {
    // With default Java serialization, `state` (a Scala Enumeration value) and `taskCpus` (a
    // BigDecimal) alone were ~1.3KB and an empty-payload message was ~1.7KB. Manual
    // (Externalizable) encoding brings it to ~191 bytes; assert an ample upper bound.
    val size = ser.serialize(statusUpdate()).remaining()
    assert(size < 512, s"StatusUpdate serialized to $size bytes")
  }
}
