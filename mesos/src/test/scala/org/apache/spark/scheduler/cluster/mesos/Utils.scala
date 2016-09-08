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

package org.apache.spark.scheduler.cluster.mesos

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.{Range => MesosRange, Ranges, Scalar}
import org.apache.mesos.SchedulerDriver
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Mockito._

object Utils {
  def createOffer(
      offerId: String,
      slaveId: String,
      mem: Int,
      cpu: Int,
      ports: Option[(Long, Long)] = None): Offer = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    ports.foreach { resourcePorts =>
      builder.addResourcesBuilder()
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Ranges.newBuilder().addRange(MesosRange.newBuilder()
          .setBegin(resourcePorts._1).setEnd(resourcePorts._2).build()))
    }
    builder.setId(createOfferId(offerId))
      .setFrameworkId(FrameworkID.newBuilder()
        .setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host${slaveId}")
      .build()
  }

  def verifyTaskLaunched(driver: SchedulerDriver, offerId: String): List[TaskInfo] = {
    val captor = ArgumentCaptor.forClass(classOf[java.util.Collection[TaskInfo]])
    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(createOfferId(offerId))),
      captor.capture())
    captor.getValue.asScala.toList
  }

  def createOfferId(offerId: String): OfferID = {
    OfferID.newBuilder().setValue(offerId).build()
  }

  def createSlaveId(slaveId: String): SlaveID = {
    SlaveID.newBuilder().setValue(slaveId).build()
  }

  def createExecutorId(executorId: String): ExecutorID = {
    ExecutorID.newBuilder().setValue(executorId).build()
  }

  def createTaskId(taskId: String): TaskID = {
    TaskID.newBuilder().setValue(taskId).build()
  }
}

