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

package org.apache.spark.scheduler.mesos

import org.scalatest.FunSuite
import org.apache.spark.{SparkConf, SparkContext, LocalSparkContext}
import org.apache.spark.scheduler.{TaskDescription, WorkerOffer, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.mesos.{MemoryUtils, MesosSchedulerBackend}
import org.apache.mesos.{Protos, SchedulerDriver}
import org.apache.mesos.Protos._
import org.scalatest.mock.EasyMockSugar
import org.apache.mesos.Protos.Value.Scalar
import org.easymock.{Capture, EasyMock}
import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList, List => JList}
import java.util.Collections
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MesosSchedulerBackendSuite extends FunSuite with LocalSparkContext with EasyMockSugar {
  def createOffer(id: Int, mem: Int, cpu: Int) = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder().setValue(s"o${id.toString}").build()).setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(s"s${id.toString}")).setHostname(s"host${id.toString}").build()
  }

  test("mesos resource offer is launching tasks") {
    val driver = EasyMock.createMock(classOf[SchedulerDriver])
    val taskScheduler = EasyMock.createMock(classOf[TaskSchedulerImpl])

    val sc = EasyMock.createMock(classOf[SparkContext])

    EasyMock.expect(sc.executorMemory).andReturn(100).anyTimes()
    EasyMock.expect(sc.getSparkHome()).andReturn(Option("/path")).anyTimes()
    EasyMock.expect(sc.executorEnvs).andReturn(new mutable.HashMap).anyTimes()
    EasyMock.expect(sc.conf).andReturn(new SparkConf).anyTimes()
    EasyMock.replay(sc)
    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4
    val offers = new java.util.ArrayList[Offer]
    offers.add(createOffer(1, minMem, minCpu))
    offers.add(createOffer(2, minMem - 1, minCpu))
    offers.add(createOffer(3, minMem, minCpu))
    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")
    val workerOffers = new ArrayBuffer[WorkerOffer](2)
    workerOffers.append(new WorkerOffer(
      offers.get(0).getSlaveId.getValue,
      offers.get(0).getHostname,
      2
    ))
    workerOffers.append(new WorkerOffer(
      offers.get(2).getSlaveId.getValue,
      offers.get(2).getHostname,
      2
    ))
    val taskDesc = new TaskDescription(1L, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    EasyMock.expect(taskScheduler.resourceOffers(EasyMock.eq(workerOffers))).andReturn(Seq(Seq(taskDesc)))
    EasyMock.expect(taskScheduler.CPUS_PER_TASK).andReturn(2).anyTimes()
    EasyMock.replay(taskScheduler)
    val capture = new Capture[util.Collection[TaskInfo]]
    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(offers.get(0).getId)),
        EasyMock.capture(capture),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once
    EasyMock.expect(driver.declineOffer(offers.get(1).getId)).andReturn(Status.valueOf(1)).times(1)
    EasyMock.expect(driver.declineOffer(offers.get(2).getId)).andReturn(Status.valueOf(1)).times(1)
    EasyMock.replay(driver)
    backend.resourceOffers(driver, offers)
    EasyMock.verify(driver)
    assert(capture.getValue.size() == 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    val cpus = taskInfo.getResourcesList.get(0)
    assert(cpus.getName.equals("cpus"))
    assert(cpus.getScalar.getValue.equals(2.0))
    assert(taskInfo.getSlaveId.getValue.equals("s1"))
  }

  test("control with multiple task in one slave") {
    val driver = EasyMock.createMock(classOf[SchedulerDriver])
    val taskScheduler = EasyMock.createMock(classOf[TaskSchedulerImpl])

    val sc = EasyMock.createMock(classOf[SparkContext])

    EasyMock.expect(sc.executorMemory).andReturn(1000).anyTimes
    EasyMock.expect(sc.getSparkHome).andReturn(Option("/path")).anyTimes
    EasyMock.expect(sc.executorEnvs).andReturn(new mutable.HashMap).anyTimes
    EasyMock.expect(sc.conf).andReturn(new SparkConf).anyTimes
    EasyMock.replay(sc)

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val offers = new JArrayList[Offer]
    offers.add(createOffer(0, minMem, 16))

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")
    val workerOffersFirst = new ArrayBuffer[WorkerOffer](1)
    workerOffersFirst.append(new WorkerOffer(offers.get(0).getSlaveId.getValue, offers.get(0).getHostname, 14))
    val workerOffersSecond = new ArrayBuffer[WorkerOffer](1)
    workerOffersSecond.append(new WorkerOffer(offers.get(0).getSlaveId.getValue, offers.get(0).getHostname, 16))
    val taskDesc = new TaskDescription(1L, "s0", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    EasyMock.expect(taskScheduler.resourceOffers(EasyMock.eq(workerOffersFirst))).andReturn(Seq(Seq(taskDesc))).once
    EasyMock.expect(taskScheduler.resourceOffers(EasyMock.eq(workerOffersSecond))).andReturn(Seq(Nil)).once
    EasyMock.expect(taskScheduler.CPUS_PER_TASK).andReturn(2).anyTimes
    EasyMock.replay(taskScheduler)

    EasyMock.expect(
      driver.launchTasks(
        EasyMock.anyObject(classOf[util.Collection[Protos.OfferID]]),
        EasyMock.anyObject(classOf[util.Collection[Protos.TaskInfo]]),
        EasyMock.anyObject(classOf[Filters]))
    ).andReturn(Status.valueOf(1)).once
    EasyMock.expect(driver.declineOffer(offers.get(0).getId)).andReturn(Status.valueOf(1)).times(1)
    EasyMock.replay(driver)
    backend.resourceOffers(driver, offers)
    backend.resourceOffers(driver, offers)
    EasyMock.verify(driver)
  }
}
