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

import org.scalatest.{Matchers, FunSuite}
import org.apache.spark.{SparkEnv, SparkConf, SparkContext, LocalSparkContext}
import org.apache.spark.scheduler.{TaskDescription, WorkerOffer, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MemoryUtils}
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.Protos._
import org.scalatest.mock.EasyMockSugar
import org.apache.mesos.Protos.Value.Scalar
import org.easymock.{Capture, EasyMock}
import java.nio.ByteBuffer
import java.util.Collections
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorSystem

class CoarseMesosSchedulerBackendSuite extends FunSuite
  with Matchers
  with LocalSparkContext
  with EasyMockSugar {

  test("launch multiple executors") {
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

    val driver = EasyMock.createMock(classOf[SchedulerDriver])
    val taskScheduler = EasyMock.createMock(classOf[TaskSchedulerImpl])

    val env = EasyMock.createMock(classOf[SparkEnv])
    val actorSystem = EasyMock.createMock(classOf[ActorSystem])
    EasyMock.expect(env.actorSystem).andReturn(actorSystem)
    EasyMock.replay(env)

    val sc = EasyMock.createMock(classOf[SparkContext])
    EasyMock.expect(sc.executorMemory).andReturn(100).anyTimes()
    EasyMock.expect(sc.getSparkHome()).andReturn(Option("/path")).anyTimes()
    EasyMock.expect(sc.executorEnvs).andReturn(new mutable.HashMap).anyTimes()
    EasyMock.expect(sc.env).andReturn(env)
    val conf = new SparkConf
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.driver.port", "1234")
    conf.set("spark.mesos.coarse.executors.max", "2")
    conf.set("spark.mesos.coarse.cores.max", "2")
    EasyMock.expect(sc.conf).andReturn(conf).anyTimes()
    EasyMock.replay(sc)

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 2

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer(1, minMem * 2, minCpu * 2))

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2
    ))

    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    val taskDesc2 = new TaskDescription(2L, 0, "s2", "n2", 0, ByteBuffer.wrap(new Array[Byte](0)))
    EasyMock.expect(taskScheduler.resourceOffers(EasyMock.eq(expectedWorkerOffers)))
      .andReturn(Seq(Seq(taskDesc, taskDesc2)))
    EasyMock.expect(taskScheduler.CPUS_PER_TASK).andReturn(2).anyTimes()
    EasyMock.expect(taskScheduler.sc).andReturn(sc)
    EasyMock.replay(taskScheduler)

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")

    val capture = new Capture[util.Collection[TaskInfo]]
    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(mesosOffers.get(0).getId)),
        EasyMock.capture(capture),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once
    EasyMock.replay(driver)

    backend.resourceOffers(driver, mesosOffers)

    EasyMock.verify(driver)
    assert(capture.getValue.size() == 2)
    val iter = capture.getValue.iterator()
    val taskInfo = iter.next()
    taskInfo.getName should be("Task 0")
    val cpus = taskInfo.getResourcesList.get(0)
    cpus.getName should be("cpus")
    cpus.getScalar.getValue should be(2.0)
    taskInfo.getSlaveId.getValue should be("s1")

    val taskInfo2 = iter.next()
    taskInfo2.getName should be("Task 1")
    val cpus2 = taskInfo2.getResourcesList.get(0)
    cpus2.getName should be("cpus")
    cpus2.getScalar.getValue should be(2.0)
    taskInfo2.getSlaveId.getValue should be("s1")

    // Already capped the max executors, shouldn't launch a new one.
    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer(1, minMem, minCpu))
    EasyMock.reset(taskScheduler)
    EasyMock.reset(driver)
    EasyMock.expect(taskScheduler.resourceOffers(EasyMock.anyObject(classOf[Seq[WorkerOffer]])).andReturn(Seq(Seq())))
    EasyMock.expect(taskScheduler.CPUS_PER_TASK).andReturn(2).anyTimes()
    EasyMock.replay(taskScheduler)
    EasyMock.expect(driver.declineOffer(mesosOffers2.get(0).getId)).andReturn(Status.valueOf(1)).times(1)
    EasyMock.replay(driver)

    backend.resourceOffers(driver, mesosOffers2)
    EasyMock.verify(driver)
  }
}
