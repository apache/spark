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
import org.apache.spark.{SparkEnv, SparkConf, SparkContext, LocalSparkContext}
import org.scalatest.mock.EasyMockSugar
import org.apache.spark.scheduler.cluster.mesos.{MemoryUtils, CoarseMesosSchedulerBackend}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.easymock.EasyMock
import org.apache.mesos.SchedulerDriver
import org.apache.spark.scheduler.TaskSchedulerImpl
import scala.collection.mutable
import akka.actor.ActorSystem
import java.util.Collections

class CoarseMesosSchedulerBackendSuite extends FunSuite with LocalSparkContext with EasyMockSugar {
  def createOffer(offerId: String, slaveId: String, mem: Int, cpu: Int) = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder().setValue(offerId).build()).setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId)).setHostname(s"host${slaveId}").build()
  }

  test("mesos supports killing and limiting executors") {
    val driver = EasyMock.createMock(classOf[SchedulerDriver])
    val taskScheduler = EasyMock.createMock(classOf[TaskSchedulerImpl])

    val se = EasyMock.createMock(classOf[SparkEnv])
    val actorSystem = EasyMock.createMock(classOf[ActorSystem])
    val sparkConf = new SparkConf
    EasyMock.expect(se.actorSystem).andReturn(actorSystem)
    EasyMock.replay(se)
    val sc = EasyMock.createMock(classOf[SparkContext])
    EasyMock.expect(sc.executorMemory).andReturn(100).anyTimes()
    EasyMock.expect(sc.getSparkHome()).andReturn(Option("/path")).anyTimes()
    EasyMock.expect(sc.executorEnvs).andReturn(new mutable.HashMap).anyTimes()
    EasyMock.expect(sc.conf).andReturn(sparkConf).anyTimes()
    EasyMock.expect(sc.env).andReturn(se)
    EasyMock.replay(sc)

    EasyMock.expect(taskScheduler.sc).andReturn(sc)
    EasyMock.replay(taskScheduler)

    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(mesosOffers.get(0).getId)),
        EasyMock.anyObject(),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once

    EasyMock.expect(
      driver.killTask(TaskID.newBuilder().setValue("0").build())
    ).andReturn(Status.valueOf(1))

    EasyMock.expect(
      driver.declineOffer(OfferID.newBuilder().setValue("o2").build())
    ).andReturn(Status.valueOf(1))

    EasyMock.replay(driver)

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    assert(backend.doKillExecutors(Seq("s1/0")))
    assert(backend.executorLimit.get.equals(0))

    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("o2", "s2", minMem, minCpu))
    backend.resourceOffers(driver, mesosOffers2)
    // Verify we didn't launch any new executor
    assert(backend.slaveStatuses.size.equals(1))
    assert(backend.slaveStatuses.values.iterator.next().taskRunning.equals(true))
    assert(backend.pendingRemovedSlaveIds.size.equals(1))

    EasyMock.verify(driver)

    EasyMock.reset(driver)

    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(mesosOffers2.get(0).getId)),
        EasyMock.anyObject(),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once

    EasyMock.replay(driver)

    backend.doRequestTotalExecutors(2)
    backend.resourceOffers(driver, mesosOffers2)
    assert(backend.slaveStatuses.size.equals(2))
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(backend.slaveStatuses.size.equals(1))
    assert(backend.pendingRemovedSlaveIds.size.equals(0))

    EasyMock.verify(driver)
  }

  test("mesos supports killing and relaunching tasks with executors") {
    val driver = EasyMock.createMock(classOf[SchedulerDriver])
    val taskScheduler = EasyMock.createMock(classOf[TaskSchedulerImpl])

    val se = EasyMock.createMock(classOf[SparkEnv])
    val actorSystem = EasyMock.createMock(classOf[ActorSystem])
    val sparkConf = new SparkConf
    EasyMock.expect(se.actorSystem).andReturn(actorSystem)
    EasyMock.replay(se)
    val sc = EasyMock.createMock(classOf[SparkContext])
    EasyMock.expect(sc.executorMemory).andReturn(100).anyTimes()
    EasyMock.expect(sc.getSparkHome()).andReturn(Option("/path")).anyTimes()
    EasyMock.expect(sc.executorEnvs).andReturn(new mutable.HashMap).anyTimes()
    EasyMock.expect(sc.conf).andReturn(sparkConf).anyTimes()
    EasyMock.expect(sc.env).andReturn(se)
    EasyMock.replay(sc)

    EasyMock.expect(taskScheduler.sc).andReturn(sc)
    EasyMock.replay(taskScheduler)

    // Enable shuffle service so it will require extra resources
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt + 1024
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(mesosOffers.get(0).getId)),
        EasyMock.anyObject(),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once

    val offer2 = createOffer("o2", "s1", minMem, 1);

    EasyMock.expect(
      driver.launchTasks(
        EasyMock.eq(Collections.singleton(offer2.getId)),
        EasyMock.anyObject(),
        EasyMock.anyObject(classOf[Filters])
      )
    ).andReturn(Status.valueOf(1)).once

    EasyMock.expect(driver.reviveOffers()).andReturn(Status.valueOf(1)).once

    EasyMock.replay(driver)

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    // Simulate task killed, but executor is still running
    val status = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue("0").build())
      .setSlaveId(SlaveID.newBuilder().setValue("s1").build())
      .setState(TaskState.TASK_KILLED)
      .build

    backend.statusUpdate(driver, status)
    assert(backend.slaveStatuses("s1").taskRunning.equals(false))
    assert(backend.slaveStatuses("s1").executorRunning.equals(true))

    mesosOffers.clear()
    mesosOffers.add(offer2)
    backend.resourceOffers(driver, mesosOffers)
    assert(backend.slaveStatuses("s1").taskRunning.equals(true))
    assert(backend.slaveStatuses("s1").executorRunning.equals(true))

    EasyMock.verify(driver)
  }
}
