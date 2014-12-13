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
    mesosOffers.add(createOffer(1, minMem, minCpu))

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

    EasyMock.replay(driver)

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    assert(backend.doKillExecutors(Seq("s1")))
    EasyMock.verify(driver)
    assert(backend.executorLimit.get.equals(0))

    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer(2, minMem, minCpu))
    backend.resourceOffers(driver, mesosOffers)
    // Verify we didn't launch any new executor
    assert(backend.slaveIdsWithExecutors.size.equals(1))
    assert(backend.pendingRemovedSlaveIds.size.equals(1))

    backend.doRequestTotalExecutors(2)
    backend.resourceOffers(driver, mesosOffers)
    assert(backend.slaveIdsWithExecutors.size.equals(2))
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(backend.slaveIdsWithExecutors.size.equals(1))
    assert(backend.pendingRemovedSlaveIds.size.equals(0))
  }
}
