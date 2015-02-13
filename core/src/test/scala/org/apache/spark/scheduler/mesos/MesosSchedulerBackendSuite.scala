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

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.SchedulerDriver
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}
import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.cluster.mesos.{MesosSchedulerBackend, MemoryUtils}

class MesosSchedulerBackendSuite extends FunSuite with LocalSparkContext with MockitoSugar {

  test("check spark-class location correctly") {
    val conf = new SparkConf
    conf.set("spark.mesos.executor.home" , "/mesos-home")

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))

    when(sc.conf).thenReturn(conf)
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.executorMemory).thenReturn(100)
    when(sc.listenerBus).thenReturn(listenerBus)
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    // uri is null.
    val executorInfo = mesosSchedulerBackend.createExecutorInfo("test-id")
    assert(executorInfo.getCommand.getValue === s" /mesos-home/bin/spark-class ${classOf[MesosExecutorBackend].getName}")

    // uri exists.
    conf.set("spark.executor.uri", "hdfs:///test-app-1.0.0.tgz")
    val executorInfo1 = mesosSchedulerBackend.createExecutorInfo("test-id")
    assert(executorInfo1.getCommand.getValue === s"cd test-app-1*;  ./bin/spark-class ${classOf[MesosExecutorBackend].getName}")
  }

  test("mesos resource offers result in launching tasks") {
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

    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(new SparkConf)
    when(sc.listenerBus).thenReturn(listenerBus)

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer(1, minMem, minCpu))
    mesosOffers.add(createOffer(2, minMem - 1, minCpu))
    mesosOffers.add(createOffer(3, minMem, minCpu))

    val backend = new MesosSchedulerBackend(taskScheduler, sc, "master")

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](2)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2
    ))
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(2).getSlaveId.getValue,
      mesosOffers.get(2).getHostname,
      2
    ))
    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val capture = ArgumentCaptor.forClass(classOf[util.Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(1).getId)).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(2).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )
    verify(driver, times(1)).declineOffer(mesosOffers.get(1).getId)
    verify(driver, times(1)).declineOffer(mesosOffers.get(2).getId)
    assert(capture.getValue.size() == 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    val cpus = taskInfo.getResourcesList.get(0)
    assert(cpus.getName.equals("cpus"))
    assert(cpus.getScalar.getValue.equals(2.0))
    assert(taskInfo.getSlaveId.getValue.equals("s1"))

    // Unwanted resources offered on an existing node. Make sure they are declined
    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer(1, minMem, minCpu))
    reset(taskScheduler)
    reset(driver)
    when(taskScheduler.resourceOffers(any(classOf[Seq[WorkerOffer]]))).thenReturn(Seq(Seq()))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(driver.declineOffer(mesosOffers2.get(0).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).declineOffer(mesosOffers2.get(0).getId)
  }
}
