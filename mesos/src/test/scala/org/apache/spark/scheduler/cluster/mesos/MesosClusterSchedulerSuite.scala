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

import java.nio.ByteBuffer
import java.util.{Collection, Collections, Date}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.{Scalar, Type}
import org.apache.mesos.SchedulerDriver
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerExecutorAdded,
  TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.scheduler.cluster.ExecutorInfo


class MesosClusterSchedulerSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar {

  private val command = new Command("mainClass", Seq("arg"), Map(), Seq(), Seq(), Seq())
  private var driver: SchedulerDriver = _
  private var scheduler: MesosClusterScheduler = _

  private def setScheduler(sparkConfVars: Map[String, String] = null): Unit = {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")

    if (sparkConfVars != null) {
      conf.setAll(sparkConfVars)
    }

    driver = mock[SchedulerDriver]
    scheduler = new MesosClusterScheduler(
      new BlackHoleMesosClusterPersistenceEngineFactory, conf) {
      override def start(): Unit = { ready = true }
    }
    scheduler.start()
  }

  test("can queue drivers") {
    setScheduler()

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 1000, 1, true,
        command, Map[String, String](), "s1", new Date()))
    assert(response.success)
    val response2 =
      scheduler.submitDriver(new MesosDriverDescription(
        "d1", "jar", 1000, 1, true, command, Map[String, String](), "s2", new Date()))
    assert(response2.success)
    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response.submissionId)
    assert(queuedDrivers(1).submissionId == response2.submissionId)
  }

  test("can kill queued drivers") {
    setScheduler()

    val response = scheduler.submitDriver(
        new MesosDriverDescription("d1", "jar", 1000, 1, true,
          command, Map[String, String](), "s1", new Date()))
    assert(response.success)
    val killResponse = scheduler.killDriver(response.submissionId)
    assert(killResponse.success)
    val state = scheduler.getSchedulerState()
    assert(state.queuedDrivers.isEmpty)
  }

  test("accept all roles by default") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    val conf = new SparkConf
    conf.set("spark.mesos.role", "dev")

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(conf)
    when(sc.listenerBus).thenReturn(listenerBus)

    val id = 1
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setRole("*")
      .setScalar(Scalar.newBuilder().setValue(500))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("*")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(1))
    builder.addResourcesBuilder()
      .setName("mem")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(600))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(2))
    val offer = builder.setId(OfferID.newBuilder().setValue(s"o${id.toString}").build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(s"s${id.toString}"))
      .setHostname(s"host${id.toString}").build()

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(offer)

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2 // Deducting 1 for executor
    ))

    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(1)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )

    assert(capture.getValue.size() === 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    assert(taskInfo.getResourcesCount === 1)
    val cpusDev = taskInfo.getResourcesList.get(0)
    assert(cpusDev.getName.equals("cpus"))
    assert(cpusDev.getScalar.getValue.equals(1.0))
    assert(cpusDev.getRole.equals("dev"))
    val executorResources = taskInfo.getExecutor.getResourcesList.asScala
    assert(executorResources.exists { r =>
      r.getName.equals("mem") && r.getScalar.getValue.equals(484.0) && r.getRole.equals("*")
    })
    assert(executorResources.exists { r =>
      r.getName.equals("cpus") && r.getScalar.getValue.equals(1.0) && r.getRole.equals("*")
    })
  }

  test("can ignore default role") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val listenerBus = mock[LiveListenerBus]
    listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    val conf = new SparkConf
    conf.set("spark.mesos.role", "dev")
    conf.set("spark.mesos.ignoreDefaultRoleResources", "true")

    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(conf)
    when(sc.listenerBus).thenReturn(listenerBus)

    val id = 1
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setRole("*")
      .setScalar(Scalar.newBuilder().setValue(500))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("*")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(1))
    builder.addResourcesBuilder()
      .setName("mem")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(600))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setRole("dev")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(2))
    val offer = builder.setId(OfferID.newBuilder().setValue(s"o${id.toString}").build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(s"s${id.toString}"))
      .setHostname(s"host${id.toString}").build()

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(offer)

    val backend = new MesosFineGrainedSchedulerBackend(taskScheduler, sc, "master")

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](1)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2 // Deducting 1 for executor
    ))

    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(1)

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )

    assert(capture.getValue.size() === 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName.equals("n1"))
    assert(taskInfo.getResourcesCount === 1)
    val cpusDev = taskInfo.getResourcesList.get(0)
    assert(cpusDev.getName.equals("cpus"))
    assert(cpusDev.getScalar.getValue.equals(1.0))
    assert(cpusDev.getRole.equals("dev"))
    val executorResources = taskInfo.getExecutor.getResourcesList.asScala
    assert(executorResources.exists { r =>
      r.getName.equals("mem") && r.getScalar.getValue.equals(484.0) && r.getRole.equals("*")
    })
    assert(executorResources.exists { r =>
      r.getName.equals("cpus") && r.getScalar.getValue.equals(1.0) && r.getRole.equals("*")
    })
  }

  test("escapes commandline args for the shell") {
    setScheduler()

    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    val scheduler = new MesosClusterScheduler(
      new BlackHoleMesosClusterPersistenceEngineFactory, conf) {
      override def start(): Unit = { ready = true }
    }
    val escape = scheduler.shellEscape _
    def wrapped(str: String): String = "\"" + str + "\""

    // Wrapped in quotes
    assert(escape("'should be left untouched'") === "'should be left untouched'")
    assert(escape("\"should be left untouched\"") === "\"should be left untouched\"")

    // Harmless
    assert(escape("") === "")
    assert(escape("harmless") === "harmless")
    assert(escape("har-m.l3ss") === "har-m.l3ss")

    // Special Chars escape
    assert(escape("should escape this \" quote") === wrapped("should escape this \\\" quote"))
    assert(escape("shouldescape\"quote") === wrapped("shouldescape\\\"quote"))
    assert(escape("should escape this $ dollar") === wrapped("should escape this \\$ dollar"))
    assert(escape("should escape this ` backtick") === wrapped("should escape this \\` backtick"))
    assert(escape("""should escape this \ backslash""")
      === wrapped("""should escape this \\ backslash"""))
    assert(escape("""\"?""") === wrapped("""\\\"?"""))


    // Special Chars no escape only wrap
    List(" ", "'", "<", ">", "&", "|", "?", "*", ";", "!", "#", "(", ")").foreach(char => {
      assert(escape(s"onlywrap${char}this") === wrapped(s"onlywrap${char}this"))
    })
  }

  test("supports spark.mesos.driverEnv.*") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.executor.home" -> "test",
          "spark.app.name" -> "test",
          "spark.mesos.driverEnv.TEST_ENV" -> "TEST_VAL"),
        "s1",
        new Date()))
    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)
    val tasks = Utils.verifyTaskLaunched(driver, "o1")
    val env = tasks.head.getCommand.getEnvironment.getVariablesList.asScala.map(v =>
      (v.getName, v.getValue)).toMap
    assert(env.getOrElse("TEST_ENV", null) == "TEST_VAL")
  }
}
