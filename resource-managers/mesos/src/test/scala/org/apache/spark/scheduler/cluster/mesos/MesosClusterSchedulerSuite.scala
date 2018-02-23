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

import java.util.{Collection, Collections, Date}

import scala.collection.JavaConverters._

import org.apache.mesos.Protos.{Environment, Secret, TaskState => MesosTaskState, _}
import org.apache.mesos.Protos.Value.{Scalar, Type}
import org.apache.mesos.SchedulerDriver
import org.mockito.{ArgumentCaptor, Matchers}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.mesos.config

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
    scheduler.registered(driver, Utils.TEST_FRAMEWORK_ID, Utils.TEST_MASTER_INFO)
  }

  private def testDriverDescription(submissionId: String): MesosDriverDescription = {
    new MesosDriverDescription(
      "d1",
      "jar",
      1000,
      1,
      true,
      command,
      Map[String, String](),
      submissionId,
      new Date())
  }

  test("can queue drivers") {
    setScheduler()

    val response = scheduler.submitDriver(testDriverDescription("s1"))
    assert(response.success)
    verify(driver, times(1)).reviveOffers()

    val response2 = scheduler.submitDriver(testDriverDescription("s2"))
    assert(response2.success)

    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response.submissionId)
    assert(queuedDrivers(1).submissionId == response2.submissionId)
  }

  test("can kill queued drivers") {
    setScheduler()

    val response = scheduler.submitDriver(testDriverDescription("s1"))
    assert(response.success)
    val killResponse = scheduler.killDriver(response.submissionId)
    assert(killResponse.success)
    val state = scheduler.getSchedulerState()
    assert(state.queuedDrivers.isEmpty)
  }

  test("can handle multiple roles") {
    setScheduler()

    val driver = mock[SchedulerDriver]
    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 1200, 1.5, true,
        command,
        Map(("spark.mesos.executor.home", "test"), ("spark.app.name", "test")),
        "s1",
        new Date()))
    assert(response.success)
    val offer = Offer.newBuilder()
      .addResources(
        Resource.newBuilder().setRole("*")
          .setScalar(Scalar.newBuilder().setValue(1).build()).setName("cpus").setType(Type.SCALAR))
      .addResources(
        Resource.newBuilder().setRole("*")
          .setScalar(Scalar.newBuilder().setValue(1000).build())
          .setName("mem")
          .setType(Type.SCALAR))
      .addResources(
        Resource.newBuilder().setRole("role2")
          .setScalar(Scalar.newBuilder().setValue(1).build()).setName("cpus").setType(Type.SCALAR))
      .addResources(
        Resource.newBuilder().setRole("role2")
          .setScalar(Scalar.newBuilder().setValue(500).build()).setName("mem").setType(Type.SCALAR))
      .setId(OfferID.newBuilder().setValue("o1").build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1").build())
      .setSlaveId(SlaveID.newBuilder().setValue("s1").build())
      .setHostname("host1")
      .build()

    val capture = ArgumentCaptor.forClass(classOf[Collection[TaskInfo]])

    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(offer.getId)),
        capture.capture())
    ).thenReturn(Status.valueOf(1))

    scheduler.resourceOffers(driver, Collections.singletonList(offer))

    val taskInfos = capture.getValue
    assert(taskInfos.size() == 1)
    val taskInfo = taskInfos.iterator().next()
    val resources = taskInfo.getResourcesList
    assert(scheduler.getResource(resources, "cpus") == 1.5)
    assert(scheduler.getResource(resources, "mem") == 1200)
    val resourcesSeq: Seq[Resource] = resources.asScala
    val cpus = resourcesSeq.filter(_.getName.equals("cpus")).toList
    assert(cpus.size == 2)
    assert(cpus.exists(_.getRole().equals("role2")))
    assert(cpus.exists(_.getRole().equals("*")))
    val mem = resourcesSeq.filter(_.getName.equals("mem")).toList
    assert(mem.size == 2)
    assert(mem.exists(_.getRole().equals("role2")))
    assert(mem.exists(_.getRole().equals("*")))

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer.getId)),
      capture.capture()
    )
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

  test("supports spark.mesos.network.name and spark.mesos.network.labels") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.executor.home" -> "test",
          "spark.app.name" -> "test",
          "spark.mesos.network.name" -> "test-network-name",
          "spark.mesos.network.labels" -> "key1:val1,key2:val2"),
        "s1",
        new Date()))

    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)

    val launchedTasks = Utils.verifyTaskLaunched(driver, "o1")
    val networkInfos = launchedTasks.head.getContainer.getNetworkInfosList
    assert(networkInfos.size == 1)
    assert(networkInfos.get(0).getName == "test-network-name")
    assert(networkInfos.get(0).getLabels.getLabels(0).getKey == "key1")
    assert(networkInfos.get(0).getLabels.getLabels(0).getValue == "val1")
    assert(networkInfos.get(0).getLabels.getLabels(1).getKey == "key2")
    assert(networkInfos.get(0).getLabels.getLabels(1).getValue == "val2")
  }

  test("accept/decline offers with driver constraints") {
    setScheduler()

    val mem = 1000
    val cpu = 1
    val s2Attributes = List(Utils.createTextAttribute("c1", "a"))
    val s3Attributes = List(
      Utils.createTextAttribute("c1", "a"),
      Utils.createTextAttribute("c2", "b"))
    val offers = List(
      Utils.createOffer("o1", "s1", mem, cpu, None, 0),
      Utils.createOffer("o2", "s2", mem, cpu, None, 0, s2Attributes),
      Utils.createOffer("o3", "s3", mem, cpu, None, 0, s3Attributes))

    def submitDriver(driverConstraints: String): Unit = {
      val response = scheduler.submitDriver(
        new MesosDriverDescription("d1", "jar", mem, cpu, true,
          command,
          Map("spark.mesos.executor.home" -> "test",
            "spark.app.name" -> "test",
            config.DRIVER_CONSTRAINTS.key -> driverConstraints),
          "s1",
          new Date()))
      assert(response.success)
    }

    submitDriver("c1:x")
    scheduler.resourceOffers(driver, offers.asJava)
    offers.foreach(o => Utils.verifyTaskNotLaunched(driver, o.getId.getValue))

    submitDriver("c1:y;c2:z")
    scheduler.resourceOffers(driver, offers.asJava)
    offers.foreach(o => Utils.verifyTaskNotLaunched(driver, o.getId.getValue))

    submitDriver("")
    scheduler.resourceOffers(driver, offers.asJava)
    Utils.verifyTaskLaunched(driver, "o1")

    submitDriver("c1:a")
    scheduler.resourceOffers(driver, offers.asJava)
    Utils.verifyTaskLaunched(driver, "o2")

    submitDriver("c1:a;c2:b")
    scheduler.resourceOffers(driver, offers.asJava)
    Utils.verifyTaskLaunched(driver, "o3")
  }

  test("supports spark.mesos.driver.labels") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.executor.home" -> "test",
          "spark.app.name" -> "test",
          "spark.mesos.driver.labels" -> "key:value"),
        "s1",
        new Date()))

    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)

    val launchedTasks = Utils.verifyTaskLaunched(driver, "o1")
    val labels = launchedTasks.head.getLabels
    assert(labels.getLabelsCount == 1)
    assert(labels.getLabels(0).getKey == "key")
    assert(labels.getLabels(0).getValue == "value")
  }

  test("can kill supervised drivers") {
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("spark mesos")
    setScheduler(conf.getAll.toMap)

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map(("spark.mesos.executor.home", "test"), ("spark.app.name", "test")), "s1", new Date()))
    assert(response.success)
    val slaveId = SlaveID.newBuilder().setValue("s1").build()
    val offer = Offer.newBuilder()
      .addResources(
        Resource.newBuilder().setRole("*")
          .setScalar(Scalar.newBuilder().setValue(1).build()).setName("cpus").setType(Type.SCALAR))
      .addResources(
        Resource.newBuilder().setRole("*")
          .setScalar(Scalar.newBuilder().setValue(1000).build())
          .setName("mem")
          .setType(Type.SCALAR))
      .setId(OfferID.newBuilder().setValue("o1").build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1").build())
      .setSlaveId(slaveId)
      .setHostname("host1")
      .build()
    // Offer the resource to launch the submitted driver
    scheduler.resourceOffers(driver, Collections.singletonList(offer))
    var state = scheduler.getSchedulerState()
    assert(state.launchedDrivers.size == 1)
    // Issue the request to kill the launched driver
    val killResponse = scheduler.killDriver(response.submissionId)
    assert(killResponse.success)

    val taskStatus = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(response.submissionId).build())
      .setSlaveId(slaveId)
      .setState(MesosTaskState.TASK_KILLED)
      .build()
    // Update the status of the killed task
    scheduler.statusUpdate(driver, taskStatus)
    // Driver should be moved to finishedDrivers for kill
    state = scheduler.getSchedulerState()
    assert(state.pendingRetryDrivers.isEmpty)
    assert(state.launchedDrivers.isEmpty)
    assert(state.finishedDrivers.size == 1)
  }

  test("Declines offer with refuse seconds = 120.") {
    setScheduler()

    val filter = Filters.newBuilder().setRefuseSeconds(120).build()
    val offerId = OfferID.newBuilder().setValue("o1").build()
    val offer = Utils.createOffer(offerId.getValue, "s1", 1000, 1)

    scheduler.resourceOffers(driver, Collections.singletonList(offer))

    verify(driver, times(1)).declineOffer(offerId, filter)
  }

  test("Creates an env-based reference secrets.") {
    val launchedTasks = launchDriverTask(
      Utils.configEnvBasedRefSecrets(config.driverSecretConfig))
    Utils.verifyEnvBasedRefSecrets(launchedTasks)
  }

  test("Creates an env-based value secrets.") {
    val launchedTasks = launchDriverTask(
      Utils.configEnvBasedValueSecrets(config.driverSecretConfig))
    Utils.verifyEnvBasedValueSecrets(launchedTasks)
  }

  test("Creates file-based reference secrets.") {
    val launchedTasks = launchDriverTask(
      Utils.configFileBasedRefSecrets(config.driverSecretConfig))
    Utils.verifyFileBasedRefSecrets(launchedTasks)
  }

  test("Creates a file-based value secrets.") {
    val launchedTasks = launchDriverTask(
      Utils.configFileBasedValueSecrets(config.driverSecretConfig))
    Utils.verifyFileBasedValueSecrets(launchedTasks)
  }

  test("Get driver priority") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.ROUTINE", "1.0")
    conf.set("spark.mesos.dispatcher.queue.URGENT", "2.0")
    conf.set("spark.mesos.dispatcher.queue.EXCEPTIONAL", "3.0")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1

    // Test queue not declared in scheduler
    var desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.dispatcher.queue" -> "dummy"),
        "s1",
        new Date())

    assertThrows[NoSuchElementException] {
      scheduler.getDriverPriority(desc)
    }

    // Test with no specified queue
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map[String, String](),
      "s2",
      new Date())

    assert(scheduler.getDriverPriority(desc) == 0.0f)

    // Test with "default" queue specified
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "default"),
      "s3",
      new Date())

    assert(scheduler.getDriverPriority(desc) == 0.0f)

    // Test queue declared in scheduler
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "ROUTINE"),
      "s4",
      new Date())

    assert(scheduler.getDriverPriority(desc) == 1.0f)

    // Test other queue declared in scheduler
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "URGENT"),
      "s5",
      new Date())

    assert(scheduler.getDriverPriority(desc) == 2.0f)
  }

  test("Can queue drivers with priority") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.ROUTINE", "1.0")
    conf.set("spark.mesos.dispatcher.queue.URGENT", "2.0")
    conf.set("spark.mesos.dispatcher.queue.EXCEPTIONAL", "3.0")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1

    val response0 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "ROUTINE"), "s0", new Date()))
    assert(response0.success)

    val response1 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map[String, String](), "s1", new Date()))
    assert(response1.success)

    val response2 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "EXCEPTIONAL"), "s2", new Date()))
    assert(response2.success)

    val response3 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "URGENT"), "s3", new Date()))
    assert(response3.success)

    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response2.submissionId)
    assert(queuedDrivers(1).submissionId == response3.submissionId)
    assert(queuedDrivers(2).submissionId == response0.submissionId)
    assert(queuedDrivers(3).submissionId == response1.submissionId)
  }

  test("Can queue drivers with negative priority") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.LOWER", "-1.0")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1

    val response0 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "LOWER"), "s0", new Date()))
    assert(response0.success)

    val response1 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map[String, String](), "s1", new Date()))
    assert(response1.success)

    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response1.submissionId)
    assert(queuedDrivers(1).submissionId == response0.submissionId)
  }

  private def launchDriverTask(addlSparkConfVars: Map[String, String]): List[TaskInfo] = {
    setScheduler()
    val mem = 1000
    val cpu = 1
    val driverDesc = new MesosDriverDescription(
      "d1",
      "jar",
      mem,
      cpu,
      true,
      command,
      Map("spark.mesos.executor.home" -> "test",
        "spark.app.name" -> "test") ++
        addlSparkConfVars,
      "s1",
      new Date())
    val response = scheduler.submitDriver(driverDesc)
    assert(response.success)
    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, Collections.singletonList(offer))
    Utils.verifyTaskLaunched(driver, "o1")
  }
}
