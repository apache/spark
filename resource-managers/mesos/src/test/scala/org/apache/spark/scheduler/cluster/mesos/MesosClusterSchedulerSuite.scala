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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.Protos.Value.{Scalar, Type}
import org.apache.mesos.SchedulerDriver
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.mesos.config

class MesosClusterSchedulerSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar {

  private val command = new Command("mainClass", Seq("arg"), Map(), Seq(), Seq(), Seq())
  private var driver: SchedulerDriver = _
  private var scheduler: MesosClusterScheduler = _

  private val submissionTime = new AtomicLong(System.currentTimeMillis())

  // Queued drivers in MesosClusterScheduler are ordered based on MesosDriverDescription
  // The default ordering checks for priority, followed by submission time. For two driver
  // submissions with same priority and if made in quick succession (such that submission
  // time is same due to millisecond granularity), this results in dropping the
  // second MesosDriverDescription from the queuedDrivers - as driverOrdering
  // returns 0 when comparing the descriptions. Ensure two seperate submissions
  // have differnt dates
  private def getDate: Date = {
    new Date(submissionTime.incrementAndGet())
  }

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
      getDate)
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
        Map((config.EXECUTOR_HOME.key, "test"), ("spark.app.name", "test"),
          (config.DRIVER_MEMORY_OVERHEAD.key, "0")),
        "s1",
        getDate))
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
        meq(Collections.singleton(offer.getId)),
        capture.capture())
    ).thenReturn(Status.valueOf(1))

    scheduler.resourceOffers(driver, Collections.singletonList(offer))

    val taskInfos = capture.getValue
    assert(taskInfos.size() == 1)
    val taskInfo = taskInfos.iterator().next()
    val resources = taskInfo.getResourcesList
    assert(scheduler.getResource(resources, "cpus") == 1.5)
    assert(scheduler.getResource(resources, "mem") == 1200)
    val resourcesSeq: Seq[Resource] = resources.asScala.toSeq
    val cpus = resourcesSeq.filter(_.getName == "cpus").toList
    assert(cpus.size == 2)
    assert(cpus.exists(_.getRole() == "role2"))
    assert(cpus.exists(_.getRole() == "*"))
    val mem = resourcesSeq.filter(_.getName == "mem").toList
    assert(mem.size == 2)
    assert(mem.exists(_.getRole() == "role2"))
    assert(mem.exists(_.getRole() == "*"))

    verify(driver, times(1)).launchTasks(
      meq(Collections.singleton(offer.getId)),
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

  test("SPARK-22256: supports spark.mesos.driver.memoryOverhead with 384mb default") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.executor.home" -> "test",
          "spark.app.name" -> "test"),
        "s1",
        getDate))
    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem*2, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)
    val tasks = Utils.verifyTaskLaunched(driver, "o1")
    // 1384.0
    val taskMem = tasks.head.getResourcesList
      .asScala
      .filter(_.getName.equals("mem"))
      .map(_.getScalar.getValue)
      .head
    assert(1384.0 === taskMem)
  }

  test("SPARK-22256: supports spark.mesos.driver.memoryOverhead with 10% default") {
    setScheduler()

    val mem = 10000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map("spark.mesos.executor.home" -> "test",
          "spark.app.name" -> "test"),
        "s1",
        getDate))
    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem*2, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)
    val tasks = Utils.verifyTaskLaunched(driver, "o1")
    // 11000.0
    val taskMem = tasks.head.getResourcesList
      .asScala
      .filter(_.getName.equals("mem"))
      .map(_.getScalar.getValue)
      .head
    assert(11000.0 === taskMem)
  }

  test("supports spark.mesos.driverEnv.*") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map(config.EXECUTOR_HOME.key -> "test",
          "spark.app.name" -> "test",
          config.DRIVER_ENV_PREFIX + "TEST_ENV" -> "TEST_VAL",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"
        ),
        "s1",
        getDate))
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
        Map(config.EXECUTOR_HOME.key -> "test",
          "spark.app.name" -> "test",
          config.NETWORK_NAME.key -> "test-network-name",
          config.NETWORK_LABELS.key -> "key1:val1,key2:val2",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
        "s1",
        getDate))

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

  test("supports setting fetcher cache") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map(config.EXECUTOR_HOME.key -> "test",
          config.ENABLE_FETCHER_CACHE.key -> "true",
          "spark.app.name" -> "test",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
        "s1",
        getDate))

    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)

    val launchedTasks = Utils.verifyTaskLaunched(driver, "o1")
    val uris = launchedTasks.head.getCommand.getUrisList
    assert(uris.asScala.forall(_.getCache))
  }

  test("supports setting fetcher cache on the dispatcher") {
    setScheduler(Map(config.ENABLE_FETCHER_CACHE.key -> "true"))

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map(config.EXECUTOR_HOME.key -> "test",
          "spark.app.name" -> "test",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
        "s1",
        getDate))

    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)

    val launchedTasks = Utils.verifyTaskLaunched(driver, "o1")
    val uris = launchedTasks.head.getCommand.getUrisList
    assert(uris.asScala.forall(_.getCache))
  }

  test("supports disabling fetcher cache") {
    setScheduler()

    val mem = 1000
    val cpu = 1

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", mem, cpu, true,
        command,
        Map(config.EXECUTOR_HOME.key -> "test",
          config.ENABLE_FETCHER_CACHE.key -> "false",
          "spark.app.name" -> "test",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
        "s1",
        getDate))

    assert(response.success)

    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, List(offer).asJava)

    val launchedTasks = Utils.verifyTaskLaunched(driver, "o1")
    val uris = launchedTasks.head.getCommand.getUrisList
    assert(uris.asScala.forall(!_.getCache))
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
          Map(config.EXECUTOR_HOME.key -> "test",
            "spark.app.name" -> "test",
            config.DRIVER_CONSTRAINTS.key -> driverConstraints,
            config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
          "s1",
          getDate))
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
        Map(config.EXECUTOR_HOME.key -> "test",
          "spark.app.name" -> "test",
          config.DRIVER_LABELS.key -> "key:value",
          config.DRIVER_MEMORY_OVERHEAD.key -> "0"),
        "s1",
        getDate))

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
        Map((config.EXECUTOR_HOME.key, "test"), ("spark.app.name", "test")), "s1", getDate))
    assert(response.success)
    val agentId = SlaveID.newBuilder().setValue("s1").build()
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
      .setSlaveId(agentId)
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
      .setSlaveId(agentId)
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

  test("SPARK-27347: do not restart outdated supervised drivers") {
    // Covers scenario where:
    // - agent goes down
    // - supervised job is relaunched on another agent
    // - first agent re-registers and sends status update: TASK_FAILED
    // - job should NOT be relaunched again
    val conf = new SparkConf()
    conf.setMaster("mesos://localhost:5050")
    conf.setAppName("SparkMesosDriverRetries")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1
    val offers = List(
      Utils.createOffer("o1", "s1", mem, cpu, None),
      Utils.createOffer("o2", "s2", mem, cpu, None),
      Utils.createOffer("o3", "s1", mem, cpu, None))

    val response = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map(("spark.mesos.executor.home", "test"), ("spark.app.name", "test")), "sub1", getDate))
    assert(response.success)

    // Offer a resource to launch the submitted driver
    scheduler.resourceOffers(driver, Collections.singletonList(offers.head))
    var state = scheduler.getSchedulerState()
    assert(state.launchedDrivers.size == 1)

    // Signal agent lost with status with TASK_LOST
    val agent1 = SlaveID.newBuilder().setValue("s1").build()
    var taskStatus = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(response.submissionId).build())
      .setSlaveId(agent1)
      .setReason(TaskStatus.Reason.REASON_SLAVE_REMOVED)
      .setState(MesosTaskState.TASK_LOST)
      .build()

    scheduler.statusUpdate(driver, taskStatus)
    state = scheduler.getSchedulerState()
    assert(state.pendingRetryDrivers.size == 1)
    assert(state.pendingRetryDrivers.head.submissionId == taskStatus.getTaskId.getValue)
    assert(state.launchedDrivers.isEmpty)

    // Offer new resource to retry driver on a new agent
    Thread.sleep(1500) // sleep to cover nextRetry's default wait time of 1s
    scheduler.resourceOffers(driver, Collections.singletonList(offers(1)))

    val agent2 = SlaveID.newBuilder().setValue("s2").build()
    taskStatus = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(response.submissionId).build())
      .setSlaveId(agent2)
      .setState(MesosTaskState.TASK_RUNNING)
      .build()

    scheduler.statusUpdate(driver, taskStatus)
    state = scheduler.getSchedulerState()
    assert(state.pendingRetryDrivers.isEmpty)
    assert(state.launchedDrivers.size == 1)
    assert(state.launchedDrivers.head.taskId.getValue.endsWith("-retry-1"))
    assert(state.launchedDrivers.head.taskId.getValue != taskStatus.getTaskId.getValue)

    // Agent1 comes back online and sends status update: TASK_FAILED
    taskStatus = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(response.submissionId).build())
      .setSlaveId(agent1)
      .setState(MesosTaskState.TASK_FAILED)
      .setMessage("Abnormal executor termination")
      .setReason(TaskStatus.Reason.REASON_EXECUTOR_TERMINATED)
      .build()

    scheduler.statusUpdate(driver, taskStatus)
    scheduler.resourceOffers(driver, Collections.singletonList(offers.last))

    // Assert driver does not restart 2nd time
    state = scheduler.getSchedulerState()
    assert(state.pendingRetryDrivers.isEmpty)
    assert(state.launchedDrivers.size == 1)
    assert(state.launchedDrivers.head.taskId.getValue.endsWith("-retry-1"))
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

  test("assembles a valid driver command, escaping all confs and args") {
    setScheduler()

    val mem = 1000
    val cpu = 1
    val driverDesc = new MesosDriverDescription(
      "d1",
      "jar",
      mem,
      cpu,
      true,
      new Command(
        "Main",
        Seq("--a=$2", "--b", "x y z"),
        Map(),
        Seq(),
        Seq(),
        Seq()),
      Map("spark.app.name" -> "app name",
        config.EXECUTOR_URI.key -> "s3a://bucket/spark-version.tgz",
        "another.conf" -> "\\value"),
      "s1",
      getDate)

    val expectedCmd = "cd spark-version*;  " +
        "bin/spark-submit --name \"app name\" --master mesos://mesos://localhost:5050 " +
        "--driver-cores 1.0 --driver-memory 1000M --class Main " +
        "--conf \"another.conf=\\\\value\" " +
        "--conf \"spark.app.name=app name\" " +
        "--conf spark.executor.uri=s3a://bucket/spark-version.tgz " +
        "../jar " +
        "\"--a=\\$2\" " +
        "--b \"x y z\""

    assert(scheduler.getDriverCommandValue(driverDesc) == expectedCmd)
  }

  test("SPARK-23499: Test dispatcher priority queue with non float value") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.ROUTINE", "1.0")
    conf.set("spark.mesos.dispatcher.queue.URGENT", "abc")
    conf.set("spark.mesos.dispatcher.queue.EXCEPTIONAL", "3.0")
    assertThrows[NumberFormatException] {
      setScheduler(conf.getAll.toMap)
    }
  }

  test("SPARK-23499: Get driver priority") {
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
      getDate)

    assertThrows[NoSuchElementException] {
      scheduler.getDriverPriority(desc)
    }

    // Test with no specified queue
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map[String, String](),
      "s2",
      getDate)

    assert(scheduler.getDriverPriority(desc) == 0.0f)

    // Test with "default" queue specified
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "default"),
      "s3",
      getDate)

    assert(scheduler.getDriverPriority(desc) == 0.0f)

    // Test queue declared in scheduler
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "ROUTINE"),
      "s4",
      getDate)

    assert(scheduler.getDriverPriority(desc) == 1.0f)

    // Test other queue declared in scheduler
    desc = new MesosDriverDescription("d1", "jar", mem, cpu, true,
      command,
      Map("spark.mesos.dispatcher.queue" -> "URGENT"),
      "s5",
      getDate)

    assert(scheduler.getDriverPriority(desc) == 2.0f)
  }

  test("SPARK-23499: Can queue drivers with priority") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.ROUTINE", "1.0")
    conf.set("spark.mesos.dispatcher.queue.URGENT", "2.0")
    conf.set("spark.mesos.dispatcher.queue.EXCEPTIONAL", "3.0")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1

    val response0 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "ROUTINE"), "s0", getDate))
    assert(response0.success)

    val response1 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map[String, String](), "s1", getDate))
    assert(response1.success)

    val response2 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "EXCEPTIONAL"), "s2", getDate))
    assert(response2.success)

    val response3 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "URGENT"), "s3", getDate))
    assert(response3.success)

    val state = scheduler.getSchedulerState()
    val queuedDrivers = state.queuedDrivers.toList
    assert(queuedDrivers(0).submissionId == response2.submissionId)
    assert(queuedDrivers(1).submissionId == response3.submissionId)
    assert(queuedDrivers(2).submissionId == response0.submissionId)
    assert(queuedDrivers(3).submissionId == response1.submissionId)
  }

  test("SPARK-23499: Can queue drivers with negative priority") {
    val conf = new SparkConf()
    conf.set("spark.mesos.dispatcher.queue.LOWER", "-1.0")
    setScheduler(conf.getAll.toMap)

    val mem = 1000
    val cpu = 1

    val response0 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map("spark.mesos.dispatcher.queue" -> "LOWER"), "s0", getDate))
    assert(response0.success)

    val response1 = scheduler.submitDriver(
      new MesosDriverDescription("d1", "jar", 100, 1, true, command,
        Map[String, String](), "s1", getDate))
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
      Map(config.EXECUTOR_HOME.key -> "test",
        "spark.app.name" -> "test",
        config.DRIVER_MEMORY_OVERHEAD.key -> "0") ++
        addlSparkConfVars,
      "s1",
      getDate)
    val response = scheduler.submitDriver(driverDesc)
    assert(response.success)
    val offer = Utils.createOffer("o1", "s1", mem, cpu)
    scheduler.resourceOffers(driver, Collections.singletonList(offer))
    Utils.verifyTaskLaunched(driver, "o1")
  }
}
