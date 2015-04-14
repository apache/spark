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

package org.apache.spark.deploy

import java.io._
import java.net.URL
import java.util.concurrent.TimeoutException

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, future, promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._

import org.json4s._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.deploy.master.{RecoveryState, SparkCuratorUtil}
import org.apache.spark.util.Utils

/**
 * This suite tests the fault tolerance of the Spark standalone scheduler, mainly the Master.
 * In order to mimic a real distributed cluster more closely, Docker is used.
 * Execute using
 * ./bin/spark-class org.apache.spark.deploy.FaultToleranceTest
 *
 * Make sure that that the environment includes the following properties in SPARK_DAEMON_JAVA_OPTS
 * *and* SPARK_JAVA_OPTS:
 *   - spark.deploy.recoveryMode=ZOOKEEPER
 *   - spark.deploy.zookeeper.url=172.17.42.1:2181
 * Note that 172.17.42.1 is the default docker ip for the host and 2181 is the default ZK port.
 *
 * In case of failure, make sure to kill off prior docker containers before restarting:
 *   docker kill $(docker ps -q)
 *
 * Unfortunately, due to the Docker dependency this suite cannot be run automatically without a
 * working installation of Docker. In addition to having Docker, the following are assumed:
 *   - Docker can run without sudo (see http://docs.docker.io/en/latest/use/basics/)
 *   - The docker images tagged spark-test-master and spark-test-worker are built from the
 *     docker/ directory. Run 'docker/spark-test/build' to generate these.
 */
private object FaultToleranceTest extends App with Logging {

  private val conf = new SparkConf()
  private val ZK_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark")

  private val masters = ListBuffer[TestMasterInfo]()
  private val workers = ListBuffer[TestWorkerInfo]()
  private var sc: SparkContext = _

  private val zk =  SparkCuratorUtil.newClient(conf)

  private var numPassed = 0
  private var numFailed = 0

  private val sparkHome = System.getenv("SPARK_HOME")
  assertTrue(sparkHome != null, "Run with a valid SPARK_HOME")

  private val containerSparkHome = "/opt/spark"
  private val dockerMountDir = "%s:%s".format(sparkHome, containerSparkHome)

  System.setProperty("spark.driver.host", "172.17.42.1") // default docker host ip

  private def afterEach() {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    terminateCluster()

    // Clear ZK directories in between tests (for speed purposes)
    SparkCuratorUtil.deleteRecursive(zk, ZK_DIR + "/spark_leader")
    SparkCuratorUtil.deleteRecursive(zk, ZK_DIR + "/master_status")
  }

  test("sanity-basic") {
    addMasters(1)
    addWorkers(1)
    createClient()
    assertValidClusterState()
  }

  test("sanity-many-masters") {
    addMasters(3)
    addWorkers(3)
    createClient()
    assertValidClusterState()
  }

  test("single-master-halt") {
    addMasters(3)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    killLeader()
    delay(30 seconds)
    assertValidClusterState()
    createClient()
    assertValidClusterState()
  }

  test("single-master-restart") {
    addMasters(1)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    killLeader()
    addMasters(1)
    delay(30 seconds)
    assertValidClusterState()

    killLeader()
    addMasters(1)
    delay(30 seconds)
    assertValidClusterState()
  }

  test("cluster-failure") {
    addMasters(2)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    terminateCluster()
    addMasters(2)
    addWorkers(2)
    assertValidClusterState()
  }

  test("all-but-standby-failure") {
    addMasters(2)
    addWorkers(2)
    createClient()
    assertValidClusterState()

    killLeader()
    workers.foreach(_.kill())
    workers.clear()
    delay(30 seconds)
    addWorkers(2)
    assertValidClusterState()
  }

  test("rolling-outage") {
    addMasters(1)
    delay()
    addMasters(1)
    delay()
    addMasters(1)
    addWorkers(2)
    createClient()
    assertValidClusterState()
    assertTrue(getLeader == masters.head)

    (1 to 3).foreach { _ =>
      killLeader()
      delay(30 seconds)
      assertValidClusterState()
      assertTrue(getLeader == masters.head)
      addMasters(1)
    }
  }

  private def test(name: String)(fn: => Unit) {
    try {
      fn
      numPassed += 1
      logInfo("==============================================")
      logInfo("Passed: " + name)
      logInfo("==============================================")
    } catch {
      case e: Exception =>
        numFailed += 1
        logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logError("FAILED: " + name, e)
        logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        sys.exit(1)
    }
    afterEach()
  }

  private def addMasters(num: Int) {
    logInfo(s">>>>> ADD MASTERS $num <<<<<")
    (1 to num).foreach { _ => masters += SparkDocker.startMaster(dockerMountDir) }
  }

  private def addWorkers(num: Int) {
    logInfo(s">>>>> ADD WORKERS $num <<<<<")
    val masterUrls = getMasterUrls(masters)
    (1 to num).foreach { _ => workers += SparkDocker.startWorker(dockerMountDir, masterUrls) }
  }

  /** Creates a SparkContext, which constructs a Client to interact with our cluster. */
  private def createClient() = {
    logInfo(">>>>> CREATE CLIENT <<<<<")
    if (sc != null) { sc.stop() }
    // Counter-hack: Because of a hack in SparkEnv#create() that changes this
    // property, we need to reset it.
    System.setProperty("spark.driver.port", "0")
    sc = new SparkContext(getMasterUrls(masters), "fault-tolerance", containerSparkHome)
  }

  private def getMasterUrls(masters: Seq[TestMasterInfo]): String = {
    "spark://" + masters.map(master => master.ip + ":7077").mkString(",")
  }

  private def getLeader: TestMasterInfo = {
    val leaders = masters.filter(_.state == RecoveryState.ALIVE)
    assertTrue(leaders.size == 1)
    leaders(0)
  }

  private def killLeader(): Unit = {
    logInfo(">>>>> KILL LEADER <<<<<")
    masters.foreach(_.readState())
    val leader = getLeader
    masters -= leader
    leader.kill()
  }

  private def delay(secs: Duration = 5.seconds) = Thread.sleep(secs.toMillis)

  private def terminateCluster() {
    logInfo(">>>>> TERMINATE CLUSTER <<<<<")
    masters.foreach(_.kill())
    workers.foreach(_.kill())
    masters.clear()
    workers.clear()
  }

  /** This includes Client retry logic, so it may take a while if the cluster is recovering. */
  private def assertUsable() = {
    val f = future {
      try {
        val res = sc.parallelize(0 until 10).collect()
        assertTrue(res.toList == (0 until 10))
        true
      } catch {
        case e: Exception =>
          logError("assertUsable() had exception", e)
          e.printStackTrace()
          false
      }
    }

    // Avoid waiting indefinitely (e.g., we could register but get no executors).
    assertTrue(Await.result(f, 120 seconds))
  }

  /**
   * Asserts that the cluster is usable and that the expected masters and workers
   * are all alive in a proper configuration (e.g., only one leader).
   */
  private def assertValidClusterState() = {
    logInfo(">>>>> ASSERT VALID CLUSTER STATE <<<<<")
    assertUsable()
    var numAlive = 0
    var numStandby = 0
    var numLiveApps = 0
    var liveWorkerIPs: Seq[String] = List()

    def stateValid(): Boolean = {
      (workers.map(_.ip) -- liveWorkerIPs).isEmpty &&
        numAlive == 1 && numStandby == masters.size - 1 && numLiveApps >= 1
    }

    val f = future {
      try {
        while (!stateValid()) {
          Thread.sleep(1000)

          numAlive = 0
          numStandby = 0
          numLiveApps = 0

          masters.foreach(_.readState())

          for (master <- masters) {
            master.state match {
              case RecoveryState.ALIVE =>
                numAlive += 1
                liveWorkerIPs = master.liveWorkerIPs
              case RecoveryState.STANDBY =>
                numStandby += 1
              case _ => // ignore
            }

            numLiveApps += master.numLiveApps
          }
        }
        true
      } catch {
        case e: Exception =>
          logError("assertValidClusterState() had exception", e)
          false
      }
    }

    try {
      assertTrue(Await.result(f, 120 seconds))
    } catch {
      case e: TimeoutException =>
        logError("Master states: " + masters.map(_.state))
        logError("Num apps: " + numLiveApps)
        logError("IPs expected: " + workers.map(_.ip) + " / found: " + liveWorkerIPs)
        throw new RuntimeException("Failed to get into acceptable cluster state after 2 min.", e)
    }
  }

  private def assertTrue(bool: Boolean, message: String = "") {
    if (!bool) {
      throw new IllegalStateException("Assertion failed: " + message)
    }
  }

  logInfo("Ran %s tests, %s passed and %s failed".format(numPassed + numFailed, numPassed,
    numFailed))
}

private class TestMasterInfo(val ip: String, val dockerId: DockerId, val logFile: File)
  extends Logging  {

  implicit val formats = org.json4s.DefaultFormats
  var state: RecoveryState.Value = _
  var liveWorkerIPs: List[String] = _
  var numLiveApps = 0

  logDebug("Created master: " + this)

  def readState() {
    try {
      val masterStream = new InputStreamReader(new URL("http://%s:8080/json".format(ip)).openStream)
      val json = JsonMethods.parse(masterStream)

      val workers = json \ "workers"
      val liveWorkers = workers.children.filter(w => (w \ "state").extract[String] == "ALIVE")
      // Extract the worker IP from "webuiaddress" (rather than "host") because the host name
      // on containers is a weird hash instead of the actual IP address.
      liveWorkerIPs = liveWorkers.map {
        w => (w \ "webuiaddress").extract[String].stripPrefix("http://").stripSuffix(":8081")
      }

      numLiveApps = (json \ "activeapps").children.size

      val status = json \\ "status"
      val stateString = status.extract[String]
      state = RecoveryState.values.filter(state => state.toString == stateString).head
    } catch {
      case e: Exception =>
        // ignore, no state update
        logWarning("Exception", e)
    }
  }

  def kill() { Docker.kill(dockerId) }

  override def toString: String =
    "[ip=%s, id=%s, logFile=%s, state=%s]".
      format(ip, dockerId.id, logFile.getAbsolutePath, state)
}

private class TestWorkerInfo(val ip: String, val dockerId: DockerId, val logFile: File)
  extends Logging {

  implicit val formats = org.json4s.DefaultFormats

  logDebug("Created worker: " + this)

  def kill() { Docker.kill(dockerId) }

  override def toString: String =
    "[ip=%s, id=%s, logFile=%s]".format(ip, dockerId, logFile.getAbsolutePath)
}

private object SparkDocker {
  def startMaster(mountDir: String): TestMasterInfo = {
    val cmd = Docker.makeRunCmd("spark-test-master", mountDir = mountDir)
    val (ip, id, outFile) = startNode(cmd)
    new TestMasterInfo(ip, id, outFile)
  }

  def startWorker(mountDir: String, masters: String): TestWorkerInfo = {
    val cmd = Docker.makeRunCmd("spark-test-worker", args = masters, mountDir = mountDir)
    val (ip, id, outFile) = startNode(cmd)
    new TestWorkerInfo(ip, id, outFile)
  }

  private def startNode(dockerCmd: ProcessBuilder) : (String, DockerId, File) = {
    val ipPromise = promise[String]()
    val outFile = File.createTempFile("fault-tolerance-test", "", Utils.createTempDir())
    val outStream: FileWriter = new FileWriter(outFile)
    def findIpAndLog(line: String): Unit = {
      if (line.startsWith("CONTAINER_IP=")) {
        val ip = line.split("=")(1)
        ipPromise.success(ip)
      }

      outStream.write(line + "\n")
      outStream.flush()
    }

    dockerCmd.run(ProcessLogger(findIpAndLog _))
    val ip = Await.result(ipPromise.future, 30 seconds)
    val dockerId = Docker.getLastProcessId
    (ip, dockerId, outFile)
  }
}

private class DockerId(val id: String) {
  override def toString: String = id
}

private object Docker extends Logging {
  def makeRunCmd(imageTag: String, args: String = "", mountDir: String = ""): ProcessBuilder = {
    val mountCmd = if (mountDir != "") { " -v " + mountDir } else ""

    val cmd = "docker run -privileged %s %s %s".format(mountCmd, imageTag, args)
    logDebug("Run command: " + cmd)
    cmd
  }

  def kill(dockerId: DockerId) : Unit = {
    "docker kill %s".format(dockerId.id).!
  }

  def getLastProcessId: DockerId = {
    var id: String = null
    "docker ps -l -q".!(ProcessLogger(line => id = line))
    new DockerId(id)
  }
}
