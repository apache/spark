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

package org.apache.spark.deploy.mesos

import akka.actor.{Props, ActorSystem, Actor}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.Some

import org.apache.spark.deploy.rest.MesosRestServer
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, IntParam, Utils}

import org.apache.spark.deploy.DeployMessages._

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.deploy.master.DriverInfo
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.worker.DriverRunner

import java.io.{IOException, File}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.deploy.mesos.ui.MesosClusterUI
import org.apache.spark.deploy.mesos.Messages.{DispatcherStateResponse, RequestDispatcherState}

private [deploy] object Messages {
  case object RequestDispatcherState

  case class DispatcherStateResponse(
      activeDrivers: Iterable[DriverInfo],
      completedDrivers: Iterable[DriverInfo]) {
  }
}

/*
 * A dispatcher actor that is responsible for managing drivers, that is intended to
 * used for Mesos cluster mode.
 * This class is needed since Mesos doesn't manage frameworks, so the dispatcher acts as
 * a daemon to launch drivers as Mesos frameworks upon request.
 */
private [spark] class MesosClusterDispatcher(
    host: String,
    serverPort: Int,
    actorPort: Int,
    webUiPort: Int,
    systemName: String,
    actorName: String,
    conf: SparkConf,
    masterUrl: String,
    workDirPath: Option[String] = None) extends Actor with ActorLogReceive with Logging {
  val server = new MesosRestServer(host, serverPort, self, conf, masterUrl)

  val dispatcherPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  lazy val workDir: File = {
    val dir = workDirPath.map(new File(_)).getOrElse(new File(sparkHome, "work"))

    // Attempt to remove the work directory if it exists on startup.
    // This is to avoid unbounded growing the work directory as drivers
    // are only deleted when it is over the retained count while it's running.
    // We don't fail startup if we are not able to remove, as this is
    // a short-term solution.
    try {
      if (dir.exists()) {
        dir.delete()
      }
    } catch {
      case e: IOException =>
        logError("Unable to remove work directory " + workDir, e)
    }

    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      dir.mkdirs()
      if (!dir.exists() || !dir.isDirectory) {
        logError("Failed to create work directory " + dir)
        System.exit(1)
      }
      assert (dir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + dir, e)
        System.exit(1)
    }
    dir
  }

  val webUi = new MesosClusterUI(
    self, new SecurityManager(conf), webUiPort, conf, workDir, dispatcherPublicAddress)

  val runners = new HashMap[String, DriverRunner]
  val drivers = new HashMap[String, DriverInfo]
  val completedDrivers = new ArrayBuffer[DriverInfo]
  val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  var nextDriverNumber = 0

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  val sparkHome =
    new File(sys.env.get("SPARK_HOME").getOrElse("."))

  val akkaUrl = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    systemName,
    host,
    actorPort,
    actorName)

  def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  override def preStart() {
    server.start()
    webUi.bind()
  }

  override def postStop() {
    webUi.stop()
    server.stop()
    runners.values.foreach(_.kill())
  }

  override def receiveWithLogging = {
    case RequestSubmitDriver(driverDescription) => {
      val driverInfo = createDriver(driverDescription)
      val runner = new DriverRunner(conf, driverInfo.id, workDir,
        sparkHome, driverDescription, self, akkaUrl)
      runners(driverInfo.id) = runner
      drivers(driverInfo.id) = driverInfo
      runner.start()
      sender ! SubmitDriverResponse(true, Option(driverInfo.id), "")
    }

    case RequestKillDriver(driverId) => {
      if (!drivers.contains(driverId)) {
        if (completedDrivers.exists(_.id == driverId)) {
          sender ! KillDriverResponse(driverId, false, "Driver already completed")
        } else {
          sender ! KillDriverResponse(driverId, false, "Unknown driver")
        }
      } else {
        runners(driverId).kill()
        sender ! KillDriverResponse(driverId, true, "")
      }
    }

    case RequestDriverStatus(driverId) => {
      drivers.get(driverId).orElse(completedDrivers.find(_.id == driverId)) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            None, None, driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }
    }

    case DriverStateChanged(driverId, state, exception) => {
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case RequestDispatcherState => {
      sender ! DispatcherStateResponse(drivers.values, completedDrivers)
    }
  }

  def removeDriver(driverId: String, state: DriverState, exception: Option[Exception]) {
    if (completedDrivers.size >= RETAINED_DRIVERS) {
      val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
      for (i <- 0 to (toRemove - 1)) {
        val driverId = completedDrivers(i).id
        try {
          new File(workDir, driverId).delete()
        } catch {
          case e: Exception =>
            logWarning("Unable to remove work dir for completed driver " + driverId, e)
        }
      }
      completedDrivers.trimStart(toRemove)
    }
    val driverInfo = drivers.remove(driverId).get
    driverInfo.exception = exception
    driverInfo.state = state
    completedDrivers += driverInfo
  }
}

object MesosClusterDispatcher {
  def main(args: Array[String]) {
    val conf = new SparkConf
    val clusterArgs = new ClusterDispatcherArguments(args, conf)
    val actorSystem = startSystemAndActor(clusterArgs)
    Runtime.getRuntime().addShutdownHook(new Thread("MesosClusterDispatcherShutdownHook") {
      override def run() = {
        // Makes sure we shut down the actor, which will kill all the drivers.
        actorSystem.shutdown()
        actorSystem.awaitTermination()
      }
    })
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(
      args: ClusterDispatcherArguments): ActorSystem = {
    // The LocalSparkCluster runs multiple local sparkWorkerX actor systems
    val conf = new SparkConf
    val systemName = "spark-mesos-cluster"
    val actorName = "MesosClusterDispatcher"
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      systemName, args.host, 0, conf, securityMgr)
    actorSystem.actorOf(
      Props(classOf[MesosClusterDispatcher],
        args.host,
        args.port,
        boundPort,
        args.webUiPort,
        systemName,
        actorName,
        conf,
        args.masterUrl,
        None),
      name = actorName)
    actorSystem
  }

  class ClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
    var host = Utils.localHostName()
    var port = 7077
    var webUiPort = 8081
    var masterUrl: String = null

    parse(args.toList)

    def parse(args: List[String]): Unit = args match {
      case ("--host" | "-h") :: value :: tail =>
        Utils.checkHost(value, "Please use hostname " + value)
        host = value
        parse(tail)

      case ("--port" | "-p") :: IntParam(value) :: tail =>
        port = value
        parse(tail)

      case ("--webui-port" | "-p") :: IntParam(value) :: tail =>
        webUiPort = value
        parse(tail)

      case ("--master" | "-m") :: value :: tail =>
        if (!value.startsWith("mesos://")) {
          System.err.println("Cluster dispatcher only supports mesos (uri begins with mesos://)")
          System.exit(1)
        }
        masterUrl = value
        parse(tail)

      case ("--help") :: tail =>
        printUsageAndExit(0)

      case Nil => {
        if (masterUrl == null) {
          System.err.println("--master is required")
          System.exit(1)
        }
      }

      case _ =>
        printUsageAndExit(1)
    }

    /**
     * Print usage and exit JVM with the given exit code.
     */
    def printUsageAndExit(exitCode: Int) {
      System.err.println(
        "Usage: MesosClusterDispatcher [options]\n" +
          "\n" +
          "Options:\n" +
          "  -h HOST, --host HOST   Hostname to listen on\n" +
          "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
          "  --webui-port WEBUI_PORT   WebUI Port to listen on (default: 8081)\n" +
          "  -m --master MASTER      URI for connecting to Mesos master\n")
      System.exit(exitCode)
    }
  }
}
