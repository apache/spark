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

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import org.apache.log4j.{Level, Logger}

import org.apache.spark.rpc.{RpcEndpointRef, RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.util.{SparkExitCode, Utils}

/**
 * Proxy that relays messages to the driver.
 */
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoint: RpcEndpointRef,
    conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  private val forwardMessageThread =
    Utils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
  private implicit val forwardMessageExecutionContext =
    ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // Exit normally
        case e =>
          logError(e.getMessage, e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })

  override def onStart(): Unit = {
    println(s"Sending ${driverArgs.cmd} command to ${driverArgs.master}")

    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = "spark.driver.extraClassPath"
        val classPathEntries = sys.props.get(classPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = "spark.driver.extraLibraryPath"
        val libraryPathEntries = sys.props.get(libraryPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = "spark.driver.extraJavaOptions"
        val extraJavaOpts = sys.props.get(extraJavaOptsConf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)
        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)

        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command)
        ayncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        val driverId = driverArgs.driverId
        ayncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
  }

  /**
   * Send the message to master and forward the reply to self asynchronously.
   */
  private def ayncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit = {
    masterEndpoint.sendWithReply[T](message).onComplete {
      case Success(v) => self.send(v)
      case Failure(e) =>
        println(s"Error sending messages to master ${driverArgs.master}, exiting.")
        logError(e.getMessage, e)
        System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
    }
  }

  /* Find out driver status then exit the JVM */
  def pollAndReportStatus(driverId: String) {
    // Since ClientEndpoint is the only RpcEndpoint in the process, blocking the event loop thread
    // is fine.
    println("... waiting before polling master for driver state")
    Thread.sleep(5000)
    println("... polling master for driver state")
    val statusResponse =
      masterEndpoint.askWithReply[DriverStatusResponse](RequestDriverStatus(driverId))

    statusResponse.found match {
      case false =>
        println(s"ERROR: Cluster master did not recognize $driverId")
        System.exit(-1)
      case true =>
        println(s"State of $driverId is ${statusResponse.state.get}")
        // Worker node, if present
        (statusResponse.workerId, statusResponse.workerHostPort, statusResponse.state) match {
          case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
            println(s"Driver running on $hostPort ($id)")
          case _ =>
        }
        // Exception, if present
        statusResponse.exception.map { e =>
          println(s"Exception from cluster was: $e")
          e.printStackTrace()
          System.exit(-1)
        }
        System.exit(0)
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    case SubmitDriverResponse(success, driverId, message) =>
      println(message)
      if (success) pollAndReportStatus(driverId.get) else System.exit(-1)

    case KillDriverResponse(driverId, success, message) =>
      println(message)
      if (success) pollAndReportStatus(driverId) else System.exit(-1)

  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    println(s"Error connecting to master ${driverArgs.master} ($remoteAddress), exiting.")
    System.exit(-1)
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    println(s"Error connecting to master ${driverArgs.master} ($remoteAddress), exiting.")
    cause.printStackTrace()
    System.exit(-1)
  }

  override def onError(cause: Throwable): Unit = {
    println(s"Error processing messages, exiting.")
    cause.printStackTrace()
    System.exit(-1)
  }

  override def onStop(): Unit = {
    forwardMessageThread.shutdownNow()
  }
}

/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object Client {
  def main(args: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }

    val conf = new SparkConf()
    val driverArgs = new ClientArguments(args)

    if (!driverArgs.logLevel.isGreaterOrEqual(Level.WARN)) {
      conf.set("spark.akka.logLifecycleEvents", "true")
    }
    conf.set("spark.akka.askTimeout", "10")
    conf.set("akka.loglevel", driverArgs.logLevel.toString.replace("WARN", "WARNING"))
    Logger.getRootLogger.setLevel(driverArgs.logLevel)

    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))

    val masterAddress = RpcAddress.fromSparkURL(driverArgs.master)
    val masterEndpoint =
      rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoint, conf))

    rpcEnv.awaitTermination()
  }
}
