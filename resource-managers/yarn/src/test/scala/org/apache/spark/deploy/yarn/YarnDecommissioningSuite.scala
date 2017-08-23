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

package org.apache.spark.deploy.yarn

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JFiles, Paths}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Try

import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.exceptions.TestFailedDueToTimeoutException

import org.apache.spark.{HostState, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.tags.ExtendedYarnTest
import org.apache.spark.util.ThreadUtils

/**
 * Integration test for YARN's graceful decommission mechanism; these tests use a mini
 * Yarn cluster to run Spark-on-YARN applications, and require the Spark assembly to be built
 * before they can be successfully run.
 * Tests trigger the decommission of the only node in the mini Yarn cluster, and then check
 * in the Yarn container logs that the Yarn node transitions were received at the driver.
 */
@ExtendedYarnTest
class YarnDecommissioningSuite extends BaseYarnClusterSuite with BeforeAndAfter {

  private val (excludedHostsFile, syncFile) = {
    val (excludedHostsFile, syncFile) = (File.createTempFile("yarn-excludes", null, tempDir),
                                         File.createTempFile("syncFile", null, tempDir))
    excludedHostsFile.deleteOnExit()
    syncFile.deleteOnExit()
    logInfo(s"Using YARN excludes file ${excludedHostsFile.getAbsolutePath}")
    logInfo(s"Using sync file ${syncFile.getAbsolutePath}")
    (excludedHostsFile, syncFile)
  }
  // used to avoid restarting the MiniYARNCluster on the first test run
  private var fistTestRun = true
  private val executorService = Executors.newSingleThreadScheduledExecutor()
  private implicit val ec = ExecutionContext.fromExecutorService(executorService)

  override val newYarnConfig: YarnConfiguration = {
    val conf = new YarnConfiguration()
    conf.set("yarn.resourcemanager.nodes.exclude-path", excludedHostsFile.getAbsolutePath)
    conf
  }

  private val decommissionStates = Set(HostState.Decommissioning,
                                       HostState.Decommissioned).map{ state =>
    val yarnStateOpt = HostState.toYarnState(state)
    assert(yarnStateOpt.isDefined,
           s"Spark host state $state should have a translation to YARN state")
    yarnStateOpt.get
  }

  before {
    if (!fistTestRun) {
      Files.write("", excludedHostsFile, StandardCharsets.UTF_8)
      Files.write("", syncFile, StandardCharsets.UTF_8)
      restartCluster()
    }
    fistTestRun = false
  }

  test("Spark application master gets notified on node decommissioning when running in" +
    " cluster mode") {
    val excludedHostStateUpdates = testNodeDecommission(clientMode = false)
    excludedHostStateUpdates shouldEqual(decommissionStates)
  }

  test("Spark application master gets notified on node decommissioning when running in" +
    " client mode") {
    val excludedHostStateUpdates = testNodeDecommission(clientMode = true)
    // In client mode the node doesn't always have time to reach the decommissioned state
    assert(excludedHostStateUpdates.subsetOf(decommissionStates))
    assert(excludedHostStateUpdates
             .contains(HostState.toYarnState(HostState.Decommissioning).get))
  }

  /**
   * @return a set of strings for the Yarn decommission related states the only node in
   *         the MiniYARNCluster has transitioned to after the Spark job has started.
   */
  private def testNodeDecommission(clientMode: Boolean): Set[String] = {
    val excludedHostPromise = Promise[String]
    scheduleDecommissionRunnable(excludedHostPromise)

    // surface exceptions in the executor service
    val excludedHostFuture = excludedHostPromise.future
    excludedHostFuture.onFailure { case t => throw t  }
    // we expect a timeout exception because the job will fail when the only available node
    // is decommissioned after its timeout
    intercept[TestFailedDueToTimeoutException] {
      runSpark(clientMode, mainClassName(YarnDecommissioningDriver.getClass),
        appArgs = Seq(syncFile.getAbsolutePath),
        extraConf = Map(),
        numExecutors = 2,
        executionTimeout = 2 minutes)
    }
    assert(excludedHostPromise.isCompleted, "graceful decommission was not launched for any node")
    val excludedHost = ThreadUtils.awaitResult(excludedHostFuture, 1 millisecond)
    assert(excludedHost.length > 0)
    getExcludedHostStateUpdate(excludedHost)
  }

  /**
   * This method repeatedly schedules a task that checks the contents of the syncFile used to
   * synchronize with the Spark driver. When the syncFile is updated with the sync text then
   * YARN's graceful decommission mechanism is triggered, and the excluded host is returned
   * by completing excludedHostPromise.
   */
  private def scheduleDecommissionRunnable(excludedHostPromise: Promise[String]): Unit = {
    def decommissionRunnable(): Runnable = new Runnable() {
      override def run() {
        if (syncFile.exists() &&
          Files.toString(syncFile, StandardCharsets.UTF_8)
            .equals(YarnDecommissioningDriver.SYNC_TEXT)) {
          excludedHostPromise.complete(Try{
            logInfo("Launching graceful decommission of a node in YARN")
            gracefullyDecommissionNode(newYarnConfig, excludedHostsFile,
              decommissionTimeout = 10 seconds)
          })
        } else {
          logDebug("Waiting for sync file to be updated by the driver")
          executorService.schedule(decommissionRunnable(), 100, TimeUnit.MILLISECONDS)
        }
      }
    }
    executorService.schedule(decommissionRunnable(), 1, TimeUnit.SECONDS)
  }

  /**
   * This method should be called after the Spark application has completed, to parse
   * the container logs for messages about Yarn decommission related states involving
   * the node that was decommissioned.
   */
  private def getExcludedHostStateUpdate(excludedHost: String): Set[String] = {
    val stateChangeRe = {
      val decommissionStateRe = decommissionStates.mkString("|")
      "(?:%s.*(%s))|(?:(%s).*%s)".format(excludedHost, decommissionStateRe,
        decommissionStateRe, excludedHost).r
    }
    (for {
      file <- JFiles.walk(Paths.get(getClusterWorkDir.getAbsolutePath))
                    .iterator().asScala
      if file.getFileName.toString == "stderr"
      line <- Source.fromFile(file.toFile).getLines()
      matchingSubgroups <- stateChangeRe.findFirstMatchIn(line)
                                        .map(_.subgroups.filter(_ != null)).toSeq
      group <- matchingSubgroups
    } yield group).toSet
  }
}

private object YarnDecommissioningDriver extends Logging with Matchers {

  val SYNC_TEXT = "First action completed. Start decommissioning."
  val WAIT_TIMEOUT_MILLIS = 10000
  val DECOMMISSION_WAIT_TIME_MILLIS = 500000

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // scalastyle:off println
      System.err.println(
        s"""
           |Invalid command line: ${args.mkString(" ")}
           |
        |Usage: YarnDecommissioningDriver [sync file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }
    val sc = new SparkContext(new SparkConf()
        .setAppName("Yarn Decommissioning Test"))
    try {
      logInfo("Starting YarnDecommissioningDriver")
      val counts = sc.parallelize(1 to 10, 4)
                      .map{ x => (x%7, x)}
                      .reduceByKey(_ + _).collect
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      logInfo(s"Got ${counts.mkString(",")}")

      val syncFile = new File(args(0))
      Files.append(SYNC_TEXT, syncFile, StandardCharsets.UTF_8)
      logInfo(s"Sync file ${syncFile} written")

      // Wait for decommissioning and then for decommissioned, the timeout in
      // the corresponding call to runSpark will interrupt this
      Thread.sleep(DECOMMISSION_WAIT_TIME_MILLIS)
    } catch {
      case e =>
        logError(s"Driver exception: ${e.getMessage}")
    }
    finally {
      sc.stop()
    }
  }
}

