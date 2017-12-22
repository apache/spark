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

package org.apache.spark

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

/**
 * Thread audit for test suites.
 *
 * Thread audit happens normally in [[SparkFunSuite]] automatically when a new test suite created.
 * The only prerequisite for that is that the test class must extend [[SparkFunSuite]].
 *
 * There are some test suites which are doing initialization before [[SparkFunSuite#beforeAll]]
 * executed. This case auditing can be moved into another place in the call sequence.
 *
 * To do the audit in a custom place/way the following can be done:
 *
 * class MyTestSuite extends SparkFunSuite {
 *
 *   override val doThreadAuditInSparkFunSuite = false
 *
 *   protected override def beforeAll(): Unit = {
 *     doThreadPreAudit
 *     super.beforeAll
 *   }
 *
 *   protected override def afterAll(): Unit = {
 *     super.afterAll
 *     doThreadPostAudit
 *   }
 * }
 */
trait ThreadAudit extends Logging {

  val threadWhiteList = Set(
    /**
     * Netty related internal threads.
     * These are excluded because their lifecycle is handled by the netty itself
     * and spark has no explicit effect on them.
     */
    "netty.*",

    /**
     * Netty related internal threads.
     * A Single-thread singleton EventExecutor inside netty which creates such threads.
     * These are excluded because their lifecycle is handled by the netty itself
     * and spark has no explicit effect on them.
     */
    "globalEventExecutor.*",

    /**
     * Netty related internal threads.
     * Checks if a thread is alive periodically and runs a task when a thread dies.
     * These are excluded because their lifecycle is handled by the netty itself
     * and spark has no explicit effect on them.
     */
    "threadDeathWatcher.*",

    /**
     * During [[SparkContext]] creation [[org.apache.spark.rpc.netty.NettyRpcEnv]]
     * creates event loops. One is wrapped inside
     * [[org.apache.spark.network.server.TransportServer]]
     * the other one is inside [[org.apache.spark.network.client.TransportClient]].
     * The thread pools behind shut down asynchronously triggered by [[SparkContext#stop]].
     * Manually checked and all of them stopped properly.
     */
    "rpc-client.*",
    "rpc-server.*",

    /**
     * During [[SparkContext]] creation BlockManager
     * creates event loops. One is wrapped inside
     * [[org.apache.spark.network.server.TransportServer]]
     * the other one is inside [[org.apache.spark.network.client.TransportClient]].
     * The thread pools behind shut down asynchronously triggered by [[SparkContext#stop]].
     * Manually checked and all of them stopped properly.
     */
    "shuffle-client.*",
    "shuffle-server.*"
  )
  private var threadNamesSnapshot: Set[String] = Set.empty

  protected def doThreadPreAudit(): Unit = snapshotRunningThreadNames
  protected def doThreadPostAudit(): Unit = printRemainingThreadNames

  private def snapshotRunningThreadNames(): Unit = {
    threadNamesSnapshot = runningThreadNames
  }

  private def printRemainingThreadNames(): Unit = {
    val shortSuiteName = this.getClass.getName.replaceAll("org.apache.spark", "o.a.s")

    if (threadNamesSnapshot.nonEmpty) {
      val remainingThreadNames = runningThreadNames.diff(threadNamesSnapshot)
        .filterNot { s => threadWhiteList.exists(s.matches(_)) }
      if (remainingThreadNames.nonEmpty) {
        logWarning(s"\n\n===== POSSIBLE THREAD LEAK IN SUITE $shortSuiteName, " +
          s"thread names: ${remainingThreadNames.mkString(", ")} =====\n")
      }
    } else {
      logWarning("\n\n===== THREAD AUDIT POST ACTION CALLED " +
        s"WITHOUT PRE ACTION IN SUITE $shortSuiteName =====\n")
    }
  }

  private def runningThreadNames(): Set[String] = {
    Thread.getAllStackTraces.keySet().asScala.map(_.getName).toSet
  }
}
