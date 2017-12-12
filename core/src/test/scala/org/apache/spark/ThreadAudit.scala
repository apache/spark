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

// scalastyle:off
import scala.collection.JavaConversions._

import org.apache.spark.internal.Logging
// scalastyle:on

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
    "threadDeathWatcher.*"
  )
  private var threadNamesSnapshot: Set[String] = Set.empty

  protected def doThreadPreAudit(): Unit = snapshotRunningThreadNames
  protected def doThreadPostAudit(): Unit = printRemainingThreadNames

  private def snapshotRunningThreadNames(): Unit = {
    threadNamesSnapshot = runningThreadNames
  }

  private def printRemainingThreadNames(): Unit = {
    val remainingThreadNames = runningThreadNames.diff(threadNamesSnapshot)
      .filterNot { s => threadWhiteList.exists(s.matches(_)) }
    if (remainingThreadNames.nonEmpty) {
      val suiteName = this.getClass.getName
      val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
      logWarning(s"\n\n===== POSSIBLE THREAD LEAK IN SUITE $shortSuiteName, " +
        s"thread names: ${remainingThreadNames.mkString(", ")} =====\n")
    }
  }

  private def runningThreadNames(): Set[String] = {
    Thread.getAllStackTraces.keySet().map(_.getName).toSet
  }
}
