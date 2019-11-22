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
     * Calling [[SparkContext#stop]] will shut down the thread pool of this event group
     * asynchronously. In each case proper stopping is checked manually.
     */
    "rpc-client.*",
    "rpc-server.*",

    /**
     * During [[org.apache.spark.network.TransportContext]] construction a separate event loop could
     * be created for handling ChunkFetchRequest.
     * Calling [[org.apache.spark.network.TransportContext#close]] will shut down the thread pool
     * of this event group asynchronously. In each case proper stopping is checked manually.
     */
    "shuffle-chunk-fetch-handler.*",

    /**
     * During [[SparkContext]] creation BlockManager creates event loops. One is wrapped inside
     * [[org.apache.spark.network.server.TransportServer]]
     * the other one is inside [[org.apache.spark.network.client.TransportClient]].
     * Calling [[SparkContext#stop]] will shut down the thread pool of this event group
     * asynchronously. In each case proper stopping is checked manually.
     */
    "shuffle-client.*",
    "shuffle-server.*",

    /**
     * Global cleaner thread that manage statistics data references of Hadoop filesystems.
     * This is excluded because their lifecycle is handled by Hadoop and spark has no explicit
     * effect on it.
     */
    "org.apache.hadoop.fs.FileSystem\\$Statistics\\$StatisticsDataReferenceCleaner",

    /**
     * A global thread pool for broadcast exchange executions.
     */
    "broadcast-exchange.*",

    /**
     * A thread started by JRE to support safe parallel execution of waitFor() and exitStatus()
     * methods to forked subprocesses.
     */
    "process reaper"
  )
  private var threadNamesSnapshot: Set[String] = Set.empty

  protected def doThreadPreAudit(): Unit = {
    threadNamesSnapshot = runningThreadNames()
  }

  protected def doThreadPostAudit(): Unit = {
    val shortSuiteName = this.getClass.getName.replaceAll("org.apache.spark", "o.a.s")

    if (threadNamesSnapshot.nonEmpty) {
      val remainingThreadNames = runningThreadNames().diff(threadNamesSnapshot)
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
