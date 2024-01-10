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

package org.apache.spark.executor

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


private[spark] case class ProcfsMetrics(
    jvmVmemTotal: Long,
    jvmRSSTotal: Long,
    pythonVmemTotal: Long,
    pythonRSSTotal: Long,
    otherVmemTotal: Long,
    otherRSSTotal: Long)

// Some of the ideas here are taken from the ProcfsBasedProcessTree class in hadoop
// project.
private[spark] class ProcfsMetricsGetter(procfsDir: String = "/proc/") extends Logging {
  private val procfsStatFile = "stat"
  private val testing = Utils.isTesting
  private val pageSize = computePageSize()
  private var isAvailable: Boolean = isProcfsAvailable
  private val currentProcessHandle = ProcessHandle.current()

  private lazy val isProcfsAvailable: Boolean = {
    if (testing) {
       true
    }
    else {
      val procDirExists = Try(Files.exists(Paths.get(procfsDir))).recover {
        case ioe: IOException =>
          logWarning("Exception checking for procfs dir", ioe)
          false
      }
      val shouldPollProcessTreeMetrics =
        SparkEnv.get.conf.get(config.EXECUTOR_PROCESS_TREE_METRICS_ENABLED)
      procDirExists.get && shouldPollProcessTreeMetrics
    }
  }

  private def computePageSize(): Long = {
    if (testing) {
      return 4096;
    }
    try {
      val cmd = Array("getconf", "PAGESIZE")
      val out = Utils.executeAndGetOutput(cmd.toImmutableArraySeq)
      Integer.parseInt(out.split("\n")(0))
    } catch {
      case e: Exception =>
        logDebug("Exception when trying to compute pagesize, as a" +
          " result reporting of ProcessTree metrics is stopped")
        isAvailable = false
        0
    }
  }

  // Exposed for testing
  private[executor] def addProcfsMetricsFromOneProcess(
      allMetrics: ProcfsMetrics,
      pid: Long): ProcfsMetrics = {

    // The computation of RSS and Vmem are based on proc(5):
    // http://man7.org/linux/man-pages/man5/proc.5.html
    try {
      val pidDir = new File(procfsDir, pid.toString)
      def openReader(): BufferedReader = {
        val f = new File(pidDir, procfsStatFile)
        new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8))
      }
      Utils.tryWithResource(openReader()) { in =>
        val procInfo = in.readLine
        val procInfoSplit = procInfo.split(" ")
        val vmem = procInfoSplit(22).toLong
        val rssMem = procInfoSplit(23).toLong * pageSize
        if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
          allMetrics.copy(
            jvmVmemTotal = allMetrics.jvmVmemTotal + vmem,
            jvmRSSTotal = allMetrics.jvmRSSTotal + (rssMem)
          )
        }
        else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
          allMetrics.copy(
            pythonVmemTotal = allMetrics.pythonVmemTotal + vmem,
            pythonRSSTotal = allMetrics.pythonRSSTotal + (rssMem)
          )
        }
        else {
          allMetrics.copy(
            otherVmemTotal = allMetrics.otherVmemTotal + vmem,
            otherRSSTotal = allMetrics.otherRSSTotal + (rssMem)
          )
        }
      }
    } catch {
      case f: IOException =>
        logDebug("There was a problem with reading" +
          " the stat file of the process. ", f)
        throw f
    }
  }

  private[executor] def computeProcessTree(): Set[Long] = {
    if (!isAvailable) {
      Set.empty
    } else {
      val children = currentProcessHandle.descendants().map(_.pid()).toList.asScala.toSet
      children + currentProcessHandle.pid()
    }
  }

  private[spark] def computeAllMetrics(): ProcfsMetrics = {
    if (!isAvailable) {
      return ProcfsMetrics(0, 0, 0, 0, 0, 0)
    }
    val pids = computeProcessTree()
    var allMetrics = ProcfsMetrics(0, 0, 0, 0, 0, 0)
    for (p <- pids) {
      try {
        allMetrics = addProcfsMetricsFromOneProcess(allMetrics, p)
        // if we had an error getting any of the metrics, we don't want to
        // report partial metrics, as that would be misleading.
        if (!isAvailable) {
          return ProcfsMetrics(0, 0, 0, 0, 0, 0)
        }
      } catch {
        case _: IOException =>
          return ProcfsMetrics(0, 0, 0, 0, 0, 0)
      }
    }
    allMetrics
  }
}

private[spark] object ProcfsMetricsGetter {
  final val pTreeInfo = new ProcfsMetricsGetter
}
