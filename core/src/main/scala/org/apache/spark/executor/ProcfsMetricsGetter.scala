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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.{config, Logging}
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
  private val pid = computePid()

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

  private def computePid(): Int = {
    if (!isAvailable || testing) {
      return -1;
    }
    try {
      // This can be simplified in java9:
      // https://docs.oracle.com/javase/9/docs/api/java/lang/ProcessHandle.html
      val cmd = Array("bash", "-c", "echo $PPID")
      val out = Utils.executeAndGetOutput(cmd)
      Integer.parseInt(out.split("\n")(0))
    }
    catch {
      case e: SparkException =>
        logWarning("Exception when trying to compute process tree." +
          " As a result reporting of ProcessTree metrics is stopped", e)
        isAvailable = false
        -1
    }
  }

  private def computePageSize(): Long = {
    if (testing) {
      return 4096;
    }
    try {
      val cmd = Array("getconf", "PAGESIZE")
      val out = Utils.executeAndGetOutput(cmd)
      Integer.parseInt(out.split("\n")(0))
    } catch {
      case e: Exception =>
        logWarning("Exception when trying to compute pagesize, as a" +
          " result reporting of ProcessTree metrics is stopped")
        isAvailable = false
        0
    }
  }

  // Exposed for testing
  private[executor] def computeProcessTree(): Set[Int] = {
    if (!isAvailable || testing) {
      return Set()
    }
    var ptree: Set[Int] = Set()
    ptree += pid
    val queue = mutable.Queue.empty[Int]
    queue += pid
    while ( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPids(p)
      if (!c.isEmpty) {
        queue ++= c
        ptree ++= c.toSet
      }
    }
    ptree
  }

  private def getChildPids(pid: Int): ArrayBuffer[Int] = {
    try {
      val builder = new ProcessBuilder("pgrep", "-P", pid.toString)
      val process = builder.start()
      val childPidsInInt = mutable.ArrayBuffer.empty[Int]
      def appendChildPid(s: String): Unit = {
        if (s != "") {
          logTrace("Found a child pid:" + s)
          childPidsInInt += Integer.parseInt(s)
        }
      }
      val stdoutThread = Utils.processStreamByLine("read stdout for pgrep",
        process.getInputStream, appendChildPid)
      val errorStringBuilder = new StringBuilder()
      val stdErrThread = Utils.processStreamByLine(
        "stderr for pgrep",
        process.getErrorStream,
        line => errorStringBuilder.append(line))
      val exitCode = process.waitFor()
      stdoutThread.join()
      stdErrThread.join()
      val errorString = errorStringBuilder.toString()
      // pgrep will have exit code of 1 if there are more than one child process
      // and it will have a exit code of 2 if there is no child process
      if (exitCode != 0 && exitCode > 2) {
        val cmd = builder.command().toArray.mkString(" ")
        logWarning(s"Process $cmd exited with code $exitCode and stderr: $errorString")
        throw new SparkException(s"Process $cmd exited with code $exitCode")
      }
      childPidsInInt
    } catch {
      case e: Exception =>
        logWarning("Exception when trying to compute process tree." +
          " As a result reporting of ProcessTree metrics is stopped.", e)
        isAvailable = false
        mutable.ArrayBuffer.empty[Int]
    }
  }

  // Exposed for testing
  private[executor] def addProcfsMetricsFromOneProcess(
      allMetrics: ProcfsMetrics,
      pid: Int): ProcfsMetrics = {

    // The computation of RSS and Vmem are based on proc(5):
    // http://man7.org/linux/man-pages/man5/proc.5.html
    try {
      val pidDir = new File(procfsDir, pid.toString)
      def openReader(): BufferedReader = {
        val f = new File(new File(procfsDir, pid.toString), procfsStatFile)
        new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8))
      }
      Utils.tryWithResource(openReader) { in =>
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
        logWarning("There was a problem with reading" +
          " the stat file of the process. ", f)
        throw f
    }
  }

  private[spark] def computeAllMetrics(): ProcfsMetrics = {
    if (!isAvailable) {
      return ProcfsMetrics(0, 0, 0, 0, 0, 0)
    }
    val pids = computeProcessTree
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
