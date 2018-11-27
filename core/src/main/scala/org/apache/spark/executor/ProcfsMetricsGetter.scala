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
import java.nio.charset.Charset
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
private[spark] class ProcfsMetricsGetter(
    val procfsDir: String = "/proc/",
    val pSizeForTest: Long = 0) extends Logging {
  val procfsStatFile = "stat"
  val testing = sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  val pageSize = computePageSize()
  var isAvailable: Boolean = isProcfsAvailable
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
      val shouldLogStageExecutorMetrics =
        SparkEnv.get.conf.get(config.EVENT_LOG_STAGE_EXECUTOR_METRICS)
      val shouldLogStageExecutorProcessTreeMetrics =
        SparkEnv.get.conf.get(config.EVENT_LOG_PROCESS_TREE_METRICS)
      procDirExists.get && shouldLogStageExecutorProcessTreeMetrics && shouldLogStageExecutorMetrics
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
      val pid = Integer.parseInt(out.split("\n")(0))
      return pid;
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
      return pSizeForTest;
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

  private def computeProcessTree(): Set[Int] = {
    if (!isAvailable || testing) {
      return Set()
    }
    var ptree: Set[Int] = Set()
    ptree += pid
    val queue = mutable.Queue.empty[Int]
    queue += pid
    while( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPids(p)
      if(!c.isEmpty) {
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
      val error = process.getErrorStream
      var errorString = ""
      (0 until error.available()).foreach { i =>
        errorString += error.read()
      }
      val exitCode = process.waitFor()
      stdoutThread.join()
      // pgrep will have exit code of 1 if there are more than one child process
      // and it will have a exit code of 2 if there is no child process
      if (exitCode != 0 && exitCode > 2) {
        val cmd = builder.command().toArray.mkString(" ")
        logWarning(s"Process $cmd" +
          s" exited with code $exitCode, with stderr:" + s"${errorString} ")
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

  def addProcfsMetricsFromOneProcess(
      allMetrics: ProcfsMetrics,
      pid: Int):
      ProcfsMetrics = {

    // Hadoop ProcfsBasedProcessTree class used regex and pattern matching to retrive the memory
    // info. I tried that but found it not correct during tests, so I used normal string analysis
    // instead. The computation of RSS and Vmem are based on proc(5):
    // http://man7.org/linux/man-pages/man5/proc.5.html
    try {
      val pidDir = new File(procfsDir, pid.toString)
      Utils.tryWithResource( new InputStreamReader(
        new FileInputStream(
          new File(pidDir, procfsStatFile)), Charset.forName("UTF-8"))) { fReader =>
        Utils.tryWithResource( new BufferedReader(fReader)) { in =>
          val procInfo = in.readLine
          val procInfoSplit = procInfo.split(" ")
          if (procInfoSplit != null) {
            val vmem = procInfoSplit(22).toLong
            val rssPages = procInfoSplit(23).toLong
            if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
              return allMetrics.copy(
                jvmVmemTotal = allMetrics.jvmVmemTotal + vmem,
                jvmRSSTotal = allMetrics.jvmRSSTotal + (rssPages*pageSize)
              )
            }
            else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
              return allMetrics.copy(
                pythonVmemTotal = allMetrics.pythonVmemTotal + vmem,
                pythonRSSTotal = allMetrics.pythonRSSTotal + (rssPages*pageSize)
              )
            }
              return allMetrics.copy(
                otherVmemTotal = allMetrics.otherVmemTotal + vmem,
                otherRSSTotal = allMetrics.otherRSSTotal + (rssPages*pageSize)
              )
          }
          else {
            return ProcfsMetrics(0, 0, 0, 0, 0, 0)
          }
        }
      }
    } catch {
      case f: FileNotFoundException =>
        logWarning("There was a problem with reading" +
          " the stat file of the process. ", f)
        ProcfsMetrics(0, 0, 0, 0, 0, 0)
    }
  }

  private[spark] def computeAllMetrics(): ProcfsMetrics = {
    if (!isAvailable) {
      return ProcfsMetrics(0, 0, 0, 0, 0, 0)
    }
    val pids = computeProcessTree
    var allMetrics = ProcfsMetrics(0, 0, 0, 0, 0, 0)
    for (p <- pids) {
      allMetrics = addProcfsMetricsFromOneProcess(allMetrics, p)
    }
    return allMetrics
  }
}

private[spark] object ProcfsMetricsGetter {
  final val pTreeInfo = new ProcfsMetricsGetter
}
