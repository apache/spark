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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging


// Some of the ideas here are taken from the ProcfsBasedProcessTree class in hadoop
// project.
class ProcfsBasedSystems  extends ProcessTreeMetrics with Logging {
  val procfsDir = "/proc/"
  var isAvailable: Boolean = isItProcfsBased
  val pid: Int = computePid()
  val ptree: scala.collection.mutable.Map[ Int, Set[Int]] =
    scala.collection.mutable.Map[ Int, Set[Int]]()
  val PROCFS_STAT_FILE = "stat"
  var latestVmemTotal: Long = 0
  var latestRSSTotal: Long = 0

  createProcessTree

  def isItProcfsBased: Boolean = {
    val testing = sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
    if (testing) {
      return true
    }
    try {
      if (!Files.exists(Paths.get(procfsDir))) {
        return false
      }
    }
    catch {
      case f: FileNotFoundException => return false
    }

    val shouldLogStageExecutorProcessTreeMetrics = org.apache.spark.SparkEnv.get.conf.
      getBoolean("spark.eventLog.logStageExecutorProcessTreeMetrics.enabled", true)
    true && shouldLogStageExecutorProcessTreeMetrics
  }


  def computePid(): Int = {
    if (!isAvailable) {
      return -1;
    }
    try {
      // This can be simplified in java9:
      // https://docs.oracle.com/javase/9/docs/api/java/lang/ProcessHandle.html
      val cmd = Array("bash", "-c", "echo $PPID")
      val length = 10
      var out: Array[Byte] = Array.fill[Byte](length)(0)
      Runtime.getRuntime.exec(cmd).getInputStream.read(out)
      val pid = Integer.parseInt(new String(out, "UTF-8").trim)
      return pid;
    }
    catch {
      case e: IOException => logDebug("IO Exception when trying to compute process tree." +
        " As a result reporting of ProcessTree metrics is stopped")
        isAvailable = false
        return -1
      case _ => logDebug("Some exception occurred when trying to compute process tree. " +
        "As a result reporting of ProcessTree metrics is stopped")
        isAvailable = false
        return -1
    }
  }


  def createProcessTree(): Unit = {
    if (!isAvailable) {
      return
    }
    val queue: Queue[Int] = new Queue[Int]()
    queue += pid
    while( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPIds(p)
      if(!c.isEmpty) {
        queue ++= c
        ptree += (p -> c.toSet)
      }
      else {
        ptree += (p -> Set[Int]())
      }
    }
  }


  def updateProcessTree(): Unit = {
    if (!isAvailable) {
      return
    }
    val queue: Queue[Int] = new Queue[Int]()
    queue += pid
    while( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPIds(p)
      if(!c.isEmpty) {
        queue ++= c
        val preChildren = ptree.get(p)
        preChildren match {
          case Some(children) => if (!c.toSet.equals(children)) {
            val diff: Set[Int] = children -- c.toSet
            ptree.update(p, c.toSet )
            diff.foreach(ptree.remove(_))
          }
          case None => ptree.update(p, c.toSet )
        }
      }
      else {
        ptree.update(p, Set[Int]())
      }
    }
  }


  /**
   * Hadoop ProcfsBasedProcessTree class used regex and pattern matching to retrive the memory
   * info. I tried that but found it not correct during tests, so I used normal string analysis
   * instead. The computation of RSS and Vmem are based on proc(5):
   * http://man7.org/linux/man-pages/man5/proc.5.html
   */
  def getProcessInfo(pid: Int): Unit = {
    try {
      val pidDir: File = new File(procfsDir, pid.toString)
      val fReader = new InputStreamReader(
        new FileInputStream(
          new File(pidDir, PROCFS_STAT_FILE)), Charset.forName("UTF-8"))
      val in: BufferedReader = new BufferedReader(fReader)
      val procInfo = in.readLine
      in.close
      fReader.close
      val procInfoSplit = procInfo.split(" ")
      if ( procInfoSplit != null ) {
        latestVmemTotal += procInfoSplit(22).toLong
        latestRSSTotal += procInfoSplit(23).toLong
      }
    } catch {
      case f: FileNotFoundException => return null
    }
  }


  def getRSSInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    updateProcessTree
    val pids = ptree.keySet
    latestRSSTotal = 0
    latestVmemTotal = 0
    for (p <- pids) {
       getProcessInfo(p)
    }
    latestRSSTotal
  }


  def getVirtualMemInfo(): Long = {
    if (!isAvailable) {
      return -1
    }
    // We won't call updateProcessTree and also compute total virtual memory here
    // since we already did all of this when we computed RSS info
    latestVmemTotal
  }


  def getChildPIds(pid: Int): ArrayBuffer[Int] = {
    try {
      val cmd = Array("pgrep", "-P", pid.toString)
      val input = Runtime.getRuntime.exec(cmd).getInputStream
      val childPidsInByte: mutable.ArrayBuffer[Byte] = new mutable.ArrayBuffer()
      var d = input.read()
      while (d != -1) {
        childPidsInByte.append(d.asInstanceOf[Byte])
        d = input.read()
      }
      input.close()
      val childPids = new String(childPidsInByte.toArray, "UTF-8").split("\n")
      val childPidsInInt: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      for (p <- childPids) {
        if (p != "") {
          childPidsInInt += Integer.parseInt(p)
        }
      }
      childPidsInInt
    } catch {
    case e: IOException => logDebug("IO Exception when trying to compute process tree." +
      " As a result reporting of ProcessTree metrics is stopped")
      isAvailable = false
      return new mutable.ArrayBuffer()
    case _ => logDebug("Some exception occurred when trying to compute process tree. As a result" +
      "  reporting of ProcessTree metrics is stopped")
      isAvailable = false
      return new mutable.ArrayBuffer()
    }
  }
}
