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
package org.apache.spark.monitor

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.sun.management.HotSpotDiagnosticMXBean
import org.apache.commons.lang3.SystemUtils
import scala.concurrent.duration._
import sun.jvmstat.monitor._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

class JVMQuake(conf: SparkConf) extends Logging {

  import JVMQuake._

  private[this] val appId = conf.get("spark.app.id", "test-app-id").stripPrefix("application_")
  private[this] val threshold = conf.getTimeAsMs("spark.jvmQuake.threshold", "100s").millis.toNanos
  private[this] val jvmQuakeEnabled = conf.getBoolean("spark.jvmQuake.enabled", false)
  private[this] val checkInterval = conf.getTimeAsMs("spark.jvmQuake.checkInterval", "1s")
  private[this] val runTimeWeight = conf.getDouble("spark.jvmQuake.runTimeWeight", 1)

  private[this] var lastExitTime = getLastExitTime
  private[this] var lastGCTime = getLastGCTime
  private[this] var bucket = 0L
  private[monitor] var heapExist = false

  val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("jvm-quake")

  def run(): Unit = {
    val currentExitTime = getLastExitTime
    val currentGcTime = getLastGCTime
    val gcTime = currentGcTime - lastGCTime
    val runTime = currentExitTime - lastExitTime - gcTime

    bucket = Math.max(0, bucket + gcTime - BigDecimal(runTime * runTimeWeight).toLong)
    logDebug(s"Time: (gcTime: $gcTime, runTime: $runTime)")
    logDebug(s"Capacity: (bucket: $bucket, threshold: ${threshold})")

    if (bucket > threshold) {
      logError(s"JVM GC has reached the threshold!!! (bucket: $bucket, threshold: $threshold)")
      if (shouldDumpHeap) {
        val savePath = getHeapDumpSavePath(appId)
        val linkPath = getHeapDumpLinkPath(appId)
        saveHeap(savePath, linkPath)
      }
    }

    lastExitTime = currentExitTime
    lastGCTime = currentGcTime
  }

  def start(): Unit = {
    if (jvmQuakeEnabled) {
      scheduler.scheduleAtFixedRate(new Runnable() {
        override def run(): Unit = {
          JVMQuake.this.run()
        }
      }, 0, checkInterval, TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    scheduler.shutdown()
  }

  def shouldDumpHeap: Boolean = {
    !heapExist
  }

  def getHeapDumpSavePath(appId: String): String = {
    s"${Utils.getLocalDir(conf).stripSuffix("/")}/$appId"
  }

  def getHeapDumpLinkPath(appId: String): String = {
    s"/tmp/spark-debug/apps/$appId"
  }

  def saveHeap(savePath: String, linkPath: String, live: Boolean = false): Unit = {
    val saveDir = new File(savePath)
    if (!saveDir.exists()) {
      saveDir.mkdirs()
    }
    val heapDumpFile = new File(saveDir, s"spark-quake-heapdump.hprof")
    if (heapDumpFile.exists()) {
      logInfo(s"Heap exits $heapDumpFile")
      heapExist = true
      return
    }
    logInfo(s"Starting heap dump at $heapDumpFile.")
    val server = ManagementFactory.getPlatformMBeanServer
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(server,
      "com.sun.management:type=HotSpotDiagnostic", classOf[HotSpotDiagnosticMXBean])
    mxBean.dumpHeap(heapDumpFile.getAbsolutePath, live)
    // link
    val linkDir = new File(linkPath)
    if (linkDir.exists()) {
      logInfo(s"Soft link exists $linkPath.")
    } else if (!linkDir.getParentFile.exists()) {
      linkDir.getParentFile.mkdirs()
    }
    try {
      Files.createSymbolicLink(linkDir.toPath, saveDir.toPath)
      logInfo(s"Create soft link at $linkPath.")
    } catch {
      case e: Exception =>
        logError(s"Link failed ", e)
    } finally {
      heapExist = true
    }
  }
}

object JVMQuake {

  private[this] var monitor: JVMQuake = _

  def create(sparkConf: SparkConf): JVMQuake = {
    set(new JVMQuake(sparkConf))
    monitor
  }

  def get: JVMQuake = {
    monitor
  }

  def set(monitor: JVMQuake): Unit = {
    this.monitor = monitor
  }

  implicit def timeToTick(value: Long): Tick = {
    new Tick(value)
  }

  private[this] val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toLong

  private[this] val monitoredVm: MonitoredVm = {
    val host = MonitoredHost.getMonitoredHost(new HostIdentifier("localhost"))
    host.getMonitoredVm(new VmIdentifier("local://%s@localhost".format(pid)))
  }

  val youngGCExitTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.0.lastExitTime")
  val fullGCExitTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.1.lastExitTime")
  val youngGCTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.0.time")
  val fullGCTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.1.time")

  def getLastExitTime: Long = {
    Math.max(youngGCExitTimeMonitor.getValue.asInstanceOf[Long],
      fullGCExitTimeMonitor.getValue.asInstanceOf[Long])
  }

  def getLastGCTime: Long = {
    youngGCTimeMonitor.getValue.asInstanceOf[Long] +
      fullGCTimeMonitor.getValue.asInstanceOf[Long]
  }
}

private[monitor] class Tick(value: Long) {
  // Convert tick to nanos according to jdk version
  def toNanos: Long = {
    val javaVersion = SystemUtils.JAVA_SPECIFICATION_VERSION
    if (javaVersion < "1.8") {
      value * 1.micros.toNanos
    } else {
      // already nanos, so just return
      value
    }
  }
}