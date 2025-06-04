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
package org.apache.spark.metrics

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import javax.management.ObjectName

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ProcfsMetricsGetter
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.MemoryManager

/**
 * Executor metric types for executor-level metrics stored in ExecutorMetrics.
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long]
  private[spark] def names: Seq[String]
}

sealed trait SingleValueExecutorMetricType extends ExecutorMetricType {
  override private[spark] def names = {
    Seq(getClass().getName().
      stripSuffix("$").split("""\.""").last)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](1)
    metrics(0) = getMetricValue(memoryManager)
    metrics
  }

  private[spark] def getMetricValue(memoryManager: MemoryManager): Long
}

private[spark] abstract class MemoryManagerExecutorMetricType(
    f: MemoryManager => Long) extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends SingleValueExecutorMetricType {
  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

case object ProcessTreeMetrics extends ExecutorMetricType {
  override val names = Seq(
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory")

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
    val processTreeMetrics = new Array[Long](names.length)
    processTreeMetrics(0) = allMetrics.jvmVmemTotal
    processTreeMetrics(1) = allMetrics.jvmRSSTotal
    processTreeMetrics(2) = allMetrics.pythonVmemTotal
    processTreeMetrics(3) = allMetrics.pythonRSSTotal
    processTreeMetrics(4) = allMetrics.otherVmemTotal
    processTreeMetrics(5) = allMetrics.otherRSSTotal
    processTreeMetrics
  }
}

case object GarbageCollectionMetrics extends ExecutorMetricType with Logging {
  private var nonBuiltInCollectors: Seq[String] = Nil

  override val names = Seq(
    "MinorGCCount",
    "MinorGCTime",
    "MajorGCCount",
    "MajorGCTime",
    "TotalGCTime"
  )

  /* We builtin some common GC collectors which categorized as young generation and old */
  private[spark] val YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "Copy",
    "PS Scavenge",
    "ParNew",
    "G1 Young Generation"
  )

  private[spark] val OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "MarkSweepCompact",
    "PS MarkSweep",
    "ConcurrentMarkSweep",
    "G1 Old Generation"
  )

  private lazy val youngGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS)
  }

  private lazy val oldGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val gcMetrics = new Array[Long](names.length)
    val mxBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    gcMetrics(4) = mxBeans.map(_.getCollectionTime).sum
    mxBeans.foreach { mxBean =>
      if (youngGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(0) = mxBean.getCollectionCount
        gcMetrics(1) = mxBean.getCollectionTime
      } else if (oldGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(2) = mxBean.getCollectionCount
        gcMetrics(3) = mxBean.getCollectionTime
      } else if (!nonBuiltInCollectors.contains(mxBean.getName)) {
        nonBuiltInCollectors = mxBean.getName +: nonBuiltInCollectors
        // log it when first seen
        logWarning(s"To enable non-built-in garbage collector(s) " +
          s"$nonBuiltInCollectors, users should configure it(them) to " +
          s"${config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS.key} or " +
          s"${config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS.key}")
      } else {
        // do nothing
      }
    }
    gcMetrics
  }
}

case object ContainerCgroupMetrics extends ExecutorMetricType with Logging {

  override val names = Seq(
    "ContainerCPUMilliCores",
    "ContainerMemoryWorkingSetBytes"
  )

  lazy val isAvailable = SparkEnv.get.conf.get(config.EXECUTOR_CONTAINER_METRICS_ENABLED)

  /** read the cumulative CPU usage (ns) of the cgroup (v1 or v2) */
  private def readCpuUsageNs(): Long = {
    val v2 = java.nio.file.Paths.get("/sys/fs/cgroup/cpu.stat")
    val v1 = java.nio.file.Paths.get("/sys/fs/cgroup/cpuacct/cpuacct.usage")
    if (java.nio.file.Files.exists(v2)) {
      // cpu.stat line looks like "usage_usec 123456789"
      java.nio.file.Files.readAllLines(v2).asScala.find(_.startsWith("usage_usec"))
        .map(_.split("\\s+")(1).toLong * 1000L).getOrElse(0L)
    } else {
      java.nio.file.Files.readAllLines(v1).get(0).trim.toLong // already ns
    }
  }

  private def readWorkingSetBytes(): Long = {
    val usage = java.nio.file.Files.readAllLines(
      java.nio.file.Paths.get("/sys/fs/cgroup/memory.current")).get(0).trim.toLong
    val inactive = java.nio.file.Files.readAllLines(
        java.nio.file.Paths.get("/sys/fs/cgroup/memory.stat")).asScala
      .find(_.startsWith("inactive_file"))
      .map(_.split("\\s+")(1).toLong).getOrElse(0L)
    usage - inactive
  }

  @volatile private var lastUsageNs = readCpuUsageNs()
  @volatile private var lastTsNs = System.nanoTime()

  override private[spark] def getMetricValues(mm: MemoryManager): Array[Long] = {
    if (!isAvailable) {
      return Array(0, 0)
    }

    val nowUsageNs = readCpuUsageNs()
    val nowTsNs = System.nanoTime()

    val usageDelta = (nowUsageNs - lastUsageNs).toDouble
    val timeDelta = (nowTsNs - lastTsNs).toDouble
    val millicores = if (nowTsNs > lastTsNs) (usageDelta / timeDelta * 1000).round else 0L

    lastUsageNs = nowUsageNs
    lastTsNs = nowTsNs

    Array(millicores, readWorkingSetBytes())
  }
}

case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)

case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)

case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)

case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)

case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")

case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")

private[spark] object ExecutorMetricType {

  // List of all executor metric getters
  val metricGetters = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory,
    ProcessTreeMetrics,
    ContainerCgroupMetrics,
    GarbageCollectionMetrics
  )

  val (metricToOffset, numMetrics) = {
    var numberOfMetrics = 0
    val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
    metricGetters.foreach { m =>
      m.names.indices.foreach { idx =>
        definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
      }
      numberOfMetrics += m.names.length
    }
    (definedMetricsAndOffset, numberOfMetrics)
  }
}
