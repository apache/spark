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

import java.lang.management.ManagementFactory
import java.util.concurrent.ThreadPoolExecutor
import javax.management.{MBeanServer, ObjectName}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.metrics.source.Source

private[spark]
class ExecutorSource(threadPool: ThreadPoolExecutor, executorId: String) extends Source {

  private def fileStats(scheme: String) : Option[FileSystem.Statistics] =
    FileSystem.getAllStatistics.asScala.find(s => s.getScheme.equals(scheme))

  private def registerFileSystemStat[T](
        scheme: String, name: String, f: FileSystem.Statistics => T, defaultValue: T) = {
    metricRegistry.register(MetricRegistry.name("filesystem", scheme, name), new Gauge[T] {
      override def getValue: T = fileStats(scheme).map(f).getOrElse(defaultValue)
    })
  }

  override val metricRegistry = new MetricRegistry()

  override val sourceName = "executor"

  // Gauge for executor thread pool's actively executing task counts
  metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), new Gauge[Int] {
    override def getValue: Int = threadPool.getActiveCount()
  })

  // Gauge for executor thread pool's approximate total number of tasks that have been completed
  metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getCompletedTaskCount()
  })

  // Gauge for executor thread pool's current number of threads
  metricRegistry.register(MetricRegistry.name("threadpool", "currentPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getPoolSize()
  })

  // Gauge got executor thread pool's largest number of threads that have ever simultaneously
  // been in th pool
  metricRegistry.register(MetricRegistry.name("threadpool", "maxPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getMaximumPoolSize()
  })

  // Gauge for file system stats of this executor
  for (scheme <- Array("hdfs", "file")) {
    registerFileSystemStat(scheme, "read_bytes", _.getBytesRead(), 0L)
    registerFileSystemStat(scheme, "write_bytes", _.getBytesWritten(), 0L)
    registerFileSystemStat(scheme, "read_ops", _.getReadOps(), 0)
    registerFileSystemStat(scheme, "largeRead_ops", _.getLargeReadOps(), 0)
    registerFileSystemStat(scheme, "write_ops", _.getWriteOps(), 0)
  }

  // Dropwizard metrics gauge measuring the executor's process CPU time.
  // This Gauge will try to get and return the JVM Process CPU time or return -1 otherwise.
  // The CPU time value is returned in nanoseconds.
  // It will use proprietary extensions such as com.sun.management.OperatingSystemMXBean or
  // com.ibm.lang.management.OperatingSystemMXBean, if available.
  metricRegistry.register(MetricRegistry.name("jvmCpuTime"), new Gauge[Long] {
    val mBean: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val name = new ObjectName("java.lang", "type", "OperatingSystem")
    override def getValue: Long = {
      try {
        // return JVM process CPU time if the ProcessCpuTime method is available
        mBean.getAttribute(name, "ProcessCpuTime").asInstanceOf[Long]
      } catch {
        case NonFatal(_) => -1L
      }
    }
  })

  // Expose executor task metrics using the Dropwizard metrics system.
  // The list is taken from TaskMetrics.scala
  val METRIC_CPU_TIME = metricRegistry.counter(MetricRegistry.name("cpuTime"))
  val METRIC_RUN_TIME = metricRegistry.counter(MetricRegistry.name("runTime"))
  val METRIC_JVM_GC_TIME = metricRegistry.counter(MetricRegistry.name("jvmGCTime"))
  val METRIC_DESERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeTime"))
  val METRIC_DESERIALIZE_CPU_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeCpuTime"))
  val METRIC_RESULT_SERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("resultSerializationTime"))
  val METRIC_SHUFFLE_FETCH_WAIT_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleFetchWaitTime"))
  val METRIC_SHUFFLE_WRITE_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleWriteTime"))
  val METRIC_SHUFFLE_TOTAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleTotalBytesRead"))
  val METRIC_SHUFFLE_REMOTE_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesRead"))
  val METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesReadToDisk"))
  val METRIC_SHUFFLE_LOCAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBytesRead"))
  val METRIC_SHUFFLE_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsRead"))
  val METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBlocksFetched"))
  val METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBlocksFetched"))
  val METRIC_SHUFFLE_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleBytesWritten"))
  val METRIC_SHUFFLE_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsWritten"))
  val METRIC_INPUT_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("bytesRead"))
  val METRIC_INPUT_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("recordsRead"))
  val METRIC_OUTPUT_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("bytesWritten"))
  val METRIC_OUTPUT_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("recordsWritten"))
  val METRIC_RESULT_SIZE =
    metricRegistry.counter(MetricRegistry.name("resultSize"))
  val METRIC_DISK_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("diskBytesSpilled"))
  val METRIC_MEMORY_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("memoryBytesSpilled"))
}
