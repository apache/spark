package spark.executor

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.fs.LocalFileSystem

import scala.collection.JavaConversions._

import spark.metrics.source.Source

class ExecutorSource(val executor: Executor) extends Source {
  private def fileStats(scheme: String) : Option[FileSystem.Statistics] =
    FileSystem.getAllStatistics().filter(s => s.getScheme.equals(scheme)).headOption

  private def registerFileSystemStat[T](scheme: String, name: String, f: FileSystem.Statistics => T, defaultValue: T) = {
    metricRegistry.register(MetricRegistry.name("filesystem", scheme, name), new Gauge[T] {
      override def getValue: T = fileStats(scheme).map(f).getOrElse(defaultValue)
    })
  }

  val metricRegistry = new MetricRegistry()
  val sourceName = "executor"

  // Gauge for executor thread pool's actively executing task counts
  metricRegistry.register(MetricRegistry.name("threadpool", "activeTask", "count"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getActiveCount()
  })

  // Gauge for executor thread pool's approximate total number of tasks that have been completed
  metricRegistry.register(MetricRegistry.name("threadpool", "completeTask", "count"), new Gauge[Long] {
    override def getValue: Long = executor.threadPool.getCompletedTaskCount()
  })

  // Gauge for executor thread pool's current number of threads
  metricRegistry.register(MetricRegistry.name("threadpool", "currentPool", "size"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getPoolSize()
  })

  // Gauge got executor thread pool's largest number of threads that have ever simultaneously been in th pool
  metricRegistry.register(MetricRegistry.name("threadpool", "maxPool", "size"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getMaximumPoolSize()
  })

  for (scheme <- Array("hdfs", "file")) {
    registerFileSystemStat(scheme, "bytesRead", _.getBytesRead(), 0L)
    registerFileSystemStat(scheme, "bytesWritten", _.getBytesWritten(), 0L)
    registerFileSystemStat(scheme, "readOps", _.getReadOps(), 0)
    registerFileSystemStat(scheme, "largeReadOps", _.getLargeReadOps(), 0)
    registerFileSystemStat(scheme, "writeOps", _.getWriteOps(), 0)
  }
}
