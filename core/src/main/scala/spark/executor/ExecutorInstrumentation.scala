package spark.executor

import com.codahale.metrics.{Gauge, MetricRegistry}

import spark.metrics.source.Source

class ExecutorInstrumentation(val executor: Executor) extends Source {
  val metricRegistry = new MetricRegistry()
  val sourceName = "executor"

  // Gauge for executor thread pool's actively executing task counts
  metricRegistry.register(MetricRegistry.name("threadpool", "active_task", "number"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getActiveCount()
  })

  // Gauge for executor thread pool's approximate total number of tasks that have been completed
  metricRegistry.register(MetricRegistry.name("threadpool", "complete_task", "count"), new Gauge[Long] {
    override def getValue: Long = executor.threadPool.getCompletedTaskCount()
  })

  // Gauge for executor thread pool's current number of threads
  metricRegistry.register(MetricRegistry.name("threadpool", "current_pool", "size"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getPoolSize()
  })

  // Gauge got executor thread pool's largest number of threads that have ever simultaneously been in th pool
  metricRegistry.register(MetricRegistry.name("threadpool", "max_pool", "size"), new Gauge[Int] {
    override def getValue: Int = executor.threadPool.getMaximumPoolSize()
  })
}
