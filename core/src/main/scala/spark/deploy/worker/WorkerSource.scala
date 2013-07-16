package spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}

import spark.metrics.source.Source

private[spark] class WorkerSource(val worker: Worker) extends Source {
  val sourceName = "worker"
  val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("executors", "number"), new Gauge[Int] {
    override def getValue: Int = worker.executors.size
  })

  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name("coresUsed", "number"), new Gauge[Int] {
    override def getValue: Int = worker.coresUsed
  })

  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name("memUsed", "MBytes"), new Gauge[Int] {
    override def getValue: Int = worker.memoryUsed
  })

  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name("coresFree", "number"), new Gauge[Int] {
    override def getValue: Int = worker.coresFree
  })

  // Gauge for memory free of this worker
  metricRegistry.register(MetricRegistry.name("memFree", "MBytes"), new Gauge[Int] {
    override def getValue: Int = worker.memoryFree
  })
}
