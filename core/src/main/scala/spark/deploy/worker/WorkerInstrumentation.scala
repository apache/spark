package spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}

import spark.metrics.source.Source

private[spark] class WorkerInstrumentation(val worker: Worker) extends Source {
  val sourceName = "worker"
  val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("executor", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.executors.size
  })
  
  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name("core_used", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.coresUsed
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name("mem_used", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = worker.memoryUsed
  })
  
  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name("core_free", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.coresFree
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name("mem_free", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = worker.memoryFree
    })
}
