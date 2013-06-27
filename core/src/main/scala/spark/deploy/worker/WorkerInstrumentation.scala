package spark.deploy.worker

import com.codahale.metrics.{Gauge, Metric}

import java.util.{Map, HashMap => JHashMap}

import com.codahale.metrics.{JmxReporter, MetricSet, MetricRegistry}
import spark.metrics.source.Source

private[spark] class WorkerInstrumentation(val worker: Worker) extends Source {
  val className = classOf[Worker].getName()
  
  val sourceName = "worker"
  
  val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name(classOf[Worker], "executor", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.executors.size
  })
  
  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_used", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.coresUsed
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_used", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = worker.memoryUsed
  })
  
  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_free", "number"), 
    new Gauge[Int] {
      override def getValue: Int = worker.coresFree
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_free", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = worker.memoryFree
    })
}
