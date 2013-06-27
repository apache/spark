package spark.deploy.worker

import com.codahale.metrics.{JmxReporter, Gauge, MetricRegistry}

import spark.metrics.AbstractInstrumentation

private[spark] trait WorkerInstrumentation extends AbstractInstrumentation {
  var workerInst: Option[Worker] = None
  val metricRegistry = new MetricRegistry()
  
  override def registryHandler = metricRegistry
  
  override def instance = "worker"
  
  def initialize(worker: Worker) {
    workerInst = Some(worker)
    
    // Register all the sources
    registerSources()
    
    // Register and start all the sinks
    registerSinks()
  }
  
  def uninitialize() {
    unregisterSinks()
  }
  
  // Gauge for executors number
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "executor", "number"), 
    new Gauge[Int] {
      override def getValue: Int = workerInst.map(_.executors.size).getOrElse(0)
  })
  
  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_used", "number"), 
    new Gauge[Int] {
      override def getValue: Int = workerInst.map(_.coresUsed).getOrElse(0)
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_used", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = workerInst.map(_.memoryUsed).getOrElse(0)
  })
  
  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_free", "number"), 
    new Gauge[Int] {
      override def getValue: Int = workerInst.map(_.coresFree).getOrElse(0)
  })
  
  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_free", "MBytes"), 
    new Gauge[Int] {
      override def getValue: Int = workerInst.map(_.memoryFree).getOrElse(0)
  })
}