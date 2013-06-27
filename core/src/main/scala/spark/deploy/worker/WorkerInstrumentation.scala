package spark.deploy.worker

import com.codahale.metrics.{Gauge, Metric}

import java.util.{Map, HashMap => JHashMap}

import spark.metrics.source.Source

private[spark] class WorkerInstrumentation(val worker: Worker) extends Source {
  val className = classOf[Worker].getName()
  
  override def sourceName = "worker"
    
  override def getMetrics: Map[String, Metric] = {
    val gauges = new JHashMap[String, Metric]
    
    // Gauge for executors number
    gauges.put(className + ".executor.number", new Gauge[Int]{
      override def getValue: Int = worker.executors.size
    })
    
    gauges.put(className + ".core_used.number", new Gauge[Int]{
      override def getValue: Int = worker.coresUsed
    })
    
    gauges.put(className + ".mem_used.MBytes", new Gauge[Int]{
      override def getValue: Int = worker.memoryUsed
    })
    
    gauges.put(className + ".core_free.number", new Gauge[Int]{
      override def getValue: Int = worker.coresFree
    })
    
    gauges.put(className + ".mem_free.MBytes", new Gauge[Int]{
      override def getValue: Int = worker.memoryFree
    })
    
    gauges
  }
}
//private[spark] trait WorkerInstrumentation extends AbstractInstrumentation {
//  var workerInst: Option[Worker] = None
//  val metricRegistry = new MetricRegistry()
//  
//  override def registryHandler = metricRegistry
//  
//  override def instance = "worker"
//  
//  def initialize(worker: Worker) {
//    workerInst = Some(worker)
//
//    registerSources()
//    registerSinks()
//  }
//  
//  def uninitialize() {
//    unregisterSinks()
//  }
//  
//  // Gauge for executors number
//  metricRegistry.register(MetricRegistry.name(classOf[Worker], "executor", "number"), 
//    new Gauge[Int] {
//      override def getValue: Int = workerInst.map(_.executors.size).getOrElse(0)
//  })
//  
//  // Gauge for cores used of this worker
//  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_used", "number"), 
//    new Gauge[Int] {
//      override def getValue: Int = workerInst.map(_.coresUsed).getOrElse(0)
//  })
//  
//  // Gauge for memory used of this worker
//  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_used", "MBytes"), 
//    new Gauge[Int] {
//      override def getValue: Int = workerInst.map(_.memoryUsed).getOrElse(0)
//  })
//  
//  // Gauge for cores free of this worker
//  metricRegistry.register(MetricRegistry.name(classOf[Worker], "core_free", "number"), 
//    new Gauge[Int] {
//      override def getValue: Int = workerInst.map(_.coresFree).getOrElse(0)
//  })
//  
//  // Gauge for memory used of this worker
//  metricRegistry.register(MetricRegistry.name(classOf[Worker], "mem_free", "MBytes"), 
//    new Gauge[Int] {
//      override def getValue: Int = workerInst.map(_.memoryFree).getOrElse(0)
//  })
//}