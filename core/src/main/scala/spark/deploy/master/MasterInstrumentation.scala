package spark.deploy.master

import com.codahale.metrics.{Gauge, JmxReporter, MetricRegistry}

import spark.metrics.AbstractInstrumentation

private[spark] trait MasterInstrumentation extends AbstractInstrumentation {
  var masterInst: Option[Master] = None
  val metricRegistry = new MetricRegistry()
  
  override def registryHandler = metricRegistry
  
  override def instance = "master"
  
  def initialize(master: Master) {
    masterInst = Some(master)
    
    // Register and start all the sinks
    registerSinks
  }
  
  def uninitialize() {
    unregisterSinks
  }
  
  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name(classOf[Master], "workers", "number"), 
  	new Gauge[Int] {
      override def getValue: Int = masterInst.map(_.workers.size).getOrElse(0)
  })
  
  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name(classOf[Master], "apps", "number"),
    new Gauge[Int] {
	  override def getValue: Int = masterInst.map(_.apps.size).getOrElse(0)
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name(classOf[Master], "waiting_apps", "number"), 
    new Gauge[Int] {
      override def getValue: Int = masterInst.map(_.waitingApps.size).getOrElse(0)
  })
  
}