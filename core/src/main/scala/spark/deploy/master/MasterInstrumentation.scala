package spark.deploy.master

import java.util.{Map, HashMap => JHashMap} 

import com.codahale.metrics.{Gauge, Metric}

import com.codahale.metrics.{JmxReporter, MetricSet, MetricRegistry}

import spark.metrics.source.Source
import spark.Logging

private[spark] class MasterInstrumentation(val master: Master) extends Source {
  val className = classOf[Master].getName()
  val instrumentationName = "master"
  val metricRegistry = new MetricRegistry()    
  val sourceName = instrumentationName

  metricRegistry.register(MetricRegistry.name("workers","number"), 
       new Gauge[Int] {
      override def getValue: Int = master.workers.size
  })
  
  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps", "number"),
    new Gauge[Int] {
         override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waiting_apps", "number"), 
    new Gauge[Int] {
      override def getValue: Int = master.waitingApps.size
  })

}
