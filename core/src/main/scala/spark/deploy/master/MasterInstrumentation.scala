package spark.deploy.master

import java.util.{Map, HashMap => JHashMap} 

import com.codahale.metrics.{Gauge, Metric}

import spark.metrics.source.Source

private[spark] class MasterInstrumentation(val master: Master) extends Source {
  val className = classOf[Master].getName()
  val instrumentationName = "master"
    
  override def sourceName = instrumentationName
  
  override def getMetrics(): Map[String, Metric] = {
    val gauges = new JHashMap[String, Metric]
    
    // Gauge for worker numbers in cluster
    gauges.put(className + ".workers.number", new Gauge[Int] {
      override def getValue: Int = master.workers.size
    })
    
    // Gauge for application numbers in cluster
    gauges.put(className + ".apps.number", new Gauge[Int] {
      override def getValue: Int = master.apps.size
    })
    
    // Gauge for waiting application numbers in cluster
    gauges.put(className + ".waiting_apps.number", new Gauge[Int] {
      override def getValue: Int = master.waitingApps.size
    })
    
    gauges
  }
}