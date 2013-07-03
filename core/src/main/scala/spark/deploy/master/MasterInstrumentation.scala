package spark.deploy.master

import com.codahale.metrics.{Gauge,MetricRegistry}

import spark.metrics.source.Source

private[spark] class MasterInstrumentation(val master: Master) extends Source {
  val metricRegistry = new MetricRegistry()
  val sourceName = "master"

  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("workers","number"), new Gauge[Int] {
      override def getValue: Int = master.workers.size
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps", "number"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waiting_apps", "number"), new Gauge[Int] {
    override def getValue: Int = master.waitingApps.size
  })
}
