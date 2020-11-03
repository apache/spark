
package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[spark] class MasterSource(val master: Master) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "master"

  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
    override def getValue: Int = master.workers.size
  })

  // Gauge for alive worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("aliveWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.ALIVE)
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.WAITING)
  })
}
