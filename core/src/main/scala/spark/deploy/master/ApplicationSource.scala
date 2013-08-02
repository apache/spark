package spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import spark.metrics.source.Source

class ApplicationSource(val application: ApplicationInfo) extends Source {
  val metricRegistry = new MetricRegistry()
  val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())

  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    override def getValue: String = application.state.toString
  })

  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    override def getValue: Long = application.duration
  })

  metricRegistry.register(MetricRegistry.name("cores", "number"), new Gauge[Int] {
    override def getValue: Int = application.coresGranted
  })

}
