package spark.scheduler

import com.codahale.metrics.{Gauge,MetricRegistry}

import spark.metrics.source.Source

private[spark] class DAGSchedulerSource(val dagScheduler: DAGScheduler) extends Source {
  val metricRegistry = new MetricRegistry()
  val sourceName = "DAGScheduler"

  metricRegistry.register(MetricRegistry.name("stage", "failedStages", "number"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.failed.size
  })

  metricRegistry.register(MetricRegistry.name("stage", "runningStages", "number"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.running.size
  })

  metricRegistry.register(MetricRegistry.name("stage", "waitingStages", "number"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.waiting.size
  })

  metricRegistry.register(MetricRegistry.name("job", "allJobs", "number"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.nextRunId.get()
  })

  metricRegistry.register(MetricRegistry.name("job", "activeJobs", "number"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.activeJobs.size
  })
}
