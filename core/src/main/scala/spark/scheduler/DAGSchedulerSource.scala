package spark.scheduler

import com.codahale.metrics.{Gauge,MetricRegistry}

import spark.metrics.source.Source

private[spark] class DAGSchedulerSource(val dagScheduler: DAGScheduler) extends Source {
  val metricRegistry = new MetricRegistry()    
  val sourceName = "DAGScheduler"

  
  metricRegistry.register(MetricRegistry.name("stage","failedStage"), new  Gauge[Int] {
    override def getValue: Int = dagScheduler.failed.size 
  })

  metricRegistry.register(MetricRegistry.name("stage","runningStage"), new Gauge[Int] {
      override def getValue: Int = dagScheduler.running.size 
  })

  metricRegistry.register(MetricRegistry.name("stage","waitingStage"), new Gauge[Int] {
      override def getValue: Int = dagScheduler.waiting.size 
  })

  metricRegistry.register(MetricRegistry.name("job","allJobs"), new Gauge[Int] {
      override def getValue: Int = dagScheduler.nextRunId.get()
  })

  metricRegistry.register(MetricRegistry.name("job","ActiveJobs"), new Gauge[Int] {
      override def getValue: Int = dagScheduler.activeJobs.size
  })
}
