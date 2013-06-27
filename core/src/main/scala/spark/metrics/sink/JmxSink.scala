package spark.metrics.sink

import com.codahale.metrics.{JmxReporter, MetricRegistry}

class JmxSink(registry: MetricRegistry) extends Sink {
  var reporter: JmxReporter = _
  
  override def start() {
    reporter = JmxReporter.forRegistry(registry).build()
    reporter.start()
  }
  
  override def stop() {
    reporter.stop()
  }
  
}