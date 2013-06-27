package spark.metrics.sink

import com.codahale.metrics.{JmxReporter, MetricRegistry}

class JmxSink(registry: MetricRegistry) extends Sink {
  var reporter: JmxReporter = JmxReporter.forRegistry(registry).build()
  
  override def start() {
    reporter.start()
  }
  
  override def stop() {
    reporter.stop()
  }
  
}
