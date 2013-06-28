package spark.metrics.sink

import java.util.Properties

import com.codahale.metrics.{JmxReporter, MetricRegistry}

class JmxSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()
  
  override def start() {
    reporter.start()
  }
  
  override def stop() {
    reporter.stop()
  }
  
}
