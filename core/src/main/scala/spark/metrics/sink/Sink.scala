package spark.metrics.sink

trait Sink {
  def registerSink: Unit
  
  def unregisterSink: Unit
}