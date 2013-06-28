package spark.metrics.sink

trait Sink {
  def start: Unit
  def stop: Unit
}