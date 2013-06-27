package spark.metrics.source

trait Source {
  def registerSource: Unit
}