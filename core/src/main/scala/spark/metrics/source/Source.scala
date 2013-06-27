package spark.metrics.source

import com.codahale.metrics.MetricSet

trait Source extends MetricSet {
  def sourceName: String
}