package spark.metrics.source

import com.codahale.metrics.MetricSet
import com.codahale.metrics.MetricRegistry

trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry 
}
