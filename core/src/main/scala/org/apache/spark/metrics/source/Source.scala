
package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

private[spark] trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry
}
