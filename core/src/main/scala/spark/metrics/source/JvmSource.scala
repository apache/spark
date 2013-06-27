package spark.metrics.source

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{MemoryUsageGaugeSet, GarbageCollectorMetricSet}

class JvmSource(registry: MetricRegistry) extends Source {
  // Initialize memory usage gauge for jvm
  val memUsageMetricSet = new MemoryUsageGaugeSet
  
  // Initialize garbage collection usage gauge for jvm
  val gcMetricSet = new GarbageCollectorMetricSet
  
  override def registerSource() {
    registry.registerAll(memUsageMetricSet)
    registry.registerAll(gcMetricSet)
  }
}