package spark.metrics.source

import java.util.{Map, HashMap => JHashMap}

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{GarbageCollectorMetricSet, MemoryUsageGaugeSet}

class JvmSource extends Source {
  val sourceName = "jvm"
  val metricRegistry = new MetricRegistry()
  
    val gcMetricSet = new GarbageCollectorMetricSet
    val memGaugeSet = new MemoryUsageGaugeSet
    
    metricRegistry.registerAll(gcMetricSet)
    metricRegistry.registerAll(memGaugeSet)
}
