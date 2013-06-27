package spark.metrics.source

import java.util.{Map, HashMap => JHashMap}

import com.codahale.metrics.Metric
import com.codahale.metrics.jvm.{GarbageCollectorMetricSet, MemoryUsageGaugeSet}

class JvmSource extends Source {
  override def sourceName = "jvm"
    
  override def getMetrics(): Map[String, Metric] = {
    val gauges = new JHashMap[String, Metric]
    
    import scala.collection.JavaConversions._
    val gcMetricSet = new GarbageCollectorMetricSet
    gcMetricSet.getMetrics.foreach(kv => gauges.put(kv._1, kv._2))
    
    val memGaugeSet = new MemoryUsageGaugeSet
    memGaugeSet.getMetrics.foreach(kv => gauges.put(kv._1, kv._2))
    		
    gauges
  }
}