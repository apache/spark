package spark.metrics

import scala.collection.mutable

import com.codahale.metrics.{JmxReporter, MetricSet, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import spark.Logging
import spark.metrics.sink._
import spark.metrics.source._

private[spark] class MetricsSystem private (val instance: String) extends Logging {
  initLogging()
  
  val confFile = System.getProperty("spark.metrics.conf.file", MetricsConfig.DEFAULT_CONFIG_FILE)
  val metricsConfig = new MetricsConfig(confFile)
  
  val sinks = new mutable.ArrayBuffer[Sink]
  val sources = new mutable.ArrayBuffer[Source]
  
  def start() {
    registerSources()
    registerSinks()
  }
   
  def stop() {
    sinks.foreach(_.stop)
  }
  
  def registerSource(source: Source) {
    sources += source
    MetricsSystem.registry.registerAll(source.asInstanceOf[MetricSet])
  }
  
  def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = MetricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)
    
    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Class.forName(classPath).newInstance()
        sources += source.asInstanceOf[Source]
        MetricsSystem.registry.registerAll(source.asInstanceOf[MetricSet])
      } catch {
        case e: Exception => logError("source class " + classPath + " cannot be instantialized", e)
      }
    }
  }
  
  def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = MetricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)
    
    // Register JMX sink as a default sink
    sinks += new JmxSink(MetricsSystem.registry)
    
    // Register other sinks according to conf
    sinkConfigs.foreach { kv =>
      val classPath = if (MetricsSystem.DEFAULT_SINKS.contains(kv._1)) {
        MetricsSystem.DEFAULT_SINKS(kv._1)
      } else {
        // For non-default sink, a property class should be set and create using reflection
        kv._2.getProperty("class")
      }
      try {
        val sink = Class.forName(classPath).getConstructor(classOf[Properties], classOf[MetricRegistry])
          .newInstance(kv._2, MetricsSystem.registry)
        sinks += sink.asInstanceOf[Sink]
      } catch {
        case e: Exception => logError("sink class " + classPath + " cannot be instantialized", e)
      }
    }
    sinks.foreach(_.start)
  }
}

private[spark] object MetricsSystem {
  val registry = new MetricRegistry()
  
  val DEFAULT_SINKS = Map(
      "console" -> "spark.metrics.sink.ConsoleSink",
      "csv" -> "spark.metrics.sink.CsvSink")
      
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r
  
  val timeUnits = Map(
      "millisecond" -> TimeUnit.MILLISECONDS,
      "second" -> TimeUnit.SECONDS,
      "minute" -> TimeUnit.MINUTES,
      "hour" -> TimeUnit.HOURS,
      "day" -> TimeUnit.DAYS)
      
   def createMetricsSystem(instance: String) = new MetricsSystem(instance)
}