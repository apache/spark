package spark.metrics

import scala.collection.mutable

import com.codahale.metrics.{JmxReporter, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import spark.Logging
import spark.metrics.sink._
import spark.metrics.source._

private [spark] trait AbstractInstrumentation extends Logging {
  initLogging()
  
  // Get MetricRegistry handler
  def registryHandler: MetricRegistry
  // Get the instance name
  def instance: String
  
  val confFile = System.getProperty("spark.metrics.conf.file", MetricsConfig.DEFAULT_CONFIG_FILE)
  val metricsConfig = new MetricsConfig(confFile)
  
  val sinks = new mutable.ArrayBuffer[Sink]
  val sources = new mutable.ArrayBuffer[Source]
  
  def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = MetricsConfig.subProperties(instConfig, AbstractInstrumentation.SOURCE_REGEX)
    
    // Register all the sources
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Class.forName(classPath).getConstructor(classOf[MetricRegistry])
          .newInstance(registryHandler)
        sources += source.asInstanceOf[Source]
      } catch {
        case e: Exception => logError("source class " + classPath + " cannot be instantialized", e)
      }
    }
    sources.foreach(_.registerSource)
  }
  
  def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = MetricsConfig.subProperties(instConfig, AbstractInstrumentation.SINK_REGEX)
    
    // Register JMX sink as a default sink
    sinks += new JmxSink(registryHandler)
    
    // Register other sinks according to conf
    sinkConfigs.foreach { kv =>
      val classPath = if (AbstractInstrumentation.DEFAULT_SINKS.contains(kv._1)) {
        AbstractInstrumentation.DEFAULT_SINKS(kv._1)
      } else {
        // For non-default sink, a property class should be set and create using reflection
        kv._2.getProperty("class")
      }
      try {
        val sink = Class.forName(classPath).getConstructor(classOf[Properties], classOf[MetricRegistry])
          .newInstance(kv._2, registryHandler)
        sinks += sink.asInstanceOf[Sink]
      } catch {
        case e: Exception => logError("sink class " + classPath + " cannot be instantialized", e)
      }
    }
    sinks.foreach(_.registerSink)
  }
  
  def unregisterSinks() {
    sinks.foreach(_.unregisterSink)
  }
}

object AbstractInstrumentation {
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
}