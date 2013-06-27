package spark.metrics

import scala.collection.mutable

import com.codahale.metrics.{JmxReporter, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import spark.Logging
import spark.metrics.sink._

trait AbstractInstrumentation extends Logging {
  initLogging()
  
  def registryHandler: MetricRegistry
  def instance: String
  
  val confFile = System.getProperty("spark.metrics.conf.file", MetricsConfig.DEFAULT_CONFIG_FILE)
  val metricsConfig = new MetricsConfig(confFile)
  
  val sinks = new mutable.ArrayBuffer[Sink]
  
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
        kv._2.getProperty("class")
      }
      try {
        val sink = Class.forName(classPath).getConstructor(classOf[Properties], classOf[MetricRegistry])
          .newInstance(kv._2, registryHandler)
        sinks += sink.asInstanceOf[Sink]
      } catch {
        case e: Exception => logError("class " + classPath + "cannot be instantialize", e)
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
  
  val timeUnits = Map(
      "millisecond" -> TimeUnit.MILLISECONDS,
      "second" -> TimeUnit.SECONDS,
      "minute" -> TimeUnit.MINUTES,
      "hour" -> TimeUnit.HOURS,
      "day" -> TimeUnit.DAYS)
}