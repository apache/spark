package spark.metrics

import com.codahale.metrics.{JmxReporter, MetricSet, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import spark.Logging
import spark.metrics.sink.Sink
import spark.metrics.source.Source

private[spark] class MetricsSystem private (val instance: String) extends Logging {
  initLogging()

  val confFile = System.getProperty("spark.metrics.conf.file", "unsupported")
  val metricsConfig = new MetricsConfig(confFile)

  val sinks = new mutable.ArrayBuffer[Sink]
  val sources = new mutable.ArrayBuffer[Source]
  val registry = new MetricRegistry()

  val DEFAULT_SINKS = Map("jmx" -> "spark.metrics.sink.JmxSink")

  metricsConfig.initilize()
  registerSources()
  registerSinks()

  def start() {
    sinks.foreach(_.start)
  }

  def stop() {
    sinks.foreach(_.stop)
  }

  def registerSource(source: Source) {
    sources += source
    registry.register(source.sourceName, source.metricRegistry)
  }

  def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Class.forName(classPath).newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("source class " + classPath + " cannot be instantialized", e)
      }
    }
  }

  def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = if (DEFAULT_SINKS.contains(kv._1)) {
        DEFAULT_SINKS(kv._1)
      } else {
        // For non-default sink, a property class should be set and create using reflection
        kv._2.getProperty("class")
      }
      try {
        val sink = Class.forName(classPath).getConstructor(classOf[Properties], classOf[MetricRegistry])
          .newInstance(kv._2, registry)
        sinks += sink.asInstanceOf[Sink]
      } catch {
        case e: Exception => logError("sink class " + classPath + " cannot be instantialized", e)
      }
    }
  }
}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r
  val timeUnits = Map(
      "illisecond" -> TimeUnit.MILLISECONDS,
      "second" -> TimeUnit.SECONDS,
      "minute" -> TimeUnit.MINUTES,
      "hour" -> TimeUnit.HOURS,
      "day" -> TimeUnit.DAYS)

   def createMetricsSystem(instance: String): MetricsSystem = new MetricsSystem(instance)
}
