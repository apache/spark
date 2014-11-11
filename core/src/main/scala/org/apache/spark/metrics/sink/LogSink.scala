package org.apache.spark.metrics.sink

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.shopify.metrics.reporting.LogReporter

import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

private[spark] class LogSink(val property: Properties, val registry: MetricRegistry,
  securityMgr: SecurityManager) extends Sink {
  
    val LOG_KEY_PERIOD = "period"
    val LOG_KEY_UNIT = "unit"
    val LOG_KEY_DIR  = "directory"

    val LOG_DEFAULT_PERIOD = 10
    val LOG_DEFAULT_UNIT = "SECONDS"
    val LOG_DEFAULT_DIR = "/tmp"

    val pollPeriod = Option(property.getProperty(LOG_KEY_PERIOD)) match {
      case Some(s) => s.toInt
      case None => LOG_DEFAULT_PERIOD
    }

    val pollUnit: TimeUnit = Option(property.getProperty(LOG_KEY_UNIT)) match {
      case Some(s) => TimeUnit.valueOf(s.toUpperCase())
      case None => TimeUnit.valueOf(LOG_KEY_UNIT)
    }

    MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

    val pollDir = Option(property.getProperty(LOG_KEY_DIR)) match {
      case Some(s) => s
      case None => LOG_DEFAULT_DIR
    }
    
    val reporter: LogReporter = LogReporter.forRegistry(registry)
        .formatFor(Locale.US)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(pollDir)

    override def start() {
      reporter.start(pollPeriod, pollUnit)
    }

    override def stop() {
      reporter.stop()
    }
    
    override def report() {
      reporter.report()
    }
  }
