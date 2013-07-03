package spark.metrics.sink

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import spark.metrics.MetricsSystem

class ConsoleSink(val property: Properties, val registry: MetricRegistry) extends Sink {

  val CONSOLE_DEFAULT_PERIOD = "10"
  val CONSOLE_DEFAULT_UNIT = "second"

  val CONSOLE_KEY_PERIOD = "period"
  val CONSOLE_KEY_UNIT = "unit"

  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CONSOLE_DEFAULT_PERIOD.toInt
  }

  val pollUnit = Option(property.getProperty(CONSOLE_KEY_UNIT)) match {
    case Some(s) => MetricsSystem.timeUnits(s)
    case None => MetricsSystem.timeUnits(CONSOLE_DEFAULT_UNIT)
  }

  val reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }
}

