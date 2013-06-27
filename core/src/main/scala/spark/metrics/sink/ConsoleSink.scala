package spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

import spark.metrics.MetricsSystem

class ConsoleSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val pollPeriod = Option(property.getProperty(ConsoleSink.CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => ConsoleSink.CONSOLE_DEFAULT_PERIOD.toInt
  }
  
  val pollUnit = Option(property.getProperty(ConsoleSink.CONSOLE_KEY_UNIT)) match {
    case Some(s) => MetricsSystem.timeUnits(s)
    case None => MetricsSystem.timeUnits(ConsoleSink.CONSOLE_DEFAULT_UNIT)
  }
  
  var reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
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

object ConsoleSink {
  val CONSOLE_DEFAULT_PERIOD = "10"
  val CONSOLE_DEFAULT_UNIT = "second"
    
  val CONSOLE_KEY_PERIOD = "period"
  val CONSOLE_KEY_UNIT = "unit"
}
