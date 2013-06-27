package spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

import spark.metrics.AbstractInstrumentation

class ConsoleSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val pollPeriod = Option(property.getProperty(ConsoleSink.CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => ConsoleSink.CONSOLE_DEFAULT_PERIOD.toInt
  }
  
  val pollUnit = Option(property.getProperty(ConsoleSink.CONSOLE_KEY_UNIT)) match {
    case Some(s) => AbstractInstrumentation.timeUnits(s)
    case None => AbstractInstrumentation.timeUnits(ConsoleSink.CONSOLE_DEFAULT_UNIT)
  }
  
  var reporter: ConsoleReporter = _
  
  override def registerSink() {
    reporter = ConsoleReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
      
    reporter.start(pollPeriod, pollUnit)  
  }
  
  override def unregisterSink() {
    reporter.stop()
  }
}

object ConsoleSink {
  val CONSOLE_DEFAULT_PERIOD = "10"
  val CONSOLE_DEFAULT_UNIT = "second"
    
  val CONSOLE_KEY_PERIOD = "period"
  val CONSOLE_KEY_UNIT = "unit"
}