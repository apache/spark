package spark.metrics.sink

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import spark.metrics.AbstractInstrumentation

class CsvSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val pollPeriod = Option(property.getProperty(CsvSink.CSV_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CsvSink.CSV_DEFAULT_PERIOD.toInt
  }
  
  val pollUnit = Option(property.getProperty(CsvSink.CSV_KEY_UNIT)) match {
    case Some(s) => AbstractInstrumentation.timeUnits(s)
    case None => AbstractInstrumentation.timeUnits(CsvSink.CSV_DEFAULT_UNIT)
  }
  
  val pollDir = Option(property.getProperty(CsvSink.CSV_KEY_DIR)) match {
    case Some(s) => s
    case None => CsvSink.CSV_DEFAULT_DIR
  }
  
  var reporter: CsvReporter = _
  
  override def registerSink() {
    reporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(pollDir))
      
    reporter.start(pollPeriod, pollUnit)  
  }
  
  override def unregisterSink() {
    reporter.stop()
  }
}

object CsvSink {
  val CSV_KEY_PERIOD = "period"
  val CSV_KEY_UNIT = "unit"
  val CSV_KEY_DIR = "directory"
    
  val CSV_DEFAULT_PERIOD = "10"
  val CSV_DEFAULT_UNIT = "second"
  val CSV_DEFAULT_DIR = "/tmp/"
}

