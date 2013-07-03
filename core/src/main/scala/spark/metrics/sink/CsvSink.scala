package spark.metrics.sink

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import spark.metrics.MetricsSystem

class CsvSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val CSV_KEY_PERIOD = "period"
  val CSV_KEY_UNIT = "unit"
  val CSV_KEY_DIR = "directory"

  val CSV_DEFAULT_PERIOD = "10"
  val CSV_DEFAULT_UNIT = "second"
  val CSV_DEFAULT_DIR = "/tmp/"

  val pollPeriod = Option(property.getProperty(CSV_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CSV_DEFAULT_PERIOD.toInt
  }

  val pollUnit = Option(property.getProperty(CSV_KEY_UNIT)) match {
    case Some(s) => MetricsSystem.timeUnits(s)
    case None => MetricsSystem.timeUnits(CSV_DEFAULT_UNIT)
  }

  val pollDir = Option(property.getProperty(CSV_KEY_DIR)) match {
    case Some(s) => s
    case None => CSV_DEFAULT_DIR
  }

  val reporter: CsvReporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(pollDir))

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }
}

