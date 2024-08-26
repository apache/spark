/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.metrics.sink.opentelemetry

import java.nio.file.{Files, Paths}
import java.util.{Locale, SortedMap}
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{DoubleGauge, DoubleHistogram, LongCounter}
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource

private[spark] class OpenTelemetryPushReporter(
    registry: MetricRegistry,
    pollInterval: Int = 10,
    pollUnit: TimeUnit = TimeUnit.SECONDS,
    host: String = "http://localhost",
    port: String = "4317",
    headersMap: Map[String, String] = Map(),
    attributesMap: Map[String, String] = Map(),
    trustedCertificatesPath: String,
    privateKeyPemPath: String,
    certificatePemPath: String)
  extends ScheduledReporter (
      registry,
      "opentelemetry-push-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS)
  with MetricRegistryListener {

  val FIFTEEN_MINUTE_RATE = "_fifteen_minute_rate"
  val FIVE_MINUTE_RATE = "_five_minute_rate"
  val ONE_MINUTE_RATE = "_one_minute_rate"
  val MEAN_RATE = "_mean_rate"
  val METER = "_meter"
  val TIMER = "_timer"
  val COUNT = "_count"
  val MAX = "_max"
  val MIN = "_min"
  val MEAN = "_mean"
  val MEDIAN = "_50_percentile"
  val SEVENTY_FIFTH_PERCENTILE = "_75_percentile"
  val NINETY_FIFTH_PERCENTILE = "_95_percentile"
  val NINETY_EIGHTH_PERCENTILE = "_98_percentile"
  val NINETY_NINTH_PERCENTILE = "_99_percentile"
  val NINE_HUNDRED_NINETY_NINTH_PERCENTILE = "_999_percentile"
  val STD_DEV = "_std_dev"

  val otlpGrpcMetricExporterBuilder = OtlpGrpcMetricExporter.builder()

  for ((key, value) <- headersMap) {
    otlpGrpcMetricExporterBuilder.addHeader(key, value)
  }

  if (trustedCertificatesPath != null) {
    otlpGrpcMetricExporterBuilder
      .setTrustedCertificates(Files.readAllBytes(Paths.get(trustedCertificatesPath)))
  }

  if (privateKeyPemPath != null && certificatePemPath != null) {
    otlpGrpcMetricExporterBuilder
      .setClientTls(
        Files.readAllBytes(Paths.get(privateKeyPemPath)),
        Files.readAllBytes(Paths.get(certificatePemPath)))
  }

  val grpcEndpoint = host + ":" + port
  otlpGrpcMetricExporterBuilder.setEndpoint(grpcEndpoint)

  val arrtributesBuilder = Attributes.builder()
  for ((key, value) <- attributesMap) {
    arrtributesBuilder.put(key, value)
  }

  val resource = Resource
    .getDefault()
    .merge(Resource.create(arrtributesBuilder.build()));

  val metricReader = PeriodicMetricReader
    .builder(otlpGrpcMetricExporterBuilder.build())
    .setInterval(pollInterval, pollUnit)
    .build()

  val sdkMeterProvider: SdkMeterProvider = SdkMeterProvider
    .builder()
    .registerMetricReader(metricReader)
    .setResource(resource)
    .build()

  val openTelemetryCounters = collection.mutable.Map[String, LongCounter]()
  val openTelemetryHistograms = collection.mutable.Map[String, DoubleHistogram]()
  val openTelemetryGauges = collection.mutable.Map[String, DoubleGauge]()
  val codahaleCounters = collection.mutable.Map[String, Counter]()
  val openTelemetry = OpenTelemetrySdk
    .builder()
    .setMeterProvider(sdkMeterProvider)
    .build();
  val openTelemetryMeter = openTelemetry.getMeter("apache-spark")

  override def report(
      gauges: SortedMap[String, Gauge[_]],
      counters: SortedMap[String, Counter],
      histograms: SortedMap[String, Histogram],
      meters: SortedMap[String, Meter],
      timers: SortedMap[String, Timer]): Unit = {
    counters.forEach(this.reportCounter)
    gauges.forEach(this.reportGauges)
    histograms.forEach(this.reportHistograms)
    meters.forEach(this.reportMeters)
    timers.forEach(this.reportTimers)
    sdkMeterProvider.forceFlush
  }

  override def onGaugeAdded(name: String, gauge: Gauge[_]): Unit = {
    val metricName = normalizeMetricName(name)
    generateGauge(metricName)
  }

  override def onGaugeRemoved(name: String): Unit = {
    val metricName = normalizeMetricName(name)
    openTelemetryGauges.remove(metricName)
  }

  override def onCounterAdded(name: String, counter: Counter): Unit = {
    val metricName = normalizeMetricName(name)
    val addedOpenTelemetryCounter =
      openTelemetryMeter.counterBuilder(normalizeMetricName(metricName)).build
    openTelemetryCounters.put(metricName, addedOpenTelemetryCounter)
    codahaleCounters.put(metricName, registry.counter(metricName))
  }

  override def onCounterRemoved(name: String): Unit = {
    val metricName = normalizeMetricName(name)
    openTelemetryCounters.remove(metricName)
    codahaleCounters.remove(metricName)
  }

  override def onHistogramAdded(name: String, histogram: Histogram): Unit = {
    val metricName = normalizeMetricName(name)
    generateHistogramGroup(metricName)
  }

  override def onHistogramRemoved(name: String): Unit = {
    val metricName = normalizeMetricName(name)
    cleanHistogramGroup(metricName)
  }

  override def onMeterAdded(name: String, meter: Meter): Unit = {
    val metricName = normalizeMetricName(name) + METER
    generateGauge(metricName + COUNT)
    generateGauge(metricName + MEAN_RATE)
    generateGauge(metricName + FIFTEEN_MINUTE_RATE)
    generateGauge(metricName + FIVE_MINUTE_RATE)
    generateGauge(metricName + ONE_MINUTE_RATE)
  }

  override def onMeterRemoved(name: String): Unit = {
    val metricName = normalizeMetricName(name) + METER
    openTelemetryGauges.remove(metricName + COUNT)
    openTelemetryGauges.remove(metricName + MEAN_RATE)
    openTelemetryGauges.remove(metricName + ONE_MINUTE_RATE)
    openTelemetryGauges.remove(metricName + FIVE_MINUTE_RATE)
    openTelemetryGauges.remove(metricName + FIFTEEN_MINUTE_RATE)
  }

  override def onTimerAdded(name: String, timer: Timer): Unit = {
    val metricName = normalizeMetricName(name) + TIMER
    generateHistogramGroup(metricName)
    generateAdditionalHistogramGroupForTimers(metricName)
  }

  override def onTimerRemoved(name: String): Unit = {
    val metricName = normalizeMetricName(name) + TIMER
    cleanHistogramGroup(name)
    cleanAdditionalHistogramGroupTimers(metricName)
  }

  override def stop(): Unit = {
    super.stop()
    sdkMeterProvider.close()
  }

  private def normalizeMetricName(name: String): String = {
    name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "_")
  }

  private def generateHistogram(metricName: String): Unit = {
    val openTelemetryHistogram =
      openTelemetryMeter.histogramBuilder(metricName).build
    openTelemetryHistograms.put(metricName, openTelemetryHistogram)
  }

  private def generateHistogramGroup(metricName: String): Unit = {
    generateHistogram(metricName + COUNT)
    generateHistogram(metricName + MAX)
    generateHistogram(metricName + MIN)
    generateHistogram(metricName + MEAN)
    generateHistogram(metricName + MEDIAN)
    generateHistogram(metricName + STD_DEV)
    generateHistogram(metricName + SEVENTY_FIFTH_PERCENTILE)
    generateHistogram(metricName + NINETY_FIFTH_PERCENTILE)
    generateHistogram(metricName + NINETY_EIGHTH_PERCENTILE)
    generateHistogram(metricName + NINETY_NINTH_PERCENTILE)
    generateHistogram(metricName + NINE_HUNDRED_NINETY_NINTH_PERCENTILE)
  }

  private def generateAdditionalHistogramGroupForTimers(metricName: String): Unit = {
    generateHistogram(metricName + FIFTEEN_MINUTE_RATE)
    generateHistogram(metricName + FIVE_MINUTE_RATE)
    generateHistogram(metricName + ONE_MINUTE_RATE)
    generateHistogram(metricName + MEAN_RATE)
  }

  private def cleanHistogramGroup(metricName: String): Unit = {
    openTelemetryHistograms.remove(metricName + COUNT)
    openTelemetryHistograms.remove(metricName + MAX)
    openTelemetryHistograms.remove(metricName + MIN)
    openTelemetryHistograms.remove(metricName + MEAN)
    openTelemetryHistograms.remove(metricName + MEDIAN)
    openTelemetryHistograms.remove(metricName + STD_DEV)
    openTelemetryHistograms.remove(metricName + SEVENTY_FIFTH_PERCENTILE)
    openTelemetryHistograms.remove(metricName + NINETY_FIFTH_PERCENTILE)
    openTelemetryHistograms.remove(metricName + NINETY_EIGHTH_PERCENTILE)
    openTelemetryHistograms.remove(metricName + NINETY_NINTH_PERCENTILE)
    openTelemetryHistograms.remove(metricName + NINE_HUNDRED_NINETY_NINTH_PERCENTILE)
  }

  private def cleanAdditionalHistogramGroupTimers(metricName: String): Unit = {
    openTelemetryHistograms.remove(metricName + FIFTEEN_MINUTE_RATE)
    openTelemetryHistograms.remove(metricName + FIVE_MINUTE_RATE)
    openTelemetryHistograms.remove(metricName + ONE_MINUTE_RATE)
    openTelemetryHistograms.remove(metricName + MEAN_RATE)
  }

  private def generateGauge(metricName: String): Unit = {
    val addedOpenTelemetryGauge =
      openTelemetryMeter.gaugeBuilder(normalizeMetricName(metricName)).build
    openTelemetryGauges.put(metricName, addedOpenTelemetryGauge)
  }

  private def reportCounter(name: String, counter: Counter): Unit = {
    val metricName = normalizeMetricName(name)
    val openTelemetryCounter = openTelemetryCounters(metricName)
    val codahaleCounter = codahaleCounters(metricName)
    val diff = counter.getCount - codahaleCounter.getCount
    openTelemetryCounter.add(diff)
    codahaleCounter.inc(diff)
  }

  private def reportGauges(name: String, gauge: Gauge[_]): Unit = {
    val metricName = normalizeMetricName(name)
    gauge.getValue match {
      case d: Double =>
        openTelemetryGauges(metricName).set(d.doubleValue)
      case d: Long =>
        openTelemetryGauges(metricName).set(d.doubleValue)
      case d: Int =>
        openTelemetryGauges(metricName).set(d.doubleValue)
      case _ => ()
    }
  }

  private def reportHistograms(name: String, histogram: Histogram): Unit = {
    val metricName = normalizeMetricName(name)
    reportHistogramGroup(metricName, histogram)
  }

  private def reportMeters(name: String, meter: Meter): Unit = {
    val metricName = normalizeMetricName(name) + METER
    val openTelemetryGaugeCount = openTelemetryGauges(metricName + COUNT)
    openTelemetryGaugeCount.set(meter.getCount.toDouble)
    val openTelemetryGauge0neMinuteRate = openTelemetryGauges(metricName + ONE_MINUTE_RATE)
    openTelemetryGauge0neMinuteRate.set(meter.getOneMinuteRate)
    val openTelemetryGaugeFiveMinuteRate = openTelemetryGauges(metricName + FIVE_MINUTE_RATE)
    openTelemetryGaugeFiveMinuteRate.set(meter.getFiveMinuteRate)
    val openTelemetryGaugeFifteenMinuteRate = openTelemetryGauges(
      metricName + FIFTEEN_MINUTE_RATE)
    openTelemetryGaugeFifteenMinuteRate.set(meter.getFifteenMinuteRate)
    val openTelemetryGaugeMeanRate = openTelemetryGauges(metricName + MEAN_RATE)
    openTelemetryGaugeMeanRate.set(meter.getMeanRate)
  }

  private def reportTimers(name: String, timer: Timer): Unit = {
    val metricName = normalizeMetricName(name) + TIMER
    val openTelemetryHistogramMax = openTelemetryHistograms(metricName + MAX)
    openTelemetryHistogramMax.record(timer.getCount.toDouble)
    val openTelemetryHistogram0neMinuteRate = openTelemetryHistograms(
      metricName + ONE_MINUTE_RATE)
    openTelemetryHistogram0neMinuteRate.record(timer.getOneMinuteRate)
    val openTelemetryHistogramFiveMinuteRate = openTelemetryHistograms(
      metricName + FIVE_MINUTE_RATE)
    openTelemetryHistogramFiveMinuteRate.record(timer.getFiveMinuteRate)
    val openTelemetryHistogramFifteenMinuteRate = openTelemetryHistograms(
      metricName + FIFTEEN_MINUTE_RATE)
    openTelemetryHistogramFifteenMinuteRate.record(timer.getFifteenMinuteRate)
    val openTelemetryHistogramMeanRate = openTelemetryHistograms(metricName + MEAN_RATE)
    openTelemetryHistogramMeanRate.record(timer.getMeanRate)
    val snapshot = timer.getSnapshot
    reportHistogramGroup(metricName, snapshot)
  }

  private def reportHistogramGroup(metricName: String, histogram: Histogram): Unit = {
    val openTelemetryHistogramCount = openTelemetryHistograms(metricName + COUNT)
    openTelemetryHistogramCount.record(histogram.getCount.toDouble)
    val snapshot = histogram.getSnapshot
    reportHistogramGroup(metricName, snapshot)
  }

  private def reportHistogramGroup(metricName: String, snapshot: Snapshot): Unit = {
    val openTelemetryHistogramMax = openTelemetryHistograms(metricName + MAX)
    openTelemetryHistogramMax.record(snapshot.getMax.toDouble)
    val openTelemetryHistogramMin = openTelemetryHistograms(metricName + MIN)
    openTelemetryHistogramMin.record(snapshot.getMin.toDouble)
    val openTelemetryHistogramMean = openTelemetryHistograms(metricName + MEAN)
    openTelemetryHistogramMean.record(snapshot.getMean)
    val openTelemetryHistogramMedian = openTelemetryHistograms(metricName + MEDIAN)
    openTelemetryHistogramMedian.record(snapshot.getMedian)
    val openTelemetryHistogramStdDev = openTelemetryHistograms(metricName + STD_DEV)
    openTelemetryHistogramStdDev.record(snapshot.getStdDev)
    val openTelemetryHistogram75Percentile = openTelemetryHistograms(
      metricName + SEVENTY_FIFTH_PERCENTILE)
    openTelemetryHistogram75Percentile.record(snapshot.get75thPercentile)
    val openTelemetryHistogram95Percentile = openTelemetryHistograms(
      metricName + NINETY_FIFTH_PERCENTILE)
    openTelemetryHistogram95Percentile.record(snapshot.get95thPercentile)
    val openTelemetryHistogram98Percentile = openTelemetryHistograms(
      metricName + NINETY_EIGHTH_PERCENTILE)
    openTelemetryHistogram98Percentile.record(snapshot.get98thPercentile)
    val openTelemetryHistogram99Percentile = openTelemetryHistograms(
      metricName + NINETY_NINTH_PERCENTILE)
    openTelemetryHistogram99Percentile.record(snapshot.get99thPercentile)
    val openTelemetryHistogram999Percentile = openTelemetryHistograms(
      metricName + NINE_HUNDRED_NINETY_NINTH_PERCENTILE)
    openTelemetryHistogram999Percentile.record(snapshot.get999thPercentile)
  }
}
