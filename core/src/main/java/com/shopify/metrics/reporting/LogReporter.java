package com.shopify.metrics.reporting;

import com.codahale.metrics.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.PatternLayout;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which creates an appending logfile  of the measurements for each metric
 */

public class LogReporter extends ScheduledReporter {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(LogReporter.class);
  
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /*
   * should save some code and reuse the CSVReporter Builder but override the build method
   */
  public static class Builder {
    private final MetricRegistry registry;
    private Locale locale;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private Clock clock;
    private MetricFilter filter;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.locale = Locale.getDefault();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.clock = Clock.defaultClock();
      this.filter = MetricFilter.ALL;
    }

    public Builder formatFor(Locale locale){
      this.locale = locale;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit){
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit){
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder filter(MetricFilter filter){
      this.filter = filter;
      return this;
    }

    public LogReporter build(String directory) {
      return new LogReporter(registry, directory, locale, rateUnit, durationUnit, clock, filter);
    }
  } 

  private final Locale locale;
  private final Clock clock;
  private final Logger logger;

  private LogReporter(MetricRegistry registry,
      String directory,
      Locale locale,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      Clock clock,
      MetricFilter filter) {

    super(registry, "log-reporter", filter, rateUnit, durationUnit);
    this.logger = Logger.getLogger("com.shopify.metrics");
    String file = String.format("{}/spark.metrics.log", directory);

    try {
      // TODO:: simplify
      PatternLayout layout = new PatternLayout("%d{ISO8601} %c %m%n");
      RollingFileAppender logfile = new RollingFileAppender(layout, file);

      // TODO:: these should be configurable
      logfile.setMaxFileSize("50MB");
      logfile.setMaxBackupIndex(10);

      this.logger.setLevel(Level.INFO);
      this.logger.addAppender(logfile);
    } catch (IOException e) {
      LOGGER.error("Could not add appender", e);
    }

    this.locale = locale;
    this.clock = clock;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      reportGauge(timestamp, entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      reportCounter(timestamp, entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      reportHistogram(timestamp, entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      reportMeter(timestamp, entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      reportTimer(timestamp, entry.getKey(), entry.getValue());
    }
  }

  private void reportTimer(long timestamp, String name, Timer timer ) {

    final Snapshot snapshot = timer.getSnapshot();

    report(timestamp,
        name,
        "count=%d max=%f mean=%f min=%f stddev= %f p50=%f p75=%f p95=%f p98=%f p99=%f p999=%f mean_rate=%f m1_rate=%f m5_rate=%f m15_rate=%f rate_unit=calls/%s duration_unit=%s",
        timer.getCount(),
        convertDuration(snapshot.getMax()),
        convertDuration(snapshot.getMean()),
        convertDuration(snapshot.getMin()),
        convertDuration(snapshot.getStdDev()),
        convertDuration(snapshot.getMedian()),
        convertDuration(snapshot.get75thPercentile()),
        convertDuration(snapshot.get95thPercentile()),
        convertDuration(snapshot.get98thPercentile()),
        convertDuration(snapshot.get99thPercentile()),
        convertDuration(snapshot.get999thPercentile()),
        convertRate(timer.getMeanRate()),
        convertRate(timer.getOneMinuteRate()),
        convertRate(timer.getFiveMinuteRate()),
        convertRate(timer.getFifteenMinuteRate()),
        getRateUnit(),
        getDurationUnit());
  }

  private void reportMeter(long timestamp, String name, Meter meter) {
    report(timestamp,
        name,
        "count=%d mean_rate=%f m1_rate=%f m5_rate=%f m15_rate=%f rate_unit=events/%s",
        meter.getCount(),
        convertRate(meter.getMeanRate()),
        convertRate(meter.getOneMinuteRate()),
        convertRate(meter.getFiveMinuteRate()),
        convertRate(meter.getFifteenMinuteRate()),
        getRateUnit());
  } 

  private void reportHistogram(long timestamp, String name, Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();

    report(timestamp,
        name,
        "count=%d max=%d mean=%f min=%f stddev=%f p50=%f p75=%f p95=%f p98=%f p99=%f p999=%f",
        histogram.getCount(),
        snapshot.getMax(),
        snapshot.getMean(),
        snapshot.getMin(),
        snapshot.getStdDev(),
        snapshot.getMedian(),
        snapshot.get75thPercentile(),
        snapshot.get95thPercentile(),
        snapshot.get98thPercentile(),
        snapshot.get99thPercentile(),
        snapshot.get999thPercentile());
  }

  private void reportGauge(long timestamp, String name, Gauge gauge){
    report(timestamp, name, "value=%s", gauge.getValue());
  }

  private void reportCounter(long timestamp, String name, Counter counter) {
    report(timestamp, name,"count=%d", counter.getCount());
  }
  
  private void report(long timestamp, String name, String line, Object... values) {
    String metrics = String.format(line, values);
    this.logger.info(String.format(locale, "event_at=%d metric=%s %s", timestamp, name,metrics));
  }
}
