// Copied from
// https://raw.githubusercontent.com/dropwizard/metrics/v3.2.6/metrics-ganglia/
//   src/main/java/com/codahale/metrics/ganglia/GangliaReporter.java

package com.codahale.metrics.ganglia;

import com.codahale.metrics.*;
import com.codahale.metrics.MetricAttribute;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

import static com.codahale.metrics.MetricRegistry.name;
import static com.codahale.metrics.MetricAttribute.*;

/**
 * A reporter which announces metric values to a Ganglia cluster.
 *
 * @see <a href="http://ganglia.sourceforge.net/">Ganglia Monitoring System</a>
 */
public class GangliaReporter extends ScheduledReporter {

    private static final Pattern SLASHES = Pattern.compile("\\\\");

    /**
     * Returns a new {@link Builder} for {@link GangliaReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link GangliaReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link GangliaReporter} instances. Defaults to using a {@code tmax} of {@code 60},
     * a {@code dmax} of {@code 0}, converting rates to events/second, converting durations to
     * milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private String prefix;
        private int tMax;
        private int dMax;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Set<MetricAttribute> disabledMetricAttributes = Collections.emptySet();

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.tMax = 60;
            this.dMax = 0;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
        }

        /**
         * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
         * Default value is true.
         * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
         *
         * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
         * @return {@code this}
         */
        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        /**
         * Specifies the executor to use while scheduling reporting of metrics.
         * Default value is null.
         * Null value leads to executor will be auto created on start.
         *
         * @param executor the executor to use while scheduling reporting of metrics.
         * @return {@code this}
         */
        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Use the given {@code tmax} value when announcing metrics.
         *
         * @param tMax the desired gmond {@code tmax} value
         * @return {@code this}
         */
        public Builder withTMax(int tMax) {
            this.tMax = tMax;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Use the given {@code dmax} value when announcing metrics.
         *
         * @param dMax the desired gmond {@code dmax} value
         * @return {@code this}
         */
        public Builder withDMax(int dMax) {
            this.dMax = dMax;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15").
         * See {@link MetricAttribute}.
         *
         * @param disabledMetricAttributes a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        /**
         * Builds a {@link GangliaReporter} with the given properties, announcing metrics to the
         * given {@link GMetric} client.
         *
         * @param gmetric the client to use for announcing metrics
         * @return a {@link GangliaReporter}
         */
        public GangliaReporter build(GMetric gmetric) {
            return new GangliaReporter(registry, gmetric, null, prefix, tMax, dMax, rateUnit, durationUnit, filter,
                    executor, shutdownExecutorOnStop, disabledMetricAttributes);
        }

        /**
         * Builds a {@link GangliaReporter} with the given properties, announcing metrics to the
         * given {@link GMetric} client.
         *
         * @param gmetrics the clients to use for announcing metrics
         * @return a {@link GangliaReporter}
         */
        public GangliaReporter build(GMetric... gmetrics) {
            return new GangliaReporter(registry, null, gmetrics, prefix, tMax, dMax, rateUnit, durationUnit,
                    filter, executor, shutdownExecutorOnStop , disabledMetricAttributes);
        }
    }

    private static final SparkLogger LOGGER = SparkLoggerFactory.getLogger(GangliaReporter.class);

    private final GMetric gmetric;
    private final GMetric[] gmetrics;
    private final String prefix;
    private final int tMax;
    private final int dMax;

    private GangliaReporter(MetricRegistry registry,
                            GMetric gmetric,
                            GMetric[] gmetrics,
                            String prefix,
                            int tMax,
                            int dMax,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            ScheduledExecutorService executor,
                            boolean shutdownExecutorOnStop,
                            Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, "ganglia-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop,
                disabledMetricAttributes);
        this.gmetric = gmetric;
        this.gmetrics = gmetrics;
        this.prefix = prefix;
        this.tMax = tMax;
        this.dMax = dMax;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(String name, Timer timer) {
        final String sanitizedName = escapeSlashes(name);
        final String group = group(name);
        try {
            final Snapshot snapshot = timer.getSnapshot();

            announceIfEnabled(MAX, sanitizedName, group, convertDuration(snapshot.getMax()), getDurationUnit());
            announceIfEnabled(MEAN, sanitizedName, group, convertDuration(snapshot.getMean()), getDurationUnit());
            announceIfEnabled(MIN, sanitizedName, group, convertDuration(snapshot.getMin()), getDurationUnit());
            announceIfEnabled(STDDEV, sanitizedName, group, convertDuration(snapshot.getStdDev()), getDurationUnit());

            announceIfEnabled(P50, sanitizedName, group, convertDuration(snapshot.getMedian()), getDurationUnit());
            announceIfEnabled(P75, sanitizedName,
                     group,
                     convertDuration(snapshot.get75thPercentile()),
                     getDurationUnit());
            announceIfEnabled(P95, sanitizedName,
                     group,
                     convertDuration(snapshot.get95thPercentile()),
                     getDurationUnit());
            announceIfEnabled(P98, sanitizedName,
                     group,
                     convertDuration(snapshot.get98thPercentile()),
                     getDurationUnit());
            announceIfEnabled(P99, sanitizedName,
                     group,
                     convertDuration(snapshot.get99thPercentile()),
                     getDurationUnit());
            announceIfEnabled(P999, sanitizedName,
                     group,
                     convertDuration(snapshot.get999thPercentile()),
                     getDurationUnit());

            reportMetered(sanitizedName, timer, group, "calls");
        } catch (GangliaException e) {
            LOGGER.warn("Unable to report timer {}", e,
                MDC.of(LogKeys.METRIC_NAME$.MODULE$, sanitizedName));
        }
    }

    private void reportMeter(String name, Meter meter) {
        final String sanitizedName = escapeSlashes(name);
        final String group = group(name);
        try {
            reportMetered(sanitizedName, meter, group, "events");
        } catch (GangliaException e) {
            LOGGER.warn("Unable to report meter {}", e,
                MDC.of(LogKeys.METRIC_NAME$.MODULE$, name));
        }
    }

    private void reportMetered(String name, Metered meter, String group, String eventName) throws GangliaException {
        final String unit = eventName + '/' + getRateUnit();
        announceIfEnabled(COUNT, name, group, meter.getCount(), eventName);
        announceIfEnabled(M1_RATE, name, group, convertRate(meter.getOneMinuteRate()), unit);
        announceIfEnabled(M5_RATE, name, group, convertRate(meter.getFiveMinuteRate()), unit);
        announceIfEnabled(M15_RATE, name, group, convertRate(meter.getFifteenMinuteRate()), unit);
        announceIfEnabled(MEAN_RATE, name, group, convertRate(meter.getMeanRate()), unit);
    }

    private void reportHistogram(String name, Histogram histogram) {
        final String sanitizedName = escapeSlashes(name);
        final String group = group(name);
        try {
            final Snapshot snapshot = histogram.getSnapshot();

            announceIfEnabled(COUNT, sanitizedName, group, histogram.getCount(), "");
            announceIfEnabled(MAX, sanitizedName, group, snapshot.getMax(), "");
            announceIfEnabled(MEAN, sanitizedName, group, snapshot.getMean(), "");
            announceIfEnabled(MIN, sanitizedName, group, snapshot.getMin(), "");
            announceIfEnabled(STDDEV, sanitizedName, group, snapshot.getStdDev(), "");
            announceIfEnabled(P50, sanitizedName, group, snapshot.getMedian(), "");
            announceIfEnabled(P75, sanitizedName, group, snapshot.get75thPercentile(), "");
            announceIfEnabled(P95, sanitizedName, group, snapshot.get95thPercentile(), "");
            announceIfEnabled(P98, sanitizedName, group, snapshot.get98thPercentile(), "");
            announceIfEnabled(P99, sanitizedName, group, snapshot.get99thPercentile(), "");
            announceIfEnabled(P999, sanitizedName, group, snapshot.get999thPercentile(), "");
        } catch (GangliaException e) {
            LOGGER.warn("Unable to report histogram {}", e,
                MDC.of(LogKeys.METRIC_NAME$.MODULE$, sanitizedName));
        }
    }

    private void reportCounter(String name, Counter counter) {
        final String sanitizedName = escapeSlashes(name);
        final String group = group(name);
        try {
            announce(prefix(sanitizedName, COUNT.getCode()), group, Long.toString(counter.getCount()), GMetricType.DOUBLE, "");
        } catch (GangliaException e) {
            LOGGER.warn("Unable to report counter {}", e,
                MDC.of(LogKeys.METRIC_NAME$.MODULE$, name));
        }
    }

    private void reportGauge(String name, Gauge gauge) {
        final String sanitizedName = escapeSlashes(name);
        final String group = group(name);
        final Object obj = gauge.getValue();
        final String value = String.valueOf(obj);
        final GMetricType type = detectType(obj);
        try {
            announce(name(prefix, sanitizedName), group, value, type, "");
        } catch (GangliaException e) {
            LOGGER.warn("Unable to report gauge {}", e,
                MDC.of(LogKeys.METRIC_NAME$.MODULE$, name));
        }
    }

    private static final double MIN_VAL = 1E-300;

    private void announceIfEnabled(MetricAttribute metricAttribute, String metricName, String group, double value, String units)
            throws GangliaException {
        if (getDisabledMetricAttributes().contains(metricAttribute)) {
            return;
        }
        final String string = Math.abs(value) < MIN_VAL ? "0" : Double.toString(value);
        announce(prefix(metricName, metricAttribute.getCode()), group, string, GMetricType.DOUBLE, units);
    }

    private void announceIfEnabled(MetricAttribute metricAttribute, String metricName, String group, long value, String units)
            throws GangliaException {
        if (getDisabledMetricAttributes().contains(metricAttribute)) {
            return;
        }
        announce(prefix(metricName, metricAttribute.getCode()), group, Long.toString(value), GMetricType.DOUBLE, units);
    }

    private void announce(String name, String group, String value, GMetricType type, String units)
            throws GangliaException {
        if (gmetric != null) {
            gmetric.announce(name, value, type, units, GMetricSlope.BOTH, tMax, dMax, group);
        } else {
            for (GMetric gmetric : gmetrics) {
                gmetric.announce(name, value, type, units, GMetricSlope.BOTH, tMax, dMax, group);
            }
        }
    }

    private GMetricType detectType(Object o) {
        if (o instanceof Float) {
            return GMetricType.FLOAT;
        } else if (o instanceof Double) {
            return GMetricType.DOUBLE;
        } else if (o instanceof Byte) {
            return GMetricType.INT8;
        } else if (o instanceof Short) {
            return GMetricType.INT16;
        } else if (o instanceof Integer) {
            return GMetricType.INT32;
        } else if (o instanceof Long) {
            return GMetricType.DOUBLE;
        }
        return GMetricType.STRING;
    }

    private String group(String name) {
        final int i = name.lastIndexOf('.');
        if (i < 0) {
            return "";
        }
        return name.substring(0, i);
    }

    private String prefix(String name, String n) {
        return name(prefix, name, n);
    }

    // ganglia metric names can't contain slashes.
    private String escapeSlashes(String name) {
        return SLASHES.matcher(name).replaceAll("_");
    }
}
