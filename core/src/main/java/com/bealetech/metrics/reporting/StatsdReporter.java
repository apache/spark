package com.bealetech.metrics.reporting;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a Statds server.
 *
 * @see <a href="https://github.com/etsy/statsd">Statsd</a>
 */
public class StatsdReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link StatsdReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link StatsdReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link StatsdReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
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
         * Builds a {@link StatsdReporter} with the given properties, sending metrics using the
         * given {@link Statsd} client.
         *
         * @param statsd a {@link Statsd} client
         * @return a {@link StatsdReporter}
         */
        public StatsdReporter build(Statsd statsd) {
            return new StatsdReporter(registry,
                    statsd,
                    prefix,
                    filter,
                    rateUnit,
                    durationUnit);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsdReporter.class);

    private final Statsd statsd;
    private final String prefix;

    public StatsdReporter(MetricRegistry registry,
                            Statsd statsd,
                            String prefix,
                            MetricFilter filter,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit) {
        super(registry, "statsd-reporter", filter, rateUnit, durationUnit);

        this.statsd = statsd;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        try {
            statsd.connect();

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
                reportMetered(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue());
            }

        } catch(IOException e) {
            LOGGER.warn("Unable to report to StatsD", statsd, e);
        } finally {
            try {
                statsd.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from StatsD server", statsd, e);
            }
        }
    }

    private void reportTimer(String name, Timer timer) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        statsd.send(prefix(name, "max"),
                format(convertDuration(snapshot.getMax())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "mean"),
                format(convertDuration(snapshot.getMean())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "min"),
                format(convertDuration(snapshot.getMin())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "stddev"),
                format(convertDuration(snapshot.getStdDev())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p50"),
                format(convertDuration(snapshot.getMedian())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p75"),
                format(convertDuration(snapshot.get75thPercentile())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p95"),
                format(convertDuration(snapshot.get95thPercentile())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p98"),
                format(convertDuration(snapshot.get98thPercentile())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p99"),
                format(convertDuration(snapshot.get99thPercentile())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p999"),
                format(convertDuration(snapshot.get999thPercentile())),
                Statsd.StatType.TIMER);

        reportMetered(name, timer);
    }

    private void reportMetered(String name, Metered meter) throws IOException {
        statsd.send(prefix(name, "count"), format(meter.getCount()), Statsd.StatType.GAUGE);
        statsd.send(prefix(name, "m1_rate"),
                format(convertRate(meter.getOneMinuteRate())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "m5_rate"),
                format(convertRate(meter.getFiveMinuteRate())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "m15_rate"),
                format(convertRate(meter.getFifteenMinuteRate())),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "mean_rate"),
                format(convertRate(meter.getMeanRate())),
                Statsd.StatType.TIMER);
    }

    private void reportHistogram(String name, Histogram histogram) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        statsd.send(prefix(name, "count"),
                format(histogram.getCount()),
                Statsd.StatType.GAUGE);
        statsd.send(prefix(name, "max"),
                format(snapshot.getMax()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "mean"),
                format(snapshot.getMean()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "min"),
                format(snapshot.getMin()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "stddev"),
                format(snapshot.getStdDev()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p50"),
                format(snapshot.getMedian()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p75"),
                format(snapshot.get75thPercentile()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p95"),
                format(snapshot.get95thPercentile()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p98"),
                format(snapshot.get98thPercentile()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p99"),
                format(snapshot.get99thPercentile()),
                Statsd.StatType.TIMER);
        statsd.send(prefix(name, "p999"),
                format(snapshot.get999thPercentile()),
                Statsd.StatType.TIMER);
    }

    private void reportCounter(String name, Counter counter) throws IOException {
        statsd.send(prefix(name, "count"),
                format(counter.getCount()),
                Statsd.StatType.COUNTER);
    }

    private void reportGauge(String name, Gauge gauge) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {
            statsd.send(prefix(name), value,
                    Statsd.StatType.GAUGE);
        }
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        }
        return null;
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        return String.format(Locale.US, "%2.2f", v);
    }
}
