/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.metrics.prometheus.client.exporter;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import io.prometheus.client.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writing {@link Collector.MetricFamilySamples}
 * into the format expected by Prometheus {@see http://prometheus.io/docs/instrumenting/exposition_formats/}
 */
public class TextFormatWithTimestamp {
    private static final Logger logger = LoggerFactory.getLogger(TextFormatWithTimestamp.class);

    /**
     * Content-type for text version 0.0.4.
     */
    public static final String CONTENT_TYPE_004 = "text/plain; version=0.0.4; charset=utf-8";

    /**
     * Write out the text version 0.0.4 of the given MetricFamilySamples.
     */
    public static void write004(Writer writer,
                                Enumeration<Collector.MetricFamilySamples> mfs) throws IOException {
        write004(writer, mfs, null);
    }

    /**
     * Enriches the given MetricFamilySamples with provided timestamp and
     * writes it out in text version 0.0.4.
     */
    public static void write004(Writer writer, Enumeration<Collector.MetricFamilySamples> mfs,
                                String timestamp) throws IOException {
        String payload = buildTextFormat(mfs, timestamp);

        logger.debug("Metrics data (pushgateway payload): \n{}", payload);

        writer.write(payload);
    }

    /**
     * Converts the given {@link Collector.MetricFamilySamples} collection into text version 0.0.4
     * according to {@see http://prometheus.io/docs/instrumenting/exposition_formats/}
     */
    protected static String buildTextFormat(Enumeration<Collector.MetricFamilySamples> mfs, String timestamp) {
        StringBuilder textFormatBuilder = new StringBuilder();

        for (Collector.MetricFamilySamples metricFamilySamples : Collections.list(mfs)) {
            appendHelp(textFormatBuilder, metricFamilySamples.name, metricFamilySamples.help);
            appendType(textFormatBuilder, metricFamilySamples.name, metricFamilySamples.type);

            for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
                textFormatBuilder.append('\n');
                appendMetricSample(textFormatBuilder, sample);
                appendTimestamp(textFormatBuilder, timestamp);
            }

            textFormatBuilder.append('\n');
        }

        return textFormatBuilder.toString();
    }

    private static void appendHelp(StringBuilder sb, String metricsFamilyName, String help) {
        sb.append("# HELP ").append(metricsFamilyName).append(' ');
        appendEscapedHelp(sb, help);
    }

    private static void appendType(StringBuilder sb, String metricsFamilyName, Collector.Type type) {
        sb.append('\n');
        sb.append("# TYPE ").append(metricsFamilyName).append(' ');
        sb.append(typeString(type));
    }

    private static void appendLabels(StringBuilder sb, List<String> labels, List<String> values) {
        sb.append('{');
        for (int i = 0; i < labels.size(); ++i) {
            if (i != 0) {
                sb.append(',');
            }

            sb.append(labels.get(i)).append("=\"");
            appendEscapedLabelValue(sb, values.get(i));
            sb.append('\"');
        }
        sb.append('}');
    }

    private static void appendMetricSample(StringBuilder sb, Collector.MetricFamilySamples.Sample sample) {
        sb.append(sample.name);
        if (sample.labelNames.size() > 0) {
            appendLabels(sb, sample.labelNames, sample.labelValues);
        }
        sb.append(' ').append(Collector.doubleToGoString(sample.value));
    }

    private static void appendTimestamp(StringBuilder sb, String timestamp) {
        if (timestamp != null && !timestamp.isEmpty()) {
            sb.append(" ").append(timestamp);
        }
    }

    private static void appendEscapedHelp(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                default:
                    sb.append(c);
            }
        }
    }

    private static void appendEscapedLabelValue(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                default:
                    sb.append(c);
            }
        }
    }

    private static String typeString(Collector.Type t) {
        switch (t) {
            case GAUGE:
                return "gauge";
            case COUNTER:
                return "counter";
            case SUMMARY:
                return "summary";
            case HISTOGRAM:
                return "histogram";
            default:
                return "untyped";
        }
    }
}

