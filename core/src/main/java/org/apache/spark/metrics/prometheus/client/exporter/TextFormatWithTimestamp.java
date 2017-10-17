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
import java.util.Enumeration;

import io.prometheus.client.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFormatWithTimestamp {
    private static final Logger logger = LoggerFactory.getLogger(TextFormatWithTimestamp.class);

    /**
     * Content-type for text version 0.0.4.
     */
    public static final String CONTENT_TYPE_004 = "text/plain; version=0.0.4; charset=utf-8";

    private static StringBuilder jsonMessageLogBuilder = new StringBuilder();

    public static void write004(Writer writer,
                                Enumeration<Collector.MetricFamilySamples> mfs)throws IOException {
        write004(writer, mfs, null);
    }

    /**
     * Write out the text version 0.0.4 of the given MetricFamilySamples.
     */
    public static void write004(Writer writer,Enumeration<Collector.MetricFamilySamples> mfs,
                                String timestamp) throws IOException {
    /* See http://prometheus.io/docs/instrumenting/exposition_formats/
     * for the output format specification. */
    while(mfs.hasMoreElements()) {
        Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();

        logger.debug("Metrics data");
        logger.debug(metricFamilySamples.toString());
        logger.debug("Logging metrics as a json format:");


        writer.write("# HELP ");
        appendToJsonMessageLogBuilder("# HELP ");
        writer.write(metricFamilySamples.name);
        appendToJsonMessageLogBuilder(metricFamilySamples.name);
        writer.write(' ');
        appendToJsonMessageLogBuilder(' ');
        writeEscapedHelp(writer, metricFamilySamples.help);
        writer.write('\n');
        appendToJsonMessageLogBuilder('\n');

        writer.write("# TYPE ");
        appendToJsonMessageLogBuilder("# TYPE ");
        writer.write(metricFamilySamples.name);
        appendToJsonMessageLogBuilder(metricFamilySamples.name);
        writer.write(' ');
        appendToJsonMessageLogBuilder(' ');
        writer.write(typeString(metricFamilySamples.type));
        appendToJsonMessageLogBuilder(typeString(metricFamilySamples.type));
        writer.write('\n');
        appendToJsonMessageLogBuilder('\n');

        for (Collector.MetricFamilySamples.Sample sample: metricFamilySamples.samples) {
            writer.write(sample.name);
            appendToJsonMessageLogBuilder(sample.name);
            if (sample.labelNames.size() > 0) {
                writer.write('{');
                appendToJsonMessageLogBuilder('{');
                for (int i = 0; i < sample.labelNames.size(); ++i) {
                    writer.write(sample.labelNames.get(i));
                    appendToJsonMessageLogBuilder(sample.labelNames.get(i));
                    writer.write("=\"");
                    appendToJsonMessageLogBuilder("=\"");
                    writeEscapedLabelValue(writer, sample.labelValues.get(i));
                    writer.write("\",");
                    appendToJsonMessageLogBuilder("\",");
                }
                writer.write('}');
                appendToJsonMessageLogBuilder('}');
            }
            writer.write(' ');
            appendToJsonMessageLogBuilder(' ');
            writer.write(Collector.doubleToGoString(sample.value));
            appendToJsonMessageLogBuilder(Collector.doubleToGoString(sample.value));
            if(timestamp != null && !timestamp.isEmpty()) {
                writer.write(" " + timestamp);
                appendToJsonMessageLogBuilder(" " + timestamp);
            }
            writer.write('\n');
            appendToJsonMessageLogBuilder('\n');
        }
        logger.debug("JSON: "+ jsonMessageLogBuilder);
        }
    }

    private static void writeEscapedHelp(Writer writer, String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':

                    writer.append("\\\\");
                    appendToJsonMessageLogBuilder("\\\\");
                    break;
                case '\n':
                    writer.append("\\n");
                    appendToJsonMessageLogBuilder("\\n");
                    break;
                default:
                    writer.append(c);
                    appendToJsonMessageLogBuilder(c);
            }
        }
    }

    private static void writeEscapedLabelValue(Writer writer, String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    writer.append("\\\\");
                    appendToJsonMessageLogBuilder("\\\\");
                    break;
                case '\"':
                    writer.append("\\\"");
                    appendToJsonMessageLogBuilder("\\\"");
                    break;
                case '\n':
                    writer.append("\\n");
                    appendToJsonMessageLogBuilder("\\n");
                    break;
                default:
                    writer.append(c);
                    appendToJsonMessageLogBuilder(c);
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

    private static void appendToJsonMessageLogBuilder(String msg) {
        if (logger.isDebugEnabled()) {
            jsonMessageLogBuilder.append(msg);
        }
    }

    private static void appendToJsonMessageLogBuilder(char c) {
        if (logger.isDebugEnabled()) {
            jsonMessageLogBuilder.append(c);
        }
    }
}

