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


import com.google.common.collect.Lists;
import io.prometheus.client.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;


public class TextFormatWithTimestampSuite {

    @Test
    public void buildTextFormat() {
        // Given
        List<Collector.MetricFamilySamples> mfs = Lists.newArrayList(
                new Collector.MetricFamilySamples(
                        "Metric_Family_1",
                        Collector.Type.GAUGE,
                        "help1",
                        Lists.newArrayList(
                                new Collector.MetricFamilySamples.Sample(
                                        "Sample1",
                                        Lists.newArrayList("label1", "label2"),
                                        Lists.newArrayList("1", "value1"),
                                        1.0
                                ),
                                new Collector.MetricFamilySamples.Sample(
                                        "Sample2",
                                        Lists.newArrayList("label1", "label2"),
                                        Lists.newArrayList("2", "value2"),
                                        2.0
                                )
                        )
                ),
                new Collector.MetricFamilySamples(
                        "Metric_Family_2",
                        Collector.Type.COUNTER,
                        "help2",
                        Lists.newArrayList(
                                new Collector.MetricFamilySamples.Sample(
                                        "Count1",
                                        Lists.newArrayList("label1"),
                                        Lists.newArrayList("1"),
                                        5
                                ),
                                new Collector.MetricFamilySamples.Sample(
                                        "Sum",
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        9
                                )
                        )
                ),
                new Collector.MetricFamilySamples(
                        "Metric_Family_3",
                        Collector.Type.COUNTER,
                        "multiline\nhelp\\escaped",
                        Lists.newArrayList(
                                new Collector.MetricFamilySamples.Sample(
                                        "Count1",
                                        Lists.newArrayList("label1"),
                                        Lists.newArrayList("1\\2\n"),
                                        5
                                ),
                                new Collector.MetricFamilySamples.Sample(
                                        "Sum",
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        9
                                )
                        )
                )

        );

        String timeStamp = String.valueOf(System.currentTimeMillis());


        String expected = String.join("\n",
                "# HELP Metric_Family_1 help1",
                "# TYPE Metric_Family_1 gauge",
                "Sample1{label1=\"1\",label2=\"value1\"} 1.0 " + timeStamp,
                "Sample2{label1=\"2\",label2=\"value2\"} 2.0 " + timeStamp,
                "# HELP Metric_Family_2 help2",
                "# TYPE Metric_Family_2 counter",
                "Count1{label1=\"1\"} 5.0 " + timeStamp,
                "Sum 9.0 " + timeStamp,
                "# HELP Metric_Family_3 multiline\\nhelp\\\\escaped",
                "# TYPE Metric_Family_3 counter",
                "Count1{label1=\"1\\\\2\\n\"} 5.0 " + timeStamp,
                "Sum 9.0 " + timeStamp,
                ""
        );

        // When
        String actual = TextFormatWithTimestamp.buildTextFormat(Collections.enumeration(mfs),
                timeStamp);

        // Then
        Assert.assertEquals(expected, actual);

    }

    @Test
    public void buildTextFormatWithNoMetricsTimestamp() {
        // Given
        List<Collector.MetricFamilySamples> mfs = Lists.newArrayList(
                new Collector.MetricFamilySamples(
                        "Metric_Family_1",
                        Collector.Type.GAUGE,
                        "help1",
                        Lists.newArrayList(
                                new Collector.MetricFamilySamples.Sample(
                                        "Sample1",
                                        Lists.newArrayList("label1", "label2"),
                                        Lists.newArrayList("1", "value1"),
                                        1.0
                                ),
                                new Collector.MetricFamilySamples.Sample(
                                        "Sample2",
                                        Lists.newArrayList("label1", "label2"),
                                        Lists.newArrayList("2", "value2"),
                                        2.0
                                )
                        )
                )
        );

        String expected = String.join("\n",
                "# HELP Metric_Family_1 help1",
                "# TYPE Metric_Family_1 gauge",
                "Sample1{label1=\"1\",label2=\"value1\"} 1.0",
                "Sample2{label1=\"2\",label2=\"value2\"} 2.0",
                ""
        );

        // When
        String actual = TextFormatWithTimestamp.buildTextFormat(Collections.enumeration(mfs),
                null);

        // Then
        Assert.assertEquals(expected, actual);

    }

}
