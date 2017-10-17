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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Export metrics via the Prometheus Pushgateway.
 * <p>
 * The Prometheus Pushgateway exists to allow ephemeral and
 * batch jobs to expose their metrics to Prometheus.
 * Since these kinds of jobs may not exist long enough to be scraped,
 * they can instead push their metrics to a Pushgateway.
 * This class allows pushing the contents of a {@link CollectorRegistry} to
 * a Pushgateway.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *   void executeBatchJob() throws Exception {
 *     CollectorRegistry registry = new CollectorRegistry();
 *     Gauge duration = Gauge.build()
 *         .name("my_batch_job_duration_seconds")
 *         .help("Duration of my batch job in seconds.")
 *         .register(registry);
 *     Gauge.Timer durationTimer = duration.startTimer();
 *     try {
 *       // Your code here.
 *
 *       // This is only added to the registry after success,
 *       // so that a previous success in the Pushgateway isn't overwritten on failure.
 *       Gauge lastSuccess = Gauge.build()
 *           .name("my_batch_job_last_success")
 *           .help("Last time my batch job succeeded, in unixtime.")
 *           .register(registry);
 *       lastSuccess.setToCurrentTime();
 *     } finally {
 *       durationTimer.setDuration();
 *       PushGatewayWithTimestamp pg = new PushGatewayWithTimestamp("127.0.0.1:9091");
 *       pg.pushAdd(registry, "my_batch_job");
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * See <a href="https://github.com/prometheus/pushgateway">
 *     https://github.com/prometheus/pushgateway</a>
 */
public class PushGatewayWithTimestamp {

    private static final Logger logger = LoggerFactory.getLogger(PushGatewayWithTimestamp.class);
    private final String address;
    private static final int SECONDS_PER_MILLISECOND = 1000;
    /**
     * Construct a Pushgateway, with the given address.
     * <p>
     * @param address  host:port or ip:port of the Pushgateway.
     */
    public PushGatewayWithTimestamp(String address) {
        this.address = address;
    }

    /**
     * Pushes all metrics in a registry,
     * replacing all those with the same job and no grouping key.
     * <p>
     * This uses the PUT HTTP method.
     */
    public void push(CollectorRegistry registry, String job) throws IOException {
        doRequest(registry, job, null, "PUT", null);
    }

    /**
     * Pushes all metrics in a Collector,
     * replacing all those with the same job and no grouping key.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the PUT HTTP method.
     */
    public void push(Collector collector, String job) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        collector.register(registry);
        push(registry, job);
    }

    /**
     * Pushes all metrics in a registry,
     * replacing all those with the same job and grouping key.
     * <p>
     * This uses the PUT HTTP method.
     */
    public void push(CollectorRegistry registry,
                     String job, Map<String, String> groupingKey) throws IOException {
        doRequest(registry, job, groupingKey, "PUT", null);
    }

    /**
     * Pushes all metrics in a Collector, replacing all those with the same job and grouping key.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the PUT HTTP method.
     */
    public void push(Collector collector,
                     String job, Map<String, String> groupingKey) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        collector.register(registry);
        push(registry, job, groupingKey);
    }

    /**
     * Pushes all metrics in a registry,
     * replacing only previously pushed metrics of the same name and job and no grouping key.
     * <p>
     * This uses the POST HTTP method.
     */
    public void pushAdd(CollectorRegistry registry,
                        String job, String timestamp) throws IOException {
        doRequest(registry, job, null, "POST", timestamp);
    }

    /**
     * Pushes all metrics in a Collector,
     * replacing only previously pushed metrics of the same name and job and no grouping key.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the POST HTTP method.
     */
    public void pushAdd(Collector collector, String job) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        collector.register(registry);
        pushAdd(registry, job, "");
    }

    /**
     * Pushes all metrics in a registry,
     * replacing only previously pushed metrics of the same name, job and grouping key.
     * <p>
     * This uses the POST HTTP method.
     */
    public void pushAdd(CollectorRegistry registry,String job,
                        Map<String, String> groupingKey, String timestamp) throws IOException {
        doRequest(registry, job, groupingKey, "POST", timestamp);
    }

    /**
     * Pushes all metrics in a Collector,
     * replacing only previously pushed metrics of the same name, job and grouping key.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the POST HTTP method.
     */
    public void pushAdd(Collector collector, String job,
                        Map<String, String> groupingKey) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        collector.register(registry);
        pushAdd(registry, job, groupingKey, null);
    }


    /**
     * Deletes metrics from the Pushgateway.
     * <p>
     * Deletes metrics with no grouping key and the provided job.
     * This uses the DELETE HTTP method.
     */
    public void delete(String job) throws IOException {
        doRequest(null, job, null, "DELETE", null);
    }

    /**
     * Deletes metrics from the Pushgateway.
     * <p>
     * Deletes metrics with the provided job and grouping key.
     * This uses the DELETE HTTP method.
     */
    public void delete(String job, Map<String, String> groupingKey) throws IOException {
        doRequest(null, job, groupingKey, "DELETE", null);
    }


    /**
     * Pushes all metrics in a registry, replacing all those with the same job and instance.
     * <p>
     * This uses the PUT HTTP method.
     * @deprecated use {@link #push(CollectorRegistry, String, Map)}
     */
    @Deprecated
    public void push(CollectorRegistry registry, String job, String instance) throws IOException {
        push(registry, job, Collections.singletonMap("instance", instance));
    }

    /**
     * Pushes all metrics in a Collector, replacing all those with the same job and instance.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the PUT HTTP method.
     * @deprecated use {@link #push(Collector, String, Map)}
     */
    @Deprecated
    public void push(Collector collector, String job, String instance) throws IOException {
        push(collector, job, Collections.singletonMap("instance", instance));
    }

    /**
     * Pushes all metrics in a Collector,
     * replacing only previously pushed metrics of the same name.
     * <p>
     * This is useful for pushing a single Gauge.
     * <p>
     * This uses the POST HTTP method.
     * @deprecated use {@link #pushAdd(Collector, String, Map)}
     */
    @Deprecated
    public void pushAdd(Collector collector, String job, String instance) throws IOException {
        pushAdd(collector, job, Collections.singletonMap("instance", instance));
    }

    /**
     * Deletes metrics from the Pushgateway.
     * <p>
     * This uses the DELETE HTTP method.
     * @deprecated use {@link #delete(String, Map)}
     */
    @Deprecated
    public void delete(String job, String instance) throws IOException {
        delete(job, Collections.singletonMap("instance", instance));
    }

    void doRequest(CollectorRegistry registry, String job, Map<String,
            String> groupingKey, String method, String timestamp) throws IOException {
        String url = address + "/metrics/job/" + URLEncoder.encode(job, "UTF-8");
        if (groupingKey != null) {
            for (Map.Entry<String, String> entry: groupingKey.entrySet()) {
                url += "/" + entry.getKey() + "/" + URLEncoder.encode(entry.getValue(), "UTF-8");
            }
        }

        logger.info("Sending metrics data to '{}'", url);

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty("Content-Type", TextFormatWithTimestamp.CONTENT_TYPE_004);
        if (!method.equals("DELETE")) {
            connection.setDoOutput(true);
        }
        connection.setRequestMethod(method);

        connection.setConnectTimeout(10 * SECONDS_PER_MILLISECOND);
        connection.setReadTimeout(10 * SECONDS_PER_MILLISECOND);
        connection.connect();

        try {
            if (!method.equals("DELETE")) {
                BufferedWriter writer =
                        new BufferedWriter(
                                new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
                TextFormatWithTimestamp.write004(writer,
                                                registry.metricFamilySamples(), timestamp);
                writer.flush();
                writer.close();
            }

            int response = connection.getResponseCode();
            if (response != HttpURLConnection.HTTP_ACCEPTED) {
                throw new IOException("Response code from " + url + " was " + response);
            }
        } catch (Exception ex) {
            logger.error("Sending metrics failed due to: ", ex);
        }

        finally {
            connection.disconnect();
        }
    }

    /**
     * Returns a grouping key with the instance label set to the machine's IP address.
     * <p>
     * This is a convenience function, and should only be used where you want to
     * push per-instance metrics rather than cluster/job level metrics.
     */
    public static Map<String, String> instanceIPGroupingKey() throws UnknownHostException {
        Map<String, String> groupingKey = new HashMap<String, String>();
        groupingKey.put("instance", InetAddress.getLocalHost().getHostAddress());
        return groupingKey;
    }

}

