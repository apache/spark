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

package org.apache.spark.network.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import org.apache.spark.network.TestUtils;
import org.apache.spark.network.client.TransportClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;

public class NettyMemoryMetricsSuite {

  private TransportConf conf;
  private TransportContext context;
  private TransportServer server;
  private TransportClientFactory clientFactory;

  private void setUp(boolean enableVerboseMetrics) {
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put("spark.shuffle.io.enableVerboseMetrics", String.valueOf(enableVerboseMetrics));
    conf = new TransportConf("shuffle", new MapConfigProvider(configMap));
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @After
  public void tearDown() {
    if (clientFactory != null) {
      JavaUtils.closeQuietly(clientFactory);
      clientFactory = null;
    }
    if (server != null) {
      JavaUtils.closeQuietly(server);
      server = null;
    }
    if (context != null) {
      JavaUtils.closeQuietly(context);
      context = null;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGeneralNettyMemoryMetrics() throws IOException, InterruptedException {
    setUp(false);

    MetricSet serverMetrics = server.getAllMetrics();
    Assert.assertNotNull(serverMetrics);
    Assert.assertNotNull(serverMetrics.getMetrics());
    Assert.assertNotEquals(serverMetrics.getMetrics().size(), 0);

    Map<String, Metric> serverMetricMap = serverMetrics.getMetrics();
    serverMetricMap.forEach((name, metric) ->
      Assert.assertTrue(name.startsWith("shuffle-server"))
    );

    MetricSet clientMetrics = clientFactory.getAllMetrics();
    Assert.assertNotNull(clientMetrics);
    Assert.assertNotNull(clientMetrics.getMetrics());
    Assert.assertNotEquals(clientMetrics.getMetrics().size(), 0);

    Map<String, Metric> clientMetricMap = clientMetrics.getMetrics();
    clientMetricMap.forEach((name, metrics) ->
      Assert.assertTrue(name.startsWith("shuffle-client"))
    );

    // Make sure general metrics existed.
    String heapMemoryMetric = "usedHeapMemory";
    String directMemoryMetric = "usedDirectMemory";
    Assert.assertNotNull(serverMetricMap.get(
      MetricRegistry.name("shuffle-server", heapMemoryMetric)));
    Assert.assertNotNull(serverMetricMap.get(
      MetricRegistry.name("shuffle-server", directMemoryMetric)));

    Assert.assertNotNull(clientMetricMap.get(
      MetricRegistry.name("shuffle-client", heapMemoryMetric)));
    Assert.assertNotNull(clientMetricMap.get(
      MetricRegistry.name("shuffle-client", directMemoryMetric)));

    TransportClient client = null;
    try {
      client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
      Assert.assertTrue(client.isActive());

      Assert.assertTrue(((Gauge<Long>)serverMetricMap.get(
        MetricRegistry.name("shuffle-server", heapMemoryMetric))).getValue() >= 0L);
      Assert.assertTrue(((Gauge<Long>)serverMetricMap.get(
        MetricRegistry.name("shuffle-server", directMemoryMetric))).getValue() >= 0L);

      Assert.assertTrue(((Gauge<Long>)clientMetricMap.get(
        MetricRegistry.name("shuffle-client", heapMemoryMetric))).getValue() >= 0L);
      Assert.assertTrue(((Gauge<Long>)clientMetricMap.get(
        MetricRegistry.name("shuffle-client", directMemoryMetric))).getValue() >= 0L);

    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAdditionalMetrics() throws IOException, InterruptedException {
    setUp(true);

    // Make sure additional metrics are added.
    Map<String, Metric> serverMetricMap = server.getAllMetrics().getMetrics();
    serverMetricMap.forEach((name, metric) -> {
      Assert.assertTrue(name.startsWith("shuffle-server"));
      String metricName = name.substring(name.lastIndexOf(".") + 1);
      Assert.assertTrue(metricName.equals("usedDirectMemory")
        || metricName.equals("usedHeapMemory")
        || NettyMemoryMetrics.VERBOSE_METRICS.contains(metricName));
    });

    Map<String, Metric> clientMetricMap = clientFactory.getAllMetrics().getMetrics();
    clientMetricMap.forEach((name, metric) -> {
      Assert.assertTrue(name.startsWith("shuffle-client"));
      String metricName = name.substring(name.lastIndexOf(".") + 1);
      Assert.assertTrue(metricName.equals("usedDirectMemory")
        || metricName.equals("usedHeapMemory")
        || NettyMemoryMetrics.VERBOSE_METRICS.contains(metricName));
    });

    TransportClient client = null;
    try {
      client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
      Assert.assertTrue(client.isActive());

      String activeBytesMetric = "numActiveBytes";
      Assert.assertTrue(((Gauge<Long>) serverMetricMap.get(MetricRegistry.name("shuffle-server",
        "directArena0", activeBytesMetric))).getValue() >= 0L);

      Assert.assertTrue(((Gauge<Long>) clientMetricMap.get(MetricRegistry.name("shuffle-client",
        "directArena0", activeBytesMetric))).getValue() >= 0L);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
