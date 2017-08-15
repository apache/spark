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
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import org.apache.spark.network.TestUtils;
import org.apache.spark.network.client.TransportClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

  @Before
  public void setUp() {
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
  }

  @After
  public void tearDown() {
    JavaUtils.closeQuietly(clientFactory);
    JavaUtils.closeQuietly(server);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNettyMemoryMetrics() throws IOException, InterruptedException {
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

    String chunkMetricName = "numChunkLists";
    // Assert at least one directArena's numChunkLists metric exists.
    Assert.assertNotNull(serverMetricMap.get(MetricRegistry.name("shuffle-server",
      "directArena0", chunkMetricName)));
    // Make sure aggregated metric exists.
    Assert.assertNotNull(serverMetricMap.get(MetricRegistry.name("shuffle-server",
    "directArena", chunkMetricName)));

    TransportClient client = null;
    try {
      client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
      Assert.assertTrue(client.isActive());

      String activeBytesMetric = "numActiveBytes";
      Assert.assertTrue(((Gauge<Long>)serverMetricMap.get(MetricRegistry.name("shuffle-server",
        "directArena0", activeBytesMetric))).getValue() >= 0L);
      Assert.assertTrue(((Gauge<Long>)serverMetricMap.get(MetricRegistry.name("shuffle-server",
        "directArena", activeBytesMetric))).getValue() >= 0L);

      Assert.assertTrue(((Gauge<Long>)clientMetricMap.get(MetricRegistry.name("shuffle-client",
        "directArena0", activeBytesMetric))).getValue() >= 0L);
      Assert.assertTrue(((Gauge<Long>)clientMetricMap.get(MetricRegistry.name("shuffle-client",
        "directArena", activeBytesMetric))).getValue() >= 0L);

    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
