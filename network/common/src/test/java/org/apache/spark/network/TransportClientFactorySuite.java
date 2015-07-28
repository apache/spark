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

package org.apache.spark.network;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class TransportClientFactorySuite {
  private TransportConf conf;
  private TransportContext context;
  private TransportServer server1;
  private TransportServer server2;

  @Before
  public void setUp() {
    conf = new TransportConf(new SystemPropertyConfigProvider());
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server1 = context.createServer();
    server2 = context.createServer();
  }

  @After
  public void tearDown() {
    JavaUtils.closeQuietly(server1);
    JavaUtils.closeQuietly(server2);
  }

  /**
   * Request a bunch of clients to a single server to test
   * we create up to maxConnections of clients.
   *
   * If concurrent is true, create multiple threads to create clients in parallel.
   */
  private void testClientReuse(final int maxConnections, boolean concurrent)
    throws IOException, InterruptedException {

    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("spark.shuffle.io.numConnectionsPerPeer", Integer.toString(maxConnections));
    TransportConf conf = new TransportConf(new MapConfigProvider(configMap));

    RpcHandler rpcHandler = new NoOpRpcHandler();
    TransportContext context = new TransportContext(conf, rpcHandler);
    final TransportClientFactory factory = context.createClientFactory();
    final Set<TransportClient> clients = Collections.synchronizedSet(
      new HashSet<TransportClient>());

    final AtomicInteger failed = new AtomicInteger();
    Thread[] attempts = new Thread[maxConnections * 10];

    // Launch a bunch of threads to create new clients.
    for (int i = 0; i < attempts.length; i++) {
      attempts[i] = new Thread() {
        @Override
        public void run() {
          try {
            TransportClient client =
              factory.createClient(TestUtils.getLocalHost(), server1.getPort());
            assert (client.isActive());
            clients.add(client);
          } catch (IOException e) {
            failed.incrementAndGet();
          }
        }
      };

      if (concurrent) {
        attempts[i].start();
      } else {
        attempts[i].run();
      }
    }

    // Wait until all the threads complete.
    for (int i = 0; i < attempts.length; i++) {
      attempts[i].join();
    }

    assert(failed.get() == 0);
    assert(clients.size() == maxConnections);

    for (TransportClient client : clients) {
      client.close();
    }
  }

  @Test
  public void reuseClientsUpToConfigVariable() throws Exception {
    testClientReuse(1, false);
    testClientReuse(2, false);
    testClientReuse(3, false);
    testClientReuse(4, false);
  }

  @Test
  public void reuseClientsUpToConfigVariableConcurrent() throws Exception {
    testClientReuse(1, true);
    testClientReuse(2, true);
    testClientReuse(3, true);
    testClientReuse(4, true);
  }

  @Test
  public void returnDifferentClientsForDifferentServers() throws IOException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    assertTrue(c1 != c2);
    factory.close();
  }

  @Test
  public void neverReturnInactiveClients() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    c1.close();

    long start = System.currentTimeMillis();
    while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
      Thread.sleep(10);
    }
    assertFalse(c1.isActive());

    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    assertFalse(c1 == c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws IOException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }
}
