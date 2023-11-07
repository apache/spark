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

package org.apache.spark.network.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

import static org.junit.jupiter.api.Assertions.*;

public class TransportClientFactorySuite {
  protected TransportConf conf;
  protected TransportContext context;
  protected TransportServer server1;
  protected TransportServer server2;

  @BeforeEach
  public void setUp() {
    conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server1 = context.createServer();
    server2 = context.createServer();
  }

  @AfterEach
  public void tearDown() {
    JavaUtils.closeQuietly(server1);
    JavaUtils.closeQuietly(server2);
    JavaUtils.closeQuietly(context);
  }

  /**
   * Request a bunch of clients to a single server to test
   * we create up to maxConnections of clients.
   *
   * If concurrent is true, create multiple threads to create clients in parallel.
   */
  private void testClientReuse(int maxConnections, boolean concurrent)
    throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("spark.shuffle.io.numConnectionsPerPeer", Integer.toString(maxConnections));
    TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(configMap));

    RpcHandler rpcHandler = new NoOpRpcHandler();
    try (TransportContext context = new TransportContext(conf, rpcHandler)) {
      TransportClientFactory factory = context.createClientFactory();
      Set<TransportClient> clients = Collections.synchronizedSet(
              new HashSet<>());

      AtomicInteger failed = new AtomicInteger();
      Thread[] attempts = new Thread[maxConnections * 10];

      // Launch a bunch of threads to create new clients.
      for (int i = 0; i < attempts.length; i++) {
        attempts[i] = new Thread(() -> {
          try {
            TransportClient client =
                factory.createClient(TestUtils.getLocalHost(), server1.getPort());
            assertTrue(client.isActive());
            clients.add(client);
          } catch (IOException e) {
            failed.incrementAndGet();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

        if (concurrent) {
          attempts[i].start();
        } else {
          attempts[i].run();
        }
      }

      // Wait until all the threads complete.
      for (Thread attempt : attempts) {
        attempt.join();
      }

      Assertions.assertEquals(0, failed.get());
      Assertions.assertTrue(clients.size() <= maxConnections);

      for (TransportClient client : clients) {
        client.close();
      }

      factory.close();
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
  public void returnDifferentClientsForDifferentServers() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    assertNotSame(c1, c2);
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
    assertNotSame(c1, c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }

  @Test
  public void closeIdleConnectionForRequestTimeOut() throws IOException, InterruptedException {
    TransportConf conf = new TransportConf("shuffle", new ConfigProvider() {

      @Override
      public String get(String name) {
        if ("spark.shuffle.io.connectionTimeout".equals(name)) {
          // We should make sure there is enough time for us to observe the channel is active
          return "1s";
        }
        String value = System.getProperty(name);
        if (value == null) {
          throw new NoSuchElementException(name);
        }
        return value;
      }

      @Override
      public Iterable<Map.Entry<String, String>> getAll() {
        throw new UnsupportedOperationException();
      }
    });
    try (TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
         TransportClientFactory factory = context.createClientFactory()) {
      TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      long expiredTime = System.currentTimeMillis() + 10000; // 10 seconds
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      assertFalse(c1.isActive());
    }
  }

  @Test
  public void closeFactoryBeforeCreateClient() {
    TransportClientFactory factory = context.createClientFactory();
    factory.close();
    Assertions.assertThrows(IOException.class,
      () -> factory.createClient(TestUtils.getLocalHost(), server1.getPort()));
  }

  @Test
  public void fastFailConnectionInTimeWindow() {
    TransportClientFactory factory = context.createClientFactory();
    TransportServer server = context.createServer();
    int unreachablePort = server.getPort();
    server.close();
    Assertions.assertThrows(IOException.class,
      () -> factory.createClient(TestUtils.getLocalHost(), unreachablePort, true));
    Assertions.assertThrows(IOException.class,
      () -> factory.createClient(TestUtils.getLocalHost(), unreachablePort, true),
      "fail this connection directly");
  }

  @Test
  public void unlimitedConnectionAndCreationTimeouts() throws IOException, InterruptedException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("spark.shuffle.io.connectionTimeout", "-1");
    configMap.put("spark.shuffle.io.connectionCreationTimeout", "-1");
    TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(configMap));
    RpcHandler rpcHandler = new NoOpRpcHandler();
    try (TransportContext ctx = new TransportContext(conf, rpcHandler, true);
      TransportClientFactory factory = ctx.createClientFactory()){
      TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      long expiredTime = System.currentTimeMillis() + 5000;
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      assertTrue(c1.isActive());
      // When connectionCreationTimeout is unlimited, the connection shall be able to
      // fail when the server is not reachable.
      TransportServer server = ctx.createServer();
      int unreachablePort = server.getPort();
      JavaUtils.closeQuietly(server);
      IOException exception = Assertions.assertThrows(IOException.class,
          () -> factory.createClient(TestUtils.getLocalHost(), unreachablePort, true));
      assertNotEquals(exception.getCause(), null);
    }
  }
}
