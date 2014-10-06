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

import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.client.SluiceClient;
import org.apache.spark.network.client.SluiceClientFactory;
import org.apache.spark.network.server.DefaultStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.SluiceServer;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.DefaultConfigProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.SluiceConfig;

public class SluiceClientFactorySuite {
  private SluiceConfig conf;
  private SluiceServer server1;
  private SluiceServer server2;

  @Before
  public void setUp() {
    conf = new SluiceConfig(new DefaultConfigProvider());
    StreamManager streamManager = new DefaultStreamManager();
    RpcHandler rpcHandler = new NoOpRpcHandler();
    server1 = new SluiceServer(conf, streamManager, rpcHandler);
    server2 = new SluiceServer(conf, streamManager, rpcHandler);
  }

  @After
  public void tearDown() {
    JavaUtils.closeQuietly(server1);
    JavaUtils.closeQuietly(server2);
  }

  @Test
  public void createAndReuseBlockClients() throws TimeoutException {
    SluiceClientFactory factory = new SluiceClientFactory(conf);
    SluiceClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    SluiceClient c2 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    SluiceClient c3 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c3.isActive());
    assertTrue(c1 == c2);
    assertTrue(c1 != c3);
    factory.close();
  }

  @Test
  public void neverReturnInactiveClients() throws Exception {
    SluiceClientFactory factory = new SluiceClientFactory(conf);
    SluiceClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    c1.close();

    long start = System.currentTimeMillis();
    while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
      Thread.sleep(10);
    }
    assertFalse(c1.isActive());

    SluiceClient c2 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    assertFalse(c1 == c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws TimeoutException {
    SluiceClientFactory factory = new SluiceClientFactory(conf);
    SluiceClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    SluiceClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }
}
