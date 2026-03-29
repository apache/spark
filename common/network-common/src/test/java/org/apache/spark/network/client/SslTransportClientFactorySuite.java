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

import org.junit.jupiter.api.BeforeEach;

import org.apache.spark.network.ssl.SslSampleConfigs;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.TransportContext;

public class SslTransportClientFactorySuite extends TransportClientFactorySuite {

  @BeforeEach
  public void setUp() {
    conf = new TransportConf(
      "shuffle", SslSampleConfigs.createDefaultConfigProviderForRpcNamespace());
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server1 = context.createServer();
    server2 = context.createServer();
  }
}
