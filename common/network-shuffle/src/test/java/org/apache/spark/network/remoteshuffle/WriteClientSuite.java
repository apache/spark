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

package org.apache.spark.network.remoteshuffle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class WriteClientSuite {
  private ShuffleServer server;

  @Before
  public void beforeEach() {
    server = new ShuffleServer(new HashMap<>());
    server.start();
  }

  @After
  public void afterEach() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testConnect() throws IOException, InterruptedException {
    int port = server.getBoundPort();
    long timeoutMs = 30000;
    ShuffleStageFqid shuffleStageFqid = new ShuffleStageFqid("app1", "1", 2, 3);
    try (WriteClient client = new WriteClient("localhost", port, timeoutMs, shuffleStageFqid, 4, new HashMap<>())) {
      client.connect();
    }
  }
}
