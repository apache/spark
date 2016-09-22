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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExternalShuffleBlockResolverSuite {
  private static final String sortBlock0 = "Hello!";
  private static final String sortBlock1 = "World!";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static TestShuffleDataContext dataContext;

  private static final TransportConf conf =
      new TransportConf("shuffle", new SystemPropertyConfigProvider());

  @BeforeClass
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0, new byte[][] {
        sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)});
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void testBadRequests() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    // Unregistered executor
    try {
      resolver.getBlockData("app0", "exec1", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));
    }

    // Invalid shuffle manager
    try {
      resolver.registerExecutor("app0", "exec2", dataContext.createExecutorInfo("foobar"));
      resolver.getBlockData("app0", "exec2", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    // Nonexistent shuffle block
    resolver.registerExecutor("app0", "exec3",
      dataContext.createExecutorInfo(SORT_MANAGER));
    try {
      resolver.getBlockData("app0", "exec3", "shuffle_1_1_0");
      fail("Should have failed");
    } catch (Exception e) {
      // pass
    }
  }

  @Test
  public void testSortShuffleBlocks() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo(SORT_MANAGER));

    InputStream block0Stream =
      resolver.getBlockData("app0", "exec0", "shuffle_0_0_0").createInputStream();
    String block0 = CharStreams.toString(
        new InputStreamReader(block0Stream, StandardCharsets.UTF_8));
    block0Stream.close();
    assertEquals(sortBlock0, block0);

    InputStream block1Stream =
      resolver.getBlockData("app0", "exec0", "shuffle_0_0_1").createInputStream();
    String block1 = CharStreams.toString(
        new InputStreamReader(block1Stream, StandardCharsets.UTF_8));
    block1Stream.close();
    assertEquals(sortBlock1, block1);
  }

  @Test
  public void jsonSerializationOfExecutorRegistration() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    AppExecId appId = new AppExecId("foo", "bar");
    String appIdJson = mapper.writeValueAsString(appId);
    AppExecId parsedAppId = mapper.readValue(appIdJson, AppExecId.class);
    assertEquals(parsedAppId, appId);

    ExecutorShuffleInfo shuffleInfo =
      new ExecutorShuffleInfo(new String[]{"/bippy", "/flippy"}, 7, SORT_MANAGER);
    String shuffleJson = mapper.writeValueAsString(shuffleInfo);
    ExecutorShuffleInfo parsedShuffleInfo =
      mapper.readValue(shuffleJson, ExecutorShuffleInfo.class);
    assertEquals(parsedShuffleInfo, shuffleInfo);

    // Intentionally keep these hard-coded strings in here, to check backwards-compatability.
    // its not legacy yet, but keeping this here in case anybody changes it
    String legacyAppIdJson = "{\"appId\":\"foo\", \"execId\":\"bar\"}";
    assertEquals(appId, mapper.readValue(legacyAppIdJson, AppExecId.class));
    String legacyShuffleJson = "{\"localDirs\": [\"/bippy\", \"/flippy\"], " +
      "\"subDirsPerLocalDir\": 7, \"shuffleManager\": " + "\"" + SORT_MANAGER + "\"}";
    assertEquals(shuffleInfo, mapper.readValue(legacyShuffleJson, ExecutorShuffleInfo.class));
  }
}
