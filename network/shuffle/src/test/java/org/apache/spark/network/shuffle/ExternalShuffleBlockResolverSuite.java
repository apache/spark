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
  static String sortBlock0 = "Hello!";
  static String sortBlock1 = "World!";

  static String sortBlock0_1 = "supercali";
  static String sortBlock1_1 = "fragilistic";

  static String hashBlock0 = "Elementary";
  static String hashBlock1 = "Tabular";

  static String hashBlock0_1 = "expiali";
  static String hashBlock1_1 = "docious";


  static TestShuffleDataContext dataContext;

  static TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());

  @BeforeClass
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort and hash data.
    dataContext.insertSortShuffleData(0, 0, 0,
      new byte[][] { sortBlock0.getBytes(), sortBlock1.getBytes() } );
    dataContext.insertSortShuffleData(0, 0, 1,
      new byte[][] { sortBlock0_1.getBytes(), sortBlock1_1.getBytes() } );
    dataContext.insertHashShuffleData(1, 0, 0,
      new byte[][]{hashBlock0.getBytes(), hashBlock1.getBytes()});
    dataContext.insertHashShuffleData(1, 0, 1,
      new byte[][] { hashBlock0_1.getBytes(), hashBlock1_1.getBytes() } );
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
      resolver.getBlockData("app0", "exec1", "shuffle_1_1_0_0");
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));
    }

    // Invalid shuffle manager
    resolver.registerExecutor("app0", "exec2", dataContext.createExecutorInfo("foobar"));
    try {
      resolver.getBlockData("app0", "exec2", "shuffle_1_1_0_0");
      fail("Should have failed");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    // Nonexistent shuffle block
    resolver.registerExecutor("app0", "exec3",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));
    try {
      resolver.getBlockData("app0", "exec3", "shuffle_1_1_0_0");
      fail("Should have failed");
    } catch (Exception e) {
      // pass
    }

    // wrong number of parts (note that we allow a missing stageAttemptId)
    try {
      resolver.getBlockData("app0", "exec1", "shuffle_1_1_0_0_0");
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("Unexpected block id format"));
    }

    try {
      resolver.getBlockData("app0", "exec1", "shuffle_1_1");
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("Unexpected block id format"));
    }

  }

  @Test
  public void testSortShuffleBlocks() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));

    testReadBlockData(resolver, "shuffle_0_0_0_0", sortBlock0);
    testReadBlockData(resolver, "shuffle_0_0_1_0", sortBlock1);
    testReadBlockData(resolver, "shuffle_0_0_0_1", sortBlock0_1);
    testReadBlockData(resolver, "shuffle_0_0_1_1", sortBlock1_1);
  }

  @Test
  public void testHashShuffleBlocks() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.hash.HashShuffleManager"));

    testReadBlockData(resolver, "shuffle_1_0_0_0", hashBlock0);
    testReadBlockData(resolver, "shuffle_1_0_1_0", hashBlock1);
    testReadBlockData(resolver, "shuffle_1_0_0_1", hashBlock0_1);
    testReadBlockData(resolver, "shuffle_1_0_1_1", hashBlock1_1);
  }

  private void testReadBlockData(ExternalShuffleBlockResolver resolver, String blockId,
                                 String expected) throws IOException {
    InputStream blockStream =
      resolver.getBlockData("app0", "exec0", blockId).createInputStream();
    String block0 = CharStreams.toString(new InputStreamReader(blockStream));
    blockStream.close();
    assertEquals(expected, block0);
  }

  @Test
  public void supportLegacySortShuffleBlockIds() throws IOException {
    // In Spark 1.6, the stage attempt ID was added to shuffle block ids (SPARK-8029).  However,
    // during a rolling upgrade, the shuffle service may be restarted with new code but still
    // need to serve old apps.  So we make sure we can still handle old blocks

    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));

    dataContext.insertLegacySortShuffleData(2, 1,
      new byte[][]{"legacy".getBytes(), "block".getBytes()});

    testReadBlockData(resolver, "shuffle_2_1_0", "legacy");
    testReadBlockData(resolver, "shuffle_2_1_1", "block");

    // verify everything still works when we also register some blocks which do have a
    // stageAttemptId
    testSortShuffleBlocks();

    testReadBlockData(resolver, "shuffle_2_1_0", "legacy");
    testReadBlockData(resolver, "shuffle_2_1_1", "block");
  }

  @Test
  public void supportLegacyHashShuffleBlockIds() throws IOException {
    // In Spark 1.6, the stage attempt ID was added to shuffle block ids (SPARK-8029).  However,
    // during a rolling upgrade, the shuffle service may be restarted with new code but still
    // need to serve old apps.  So we make sure we can still handle old blocks

    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo("org.apache.spark.shuffle.hash.HashShuffleManager"));

    dataContext.insertLegacyHashShuffleData(2, 0,
      new byte[][] { "more legacy".getBytes(), "hash".getBytes() } );

    testReadBlockData(resolver, "shuffle_2_0_0", "more legacy");
    testReadBlockData(resolver, "shuffle_2_0_1", "hash");

    // verify everything still works when we also register some blocks which do have a
    // stageAttemptId
    testHashShuffleBlocks();

    testReadBlockData(resolver, "shuffle_2_0_0", "more legacy");
    testReadBlockData(resolver, "shuffle_2_0_1", "hash");
  }

  @Test
  public void jsonSerializationOfExecutorRegistration() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    AppExecId appId = new AppExecId("foo", "bar");
    String appIdJson = mapper.writeValueAsString(appId);
    AppExecId parsedAppId = mapper.readValue(appIdJson, AppExecId.class);
    assertEquals(parsedAppId, appId);

    ExecutorShuffleInfo shuffleInfo =
      new ExecutorShuffleInfo(new String[]{"/bippy", "/flippy"}, 7, "hash");
    String shuffleJson = mapper.writeValueAsString(shuffleInfo);
    ExecutorShuffleInfo parsedShuffleInfo =
      mapper.readValue(shuffleJson, ExecutorShuffleInfo.class);
    assertEquals(parsedShuffleInfo, shuffleInfo);

    // Intentionally keep these hard-coded strings in here, to check backwards-compatability.
    // its not legacy yet, but keeping this here in case anybody changes it
    String legacyAppIdJson = "{\"appId\":\"foo\", \"execId\":\"bar\"}";
    assertEquals(appId, mapper.readValue(legacyAppIdJson, AppExecId.class));
    String legacyShuffleJson = "{\"localDirs\": [\"/bippy\", \"/flippy\"], " +
      "\"subDirsPerLocalDir\": 7, \"shuffleManager\": \"hash\"}";
    assertEquals(shuffleInfo, mapper.readValue(legacyShuffleJson, ExecutorShuffleInfo.class));
  }
}
