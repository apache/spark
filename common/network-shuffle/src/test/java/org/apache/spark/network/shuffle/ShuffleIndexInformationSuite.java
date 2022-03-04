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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class ShuffleIndexInformationSuite {
  private static final String sortBlock0 = "tiny block";
  private static final String sortBlock1 = "a bit longer block";

  private static TestShuffleDataContext dataContext;
  private static String blockId;

  @BeforeClass
  public static void before() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort data.
    blockId = dataContext.insertSortShuffleData(0, 0, new byte[][] {
        sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)});
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void test() throws IOException {
    String path = ExecutorDiskUtils.getFilePath(
      dataContext.localDirs,
      dataContext.subDirsPerLocalDir,
      blockId + ".index");
    ShuffleIndexInformation s = new ShuffleIndexInformation(path);
    // the index file contains 3 offsets:
    //   0, sortBlock0.length, sortBlock0.length + sortBlock1.length
    assertEquals(0L, s.getIndex(0).getOffset());
    assertEquals(sortBlock0.length(), s.getIndex(0).getLength());

    assertEquals(sortBlock0.length(), s.getIndex(1).getOffset());
    assertEquals(sortBlock1.length(), s.getIndex(1).getLength());

    assertEquals((3 * 8) + ShuffleIndexInformation.INSTANCE_MEMORY_FOOTPRINT,
      s.getRetainedMemorySize());
  }
}
