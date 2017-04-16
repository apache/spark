/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

public class TestDataXceiver {
  static BlockReaderTestUtil util = null;
  static final Path TEST_FILE = new Path("/test.file");
  static final int FILE_SIZE_K = 256;
  static LocatedBlock testBlock = null;

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int REPLICATION_FACTOR = 1;
    util = new BlockReaderTestUtil(REPLICATION_FACTOR);
    util.writeFile(TEST_FILE, FILE_SIZE_K);
    List<LocatedBlock> blkList = util.getFileBlocks(TEST_FILE, FILE_SIZE_K);
    testBlock = blkList.get(0);     // Use the first block to test
  }

  /**
   * Test that we don't call verifiedByClient() when the client only
   * reads a partial block.
   */
  @Test
  public void testCompletePartialRead() throws Exception {
    // Ask for half the file
    BlockReader reader = util.getBlockReader(testBlock, 0, FILE_SIZE_K * 1024 / 2);
    DataNode dn = util.getDataNode(testBlock);
    DataBlockScanner scanner = spy(dn.blockScanner);
    dn.blockScanner = scanner;

    // And read half the file
    util.readCasually(reader, FILE_SIZE_K * 1024 / 2, true);
    verify(scanner, never()).verifiedByClient(Mockito.isA(Block.class));
    reader.close();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdown();
  }
}
