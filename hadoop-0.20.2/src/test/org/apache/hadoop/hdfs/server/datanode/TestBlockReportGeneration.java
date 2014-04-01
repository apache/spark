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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.AsyncBlockReport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBlockReportGeneration {
  private static final long BLKID = 12345L;
  private static final long GENSTAMP = 1000L;
  private static final long LEN = 65536L;
  
  private static final Block FAKE_BLK =
    new Block(BLKID, LEN, GENSTAMP);

  
  static final File TEST_DIR = new File(
      System.getProperty("test.build.data") + File.pathSeparatorChar
      + "TestBlockReportGeneration");
  
  Map<Block, File> seenOnDisk = new HashMap<Block, File>();
  Map<Block, DatanodeBlockInfo> volumeMap =
      new HashMap<Block, DatanodeBlockInfo>();
  Map<Block, ActiveFile> ongoingCreates = new HashMap<Block, ActiveFile>();;

  
  @Before
  public void cleanupTestDir() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs());
  }
  
  @Test
  public void testEmpty() {
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    assertTrue(seenOnDisk.isEmpty());
  }
  
  /**
   * Test for case where for some reason there's a block on disk
   * that got lost in volumemap - we want to report this.
   */
  @Test
  public void testOnDiskButNotMemory() {
    fakeSeenByScan(FAKE_BLK);
    fakeBlockOnDisk(FAKE_BLK);
    
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should still be in map, since it was seen to still exist on disk
    // (exists returns true)
    assertTrue(seenOnDisk.containsKey(FAKE_BLK));
  }
  
  /**
   * Test for case where we lost a block from disk - eg a user rm -Rfed
   * a data dir accidentally.
   */
  @Test
  public void testInMemoryButNotOnDisk() {
    fakeInVolumeMap(FAKE_BLK);
    
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
    assertTrue(volumeMap.containsKey(FAKE_BLK));
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should not be added to the map, since it's truly not on disk
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
  }
  
  /**
   * Test for case where we concurrently removed a block, so it is
   * seen in scan, but during reconciliation is on longer on disk.
   */
  @Test
  public void testRemovedAfterScan() {
    fakeSeenByScan(FAKE_BLK);
    
    assertTrue(seenOnDisk.containsKey(FAKE_BLK));
    assertFalse(volumeMap.containsKey(FAKE_BLK));
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should be removed from the map since .exists() returns false
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
  }
    
  /**
   * Test for case where we concurrently added a block, so it is
   * not seen in scan, but is in volumeMap and on disk during
   * reconciliation.
   */
  @Test
  public void testAddedAfterScan() {
    fakeInVolumeMap(FAKE_BLK);
    fakeBlockOnDisk(FAKE_BLK);
    
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
    assertTrue(volumeMap.containsKey(FAKE_BLK));
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should be added, since it's found on disk when reconciling
    assertTrue(seenOnDisk.containsKey(FAKE_BLK));
  }
  
  /**
   * Test for case where we concurrently changed the generation stamp
   * of a block. So, we scanned it with one GS, but at reconciliation
   * time it has a new GS.
   */
  @Test
  public void testGenstampChangedAfterScan() {
    Block oldGenStamp = FAKE_BLK;
    Block newGenStamp = new Block(FAKE_BLK);
    newGenStamp.setGenerationStamp(GENSTAMP + 1);

    fakeSeenByScan(oldGenStamp);
    fakeInVolumeMap(newGenStamp);
    fakeBlockOnDisk(newGenStamp);
    
    assertTrue(seenOnDisk.containsKey(oldGenStamp));

    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // old genstamp should not be added
    assertFalse(seenOnDisk.containsKey(oldGenStamp));
    // new genstamp should be added, since it's found on disk when reconciling
    assertTrue(seenOnDisk.containsKey(newGenStamp));
  }
  
  @Test
  public void testGetGenerationStampFromFile() {
    File[] fileList = new File[] {
        // include some junk files which should be ignored
        new File("blk_-1362850638739812068_5351.meta.foo"),
        new File("blk_-1362850638739812068_5351meta"),
        // the real dir
        new File("."),
        new File(".."),
        new File("blk_-1362850638739812068"),
        new File("blk_-1362850638739812068_5351.meta"),
        new File("blk_1453973893701037484"),
        new File("blk_1453973893701037484_4804.meta"),
    };
    
    assertEquals(4804, FSDataset.getGenerationStampFromFile(fileList,
        new File("blk_1453973893701037484")));
    // try a prefix of a good block ID
    assertEquals(Block.GRANDFATHER_GENERATION_STAMP,
        FSDataset.getGenerationStampFromFile(fileList,
            new File("blk_145397389370103")));

    assertEquals(Block.GRANDFATHER_GENERATION_STAMP,
        FSDataset.getGenerationStampFromFile(fileList,
            new File("blk_99999")));
    
    // pass nonsense value
    assertEquals(Block.GRANDFATHER_GENERATION_STAMP,
        FSDataset.getGenerationStampFromFile(fileList,
            new File("blk_")));
  }

  
  /**
   * Test case for blocks being created - these are not seen by the
   * scan since they're in the current/ dir, not bbw/ -- but
   * they are in volumeMap and ongoingCreates. These should not
   * be reported.
   */
  @Test
  public void testFileBeingCreated() {
    fakeInVolumeMap(FAKE_BLK);
    fakeBlockOnDisk(FAKE_BLK);
    fakeBeingCreated(FAKE_BLK);
    
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
    assertTrue(volumeMap.containsKey(FAKE_BLK));
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should not be added, since it's in the midst of being created!
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
  }
  
  /**
   * Test for case where we reopened a block during the scan
   */
  @Test
  public void testReopenedDuringScan() {
    fakeSeenByScan(FAKE_BLK);
    fakeInVolumeMap(FAKE_BLK);
    fakeBeingCreated(FAKE_BLK);
    
    assertTrue(seenOnDisk.containsKey(FAKE_BLK));
    assertTrue(volumeMap.containsKey(FAKE_BLK));
    FSDataset.reconcileRoughBlockScan(seenOnDisk, volumeMap, ongoingCreates);
    // should be removed from the map since .exists() returns false
    assertFalse(seenOnDisk.containsKey(FAKE_BLK));
  }

  @Test(timeout=20000)
  public void testAsyncReport() throws Exception {
    FSDataset mock = Mockito.mock(FSDataset.class);
    AsyncBlockReport abr = new FSDataset.AsyncBlockReport(mock);
    abr.start();
    try {
      for (int i = 0; i < 3; i++) {
        HashMap<Block, File> mockResult = new HashMap<Block, File>();
        Mockito.doReturn(mockResult).when(mock).roughBlockScan();
        
        assertFalse(abr.isReady());
        abr.request();
        while (!abr.isReady()) {
          Thread.sleep(10);
        }
        assertSame(mockResult, abr.getAndReset());
        assertFalse(abr.isReady());
      }      
    } finally {
      abr.shutdown();
    }
  }

  private void fakeBeingCreated(Block b) {
    ongoingCreates.put(b,
        new ActiveFile(blockFile(b), new ArrayList<Thread>()));
  }

  private void fakeInVolumeMap(Block b) {
    volumeMap.put(b, new DatanodeBlockInfo(null, blockFile(b)));
  }

  private void fakeBlockOnDisk(Block b) {
    File f = blockFile(b);
    try {
      f.createNewFile();
      FSDataset.getMetaFile(f, b).createNewFile();
    } catch (IOException e) {
      throw new RuntimeException("Could not create: " + f);
    }
  }
  
  private void fakeSeenByScan(Block b) {
    seenOnDisk.put(b, blockFile(b));
  }

  private File blockFile(Block b) {
    return new File(TEST_DIR, b.getBlockName());
  }
}
