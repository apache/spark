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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.util.DataChecksum;

/**
 * this class tests the methods of the  SimulatedFSDataset.
 *
 */

public class TestSimulatedFSDataset extends TestCase {
  
  Configuration conf = null;
  

  
  static final int NUMBLOCKS = 20;
  static final int BLOCK_LENGTH_MULTIPLIER = 79;

  protected void setUp() throws Exception {
    super.setUp();
      conf = new Configuration();
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
 
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  long blockIdToLen(long blkid) {
    return blkid*BLOCK_LENGTH_MULTIPLIER;
  }
  
  int addSomeBlocks(FSDatasetInterface fsdataset, int startingBlockId) throws IOException {
    int bytesAdded = 0;
    for (int i = startingBlockId; i < startingBlockId+NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0); // we pass expected len as zero, - fsdataset should use the sizeof actual data written
      OutputStream dataOut  = fsdataset.writeToBlock(b, false, false).dataOut;
      assertEquals(0, fsdataset.getLength(b));
      for (int j=1; j <= blockIdToLen(i); ++j) {
        dataOut.write(j);
        assertEquals(j, fsdataset.getLength(b)); // correct length even as we write
        bytesAdded++;
      }
      dataOut.close();
      b.setNumBytes(blockIdToLen(i));
      fsdataset.finalizeBlock(b);
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
    }
    return bytesAdded;  
  }
  int addSomeBlocks(FSDatasetInterface fsdataset ) throws IOException {
    return addSomeBlocks(fsdataset, 1);
  }

  public void testGetMetaData() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block b = new Block(1, 5, 0);
    try {
      assertFalse(fsdataset.metaFileExists(b));
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    addSomeBlocks(fsdataset); // Only need to add one but ....
    b = new Block(1, 0, 0);
    InputStream metaInput = fsdataset.getMetaDataInputStream(b);
    DataInputStream metaDataInput = new DataInputStream(metaInput);
    short version = metaDataInput.readShort();
    assertEquals(FSDataset.METADATA_VERSION, version);
    DataChecksum checksum = DataChecksum.newDataChecksum(metaDataInput);
    assertEquals(DataChecksum.CHECKSUM_NULL, checksum.getChecksumType());
    assertEquals(0, checksum.getChecksumSize());  
  }


  public void testStorageUsage() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    assertEquals(fsdataset.getDfsUsed(), 0);
    assertEquals(fsdataset.getRemaining(), fsdataset.getCapacity());
    int bytesAdded = addSomeBlocks(fsdataset);
    assertEquals(bytesAdded, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded,  fsdataset.getRemaining());
    
  }



  void  checkBlockDataAndSize(FSDatasetInterface fsdataset, 
              Block b, long expectedLen) throws IOException { 
    InputStream input = fsdataset.getBlockInputStream(b);
    long lengthRead = 0;
    int data;
    while ((data = input.read()) != -1) {
      assertEquals(SimulatedFSDataset.DEFAULT_DATABYTE, data);
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);
  }
  
  public void testWriteRead() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    addSomeBlocks(fsdataset);
    for (int i=1; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(b));
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
      checkBlockDataAndSize(fsdataset, b, blockIdToLen(i));
    }
  }



  public void testGetBlockReport() throws IOException, InterruptedException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block[] blockReport = fsdataset.getBlockReport();
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
  }
  public void testInjectionEmpty() throws IOException, InterruptedException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block[] blockReport = fsdataset.getBlockReport();
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    
    // Inject blocks into an empty fsdataset
    //  - injecting the blocks we got above.
  
   
    SimulatedFSDataset sfsdataset = new SimulatedFSDataset(conf);
    sfsdataset.injectBlocks(blockReport);
    blockReport = sfsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset.getLength(b));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded, sfsdataset.getRemaining());
  }

  public void testInjectionNonEmpty() throws IOException, InterruptedException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    
    Block[] blockReport = fsdataset.getBlockReport();
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    fsdataset = null;
    
    // Inject blocks into an non-empty fsdataset
    //  - injecting the blocks we got above.
  
   
    SimulatedFSDataset sfsdataset = new SimulatedFSDataset(conf);
    // Add come blocks whose block ids do not conflict with
    // the ones we are going to inject.
    bytesAdded += addSomeBlocks(sfsdataset, NUMBLOCKS+1);
    Block[] blockReport2 = sfsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    blockReport2 = sfsdataset.getBlockReport();
    assertEquals(NUMBLOCKS, blockReport.length);
    sfsdataset.injectBlocks(blockReport);
    blockReport = sfsdataset.getBlockReport();
    assertEquals(NUMBLOCKS*2, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset.getLength(b));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded,  sfsdataset.getRemaining());
    
    
    // Now test that the dataset cannot be created if it does not have sufficient cap

    conf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY, 10);
 
    try {
      sfsdataset = new SimulatedFSDataset(conf);
      sfsdataset.injectBlocks(blockReport);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }

  }

  public void checkInvalidBlock(Block b) throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    assertFalse(fsdataset.isValidBlock(b));
    try {
      fsdataset.getLength(b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.getBlockInputStream(b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.finalizeBlock(b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
  }
  
  public void testInValidBlocks() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block b = new Block(1, 5, 0);
    checkInvalidBlock(b);
    
    // Now check invlaid after adding some blocks
    addSomeBlocks(fsdataset);
    b = new Block(NUMBLOCKS + 99, 5, 0);
    checkInvalidBlock(b);
    
  }

  public void testInvalidate() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    int bytesAdded = addSomeBlocks(fsdataset);
    Block[] deleteBlocks = new Block[2];
    deleteBlocks[0] = new Block(1, 0, 0);
    deleteBlocks[1] = new Block(2, 0, 0);
    fsdataset.invalidate(deleteBlocks);
    checkInvalidBlock(deleteBlocks[0]);
    checkInvalidBlock(deleteBlocks[1]);
    long sizeDeleted = blockIdToLen(1) + blockIdToLen(2);
    assertEquals(bytesAdded-sizeDeleted, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded+sizeDeleted,  fsdataset.getRemaining());
    
    
    
    // Now make sure the rest of the blocks are valid
    for (int i=3; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(b));
    }
  }

}
