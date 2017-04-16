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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * This class implements a simulated FSDataset.
 * 
 * Blocks that are created are recorded but their data (plus their CRCs) are
 *  discarded.
 * Fixed data is returned when blocks are read; a null CRC meta file is
 * created for such data.
 * 
 * This FSDataset does not remember any block information across its
 * restarts; it does however offer an operation to inject blocks
 *  (See the TestInectionForSImulatedStorage()
 * for a usage example of injection.
 * 
 * Note the synchronization is coarse grained - it is at each method. 
 */

public class SimulatedFSDataset  implements FSConstants, FSDatasetInterface, Configurable{
  
  public static final String CONFIG_PROPERTY_SIMULATED =
                                    "dfs.datanode.simulateddatastorage";
  public static final String CONFIG_PROPERTY_CAPACITY =
                            "dfs.datanode.simulateddatastorage.capacity";
  
  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte
  public static final byte DEFAULT_DATABYTE = 9; // 1 terabyte
  byte simulatedDataByte = DEFAULT_DATABYTE;
  Configuration conf = null;
  
  static byte[] nullCrcFileData;

  {
    DataChecksum checksum = DataChecksum.newDataChecksum( DataChecksum.
                              CHECKSUM_NULL, 16*1024 );
    byte[] nullCrcHeader = checksum.getHeader();
    nullCrcFileData =  new byte[2 + nullCrcHeader.length];
    nullCrcFileData[0] = (byte) ((FSDataset.METADATA_VERSION >>> 8) & 0xff);
    nullCrcFileData[1] = (byte) (FSDataset.METADATA_VERSION & 0xff);
    for (int i = 0; i < nullCrcHeader.length; i++) {
      nullCrcFileData[i+2] = nullCrcHeader[i];
    }
  }
  
  private class BInfo { // information about a single block
    Block theBlock;
    private boolean finalized = false; // if not finalized => ongoing creation
    SimulatedOutputStream oStream = null;
    BInfo(Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0) {
        theBlock.setNumBytes(0);
      }
      if (!storage.alloc(theBlock.getNumBytes())) { // expected length - actual length may
                                          // be more - we find out at finalize
        DataNode.LOG.warn("Lack of free storage on a block alloc");
        throw new IOException("Creating block, no free space available");
      }

      if (forWriting) {
        finalized = false;
        oStream = new SimulatedOutputStream();
      } else {
        finalized = true;
        oStream = null;
      }
    }

    synchronized long getGenerationStamp() {
      return theBlock.getGenerationStamp();
    }

    synchronized void updateBlock(Block b) {
      theBlock.setGenerationStamp(b.getGenerationStamp());
      setlength(b.getNumBytes());
    }
    
    synchronized long getlength() {
      if (!finalized) {
         return oStream.getLength();
      } else {
        return theBlock.getNumBytes();
      }
    }

    synchronized void setlength(long length) {
      if (!finalized) {
         oStream.setLength(length);
      } else {
        theBlock.setNumBytes(length);
      }
    }
    
    synchronized SimulatedInputStream getIStream() throws IOException {
      if (!finalized) {
        // throw new IOException("Trying to read an unfinalized block");
         return new SimulatedInputStream(oStream.getLength(), DEFAULT_DATABYTE);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), DEFAULT_DATABYTE);
      }
    }
    
    synchronized void finalizeBlock(long finalSize) throws IOException {
      if (finalized) {
        throw new IOException(
            "Finalizing a block that has already been finalized" + 
            theBlock.getBlockId());
      }
      if (oStream == null) {
        DataNode.LOG.error("Null oStream on unfinalized block - bug");
        throw new IOException("Unexpected error on finalize");
      }

      if (oStream.getLength() != finalSize) {
        DataNode.LOG.warn("Size passed to finalize (" + finalSize +
                    ")does not match what was written:" + oStream.getLength());
        throw new IOException(
          "Size passed to finalize does not match the amount of data written");
      }
      // We had allocated the expected length when block was created; 
      // adjust if necessary
      long extraLen = finalSize - theBlock.getNumBytes();
      if (extraLen > 0) {
        if (!storage.alloc(extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        storage.free(-extraLen);
      }
      theBlock.setNumBytes(finalSize);  

      finalized = true;
      oStream = null;
      return;
    }
    
    SimulatedInputStream getMetaIStream() {
      return new SimulatedInputStream(nullCrcFileData);  
    }

    synchronized boolean isFinalized() {
      return finalized;
    }
  }
  
  static private class SimulatedStorage {
    private long capacity;  // in bytes
    private long used;    // in bytes
    
    synchronized long getFree() {
      return capacity - used;
    }
    
    synchronized long getCapacity() {
      return capacity;
    }
    
    synchronized long getUsed() {
      return used;
    }
    
    int getNumFailedVolumes() {
      return 0;
    }

    synchronized boolean alloc(long amount) {
      if (getFree() >= amount) {
        used += amount;
        return true;
      } else {
        return false;    
      }
    }
    
    synchronized void free(long amount) {
      used -= amount;
    }
    
    SimulatedStorage(long cap) {
      capacity = cap;
      used = 0;   
    }
  }
  
  private HashMap<Block, BInfo> blockMap = null;
  private SimulatedStorage storage = null;
  private String storageId;
  
  public SimulatedFSDataset(Configuration conf) throws IOException {
    setConf(conf);
  }
  
  private SimulatedFSDataset() { // real construction when setConf called.. Uggg
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration iconf)  {
    conf = iconf;
    storageId = conf.get("StorageId", "unknownStorageId" +
                                        new Random().nextInt());
    registerMBean(storageId);
    storage = new SimulatedStorage(
        conf.getLong(CONFIG_PROPERTY_CAPACITY, DEFAULT_CAPACITY));
    //DataNode.LOG.info("Starting Simulated storage; Capacity = " + getCapacity() + 
    //    "Used = " + getDfsUsed() + "Free =" + getRemaining());

    blockMap = new HashMap<Block,BInfo>(); 
  }

  public synchronized void injectBlocks(Block[] injectBlocks)
                                            throws IOException {
    if (injectBlocks != null) {
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        if (isValidBlock(b)) {
          throw new IOException("Block already exists in  block list");
        }
      }
      HashMap<Block, BInfo> oldBlockMap = blockMap;
      blockMap = 
          new HashMap<Block,BInfo>(injectBlocks.length + oldBlockMap.size());
      blockMap.putAll(oldBlockMap);
      for (Block b: injectBlocks) {
          BInfo binfo = new BInfo(b, false);
          blockMap.put(b, binfo);
      }
    }
  }

  @Override
  public void finalizeBlock(Block b) throws IOException {
    finalizeBlockInternal(b, false);
  }

  @Override
  public void finalizeBlockIfNeeded(Block b) throws IOException {
    finalizeBlockInternal(b, true);    
  }

  private synchronized void finalizeBlockInternal(Block b, boolean refinalizeOk) 
    throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(b.getNumBytes());

  }

  public synchronized void unfinalizeBlock(Block b) throws IOException {
    if (isBeingWritten(b)) {
      blockMap.remove(b);
    }
  }

  public synchronized Block[] getBlockReport() {
    Block[] blockTable = new Block[blockMap.size()];
    int count = 0;
    for (BInfo b : blockMap.values()) {
      if (b.isFinalized()) {
        blockTable[count++] = b.theBlock;
      }
    }
    if (count != blockTable.length) {
      blockTable = Arrays.copyOf(blockTable, count);
    }
    return blockTable;
  }
  
  @Override
  public void requestAsyncBlockReport() {
  }

  @Override
  public boolean isAsyncBlockReportReady() {
    return true;
  }

  @Override
  public Block[] retrieveAsyncBlockReport() {
    return getBlockReport();
  }


  public long getCapacity() throws IOException {
    return storage.getCapacity();
  }

  public long getDfsUsed() throws IOException {
    return storage.getUsed();
  }

  public long getRemaining() throws IOException {
    return storage.getFree();
  }

  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {
    return storage.getNumFailedVolumes();
  }

  public synchronized long getLength(Block b) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    return binfo.getlength();
  }

  @Override
  public long getVisibleLength(Block b) throws IOException {
    return getLength(b);
  }

  @Override
  public void setVisibleLength(Block b, long length) throws IOException {
    //no-op
  }

  /** {@inheritDoc} */
  public Block getStoredBlock(long blkid) throws IOException {
    Block b = new Block(blkid);
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return null;
    }
    b.setGenerationStamp(binfo.getGenerationStamp());
    b.setNumBytes(binfo.getlength());
    return b;
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock) throws IOException {
    BInfo binfo = blockMap.get(newblock);
    if (binfo == null) {
      throw new IOException("BInfo not found, b=" + newblock);
    }
    binfo.updateBlock(newblock);
  }

  public synchronized void invalidate(Block[] invalidBlks) throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      BInfo binfo = blockMap.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      storage.free(binfo.getlength());
      blockMap.remove(b);
    }
      if (error) {
          throw new IOException("Invalidate: Missing blocks.");
      }
  }

  public synchronized boolean isValidBlock(Block b) {
    // return (blockMap.containsKey(b));
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return false;
    }
    return binfo.isFinalized();
  }

  /* check if a block is created but not finalized */
  private synchronized boolean isBeingWritten(Block b) {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      return false;
    }
    return !binfo.isFinalized();  
  }
  
  public String toString() {
    return getStorageInfo();
  }

  public synchronized BlockWriteStreams writeToBlock(Block b, 
                                            boolean isRecovery,
                                            boolean isReplicationRequest)
                                            throws IOException {
    if (isValidBlock(b)) {
          throw new BlockAlreadyExistsException("Block " + b + 
              " is valid, and cannot be written to.");
      }
    if (isBeingWritten(b)) {
        throw new BlockAlreadyExistsException("Block " + b + 
            " is being written, and cannot be written to.");
    }
      BInfo binfo = new BInfo(b, true);
      blockMap.put(b, binfo);
      SimulatedOutputStream crcStream = new SimulatedOutputStream();
      return new BlockWriteStreams(binfo.oStream, crcStream);
  }

  public synchronized InputStream getBlockInputStream(Block b)
                                            throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    
    //DataNode.LOG.info("Opening block(" + b.blkid + ") of length " + b.len);
    return binfo.getIStream();
  }
  
  public synchronized InputStream getBlockInputStream(Block b, long seekOffset)
                              throws IOException {
    InputStream result = getBlockInputStream(b);
    result.skip(seekOffset);
    return result;
  }

  /** Not supported */
  public BlockInputStreams getTmpInputStreams(Block b, long blkoff, long ckoff
      ) throws IOException {
    throw new IOException("Not supported");
  }

  /** No-op */
  public void validateBlockMetadata(Block b) {
  }

  /**
   * Returns metaData of block b as an input stream
   * @param b - the block for which the metadata is desired
   * @return metaData of block b as an input stream
   * @throws IOException - block does not exist or problems accessing
   *  the meta file
   */
  private synchronized InputStream getMetaDataInStream(Block b)
                                              throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b + 
          " is being written, its meta cannot be read");
    }
    return binfo.getMetaIStream();
  }

  public synchronized long getMetaDataLength(Block b) throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its metalength cannot be read");
    }
    return binfo.getMetaIStream().getLength();
  }
  
  public MetaDataInputStream getMetaDataInputStream(Block b)
  throws IOException {

       return new MetaDataInputStream(getMetaDataInStream(b),
                                                getMetaDataLength(b));
  }

  public synchronized boolean metaFileExists(Block b) throws IOException {
    if (!isValidBlock(b)) {
          throw new IOException("Block " + b +
              " is valid, and cannot be written to.");
      }
    return true; // crc exists for all valid blocks
  }

  public void checkDataDir() throws DiskErrorException {
    // nothing to check for simulated data set
  }

  public synchronized long getChannelPosition(Block b, 
                                              BlockWriteStreams stream)
                                              throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }
    return binfo.getlength();
  }

  public synchronized void setChannelPosition(Block b, BlockWriteStreams stream, 
                                              long dataOffset, long ckOffset)
                                              throws IOException {
    BInfo binfo = blockMap.get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }
    binfo.setlength(dataOffset);
  }

  /** 
   * Simulated input and output streams
   *
   */
  static private class SimulatedInputStream extends java.io.InputStream {
    

    byte theRepeatedData = 7;
    long length; // bytes
    int currentPos = 0;
    byte[] data = null;
    
    /**
     * An input stream of size l with repeated bytes
     * @param l
     * @param iRepeatedData
     */
    SimulatedInputStream(long l, byte iRepeatedData) {
      length = l;
      theRepeatedData = iRepeatedData;
    }
    
    /**
     * An input stream of of the supplied data
     * 
     * @param iData
     */
    SimulatedInputStream(byte[] iData) {
      data = iData;
      length = data.length;
      
    }
    
    /**
     * 
     * @return the lenght of the input stream
     */
    long getLength() {
      return length;
    }

    @Override
    public int read() throws IOException {
      if (currentPos >= length)
        return -1;
      if (data !=null) {
        return data[currentPos++];
      } else {
        currentPos++;
        return theRepeatedData;
      }
    }
    
    @Override
    public int read(byte[] b) throws IOException { 

      if (b == null) {
        throw new NullPointerException();
      }
      if (b.length == 0) {
        return 0;
      }
      if (currentPos >= length) { // EOF
        return -1;
      }
      int bytesRead = (int) Math.min(b.length, length-currentPos);
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i : b) {  
          b[i] = theRepeatedData;
        }
      }
      currentPos += bytesRead;
      return bytesRead;
    }
  }
  
  /**
   * This class implements an output stream that merely throws its data away, but records its
   * length.
   *
   */
  static private class SimulatedOutputStream extends OutputStream {
    long length = 0;
    
    /**
     * constructor for Simulated Output Steram
     */
    SimulatedOutputStream() {
    }
    
    /**
     * 
     * @return the length of the data created so far.
     */
    long getLength() {
      return length;
    }

    /**
     */
    void setLength(long length) {
      this.length = length;
    }
    
    @Override
    public void write(int arg0) throws IOException {
      length++;
    }
    
    @Override
    public void write(byte[] b) throws IOException {
      length += b.length;
    }
    
    @Override
    public void write(byte[] b,
              int off,
              int len) throws IOException  {
      length += len;
    }
  }
  
  private ObjectName mbeanName;


  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   *  We use storage id for MBean name since a minicluster within a single
   * Java VM may have multiple Simulated Datanodes.
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;

    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean("DataNode",
          "FSDatasetState-" + storageId, bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  public String getStorageInfo() {
    return "Simulated FSDataset-" + storageId;
  }
  
  public boolean hasEnoughResources() {
    return true;
  }

  @Override
  public BlockRecoveryInfo startBlockRecovery(long blockId)
      throws IOException {
    Block stored = getStoredBlock(blockId);
    return new BlockRecoveryInfo(stored, false);
  }

  @Override
  public Block[] getBlocksBeingWrittenReport() {
    return new Block[0];
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(Block blk) throws IOException {
    throw new IOException("getBlockLocalPathInfo not supported.");
  }
}
