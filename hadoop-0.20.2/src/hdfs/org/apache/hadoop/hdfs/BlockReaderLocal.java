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
package org.apache.hadoop.hdfs;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

/**
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 * {@link BlockReaderLocal} works as follows:
 * <ul>
 * <li>The client performing short circuit reads must be configured at the
 * datanode.</li>
 * <li>The client gets the path to the file where block is stored using
 * {@link ClientDatanodeProtocol#getBlockLocalPathInfo(Block, Token)} RPC call</li>
 * <li>Client uses kerberos authentication to connect to the datanode over RPC,
 * if security is enabled.</li>
 * </ul>
 */
class BlockReaderLocal extends FSInputChecker implements BlockReader {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  //Stores the cache and proxy for a local datanode.
  private static class LocalDatanodeInfo {
    private ClientDatanodeProtocol proxy = null;
    private final Map<Block, BlockLocalPathInfo> cache;

    LocalDatanodeInfo() {
      final int cacheSize = 10000;
      final float hashTableLoadFactor = 0.75f;
      int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
      cache = Collections
          .synchronizedMap(new LinkedHashMap<Block, BlockLocalPathInfo>(
              hashTableCapacity, hashTableLoadFactor, true) {
            private static final long serialVersionUID = 1;

            @Override
            protected boolean removeEldestEntry(
                Map.Entry<Block, BlockLocalPathInfo> eldest) {
              return size() > cacheSize;
            }
          });
    }

    private synchronized ClientDatanodeProtocol getDatanodeProxy(
        DatanodeInfo node, Configuration conf, int socketTimeout,
        boolean connectToDnViaHostname) throws IOException {
      if (proxy == null) {
        proxy = DFSClient.createClientDatanodeProtocolProxy(node, conf,
            socketTimeout, connectToDnViaHostname);
      }
      return proxy;
    }
    
    private synchronized void resetDatanodeProxy() {
      if (null != proxy) {
        RPC.stopProxy(proxy);
        proxy = null;
      }
    }

    private BlockLocalPathInfo getBlockLocalPathInfo(Block b) {
      return cache.get(b);
    }

    private void setBlockLocalPathInfo(Block b, BlockLocalPathInfo info) {
      cache.put(b, info);
    }

    private void removeBlockLocalPathInfo(Block b) {
      cache.remove(b);
    }
  }
  
  // Multiple datanodes could be running on the local machine. Store proxies in
  // a map keyed by the ipc port of the datanode.
  private static Map<Integer, LocalDatanodeInfo> localDatanodeInfoMap = new HashMap<Integer, LocalDatanodeInfo>();

  private FileInputStream dataIn; // reader for the data file
  private FileInputStream checksumIn;   // reader for the checksum file
  private DataChecksum checksum;
  private int bytesPerChecksum;
  private int checksumSize;
  private long firstChunkOffset;
  private long lastChunkLen = -1;
  private long lastChunkOffset = -1;
  private long startOffset;
  private boolean eos = false;
  private byte[] skipBuf = null;

  /**
   * The only way this object can be instantiated.
   */
  static BlockReaderLocal newBlockReader(Configuration conf,
    String file, Block blk, Token<BlockTokenIdentifier> token, DatanodeInfo node, 
    int socketTimeout, long startOffset, long length, boolean connectToDnViaHostname)
    throws IOException {
    
    LocalDatanodeInfo localDatanodeInfo =  getLocalDatanodeInfo(node.getIpcPort());
    // check the cache first
    BlockLocalPathInfo pathinfo = localDatanodeInfo.getBlockLocalPathInfo(blk);
    if (pathinfo == null) {
      pathinfo = getBlockPathInfo(blk, node, conf, socketTimeout, token, connectToDnViaHostname);
    }

    // check to see if the file exists. It may so happen that the
    // HDFS file has been deleted and this block-lookup is occurring
    // on behalf of a new HDFS file. This time, the block file could
    // be residing in a different portion of the fs.data.dir directory.
    // In this case, we remove this entry from the cache. The next
    // call to this method will re-populate the cache.
    FileInputStream dataIn = null;
    FileInputStream checksumIn = null;
    BlockReaderLocal localBlockReader = null;
    boolean skipChecksum = shouldSkipChecksum(conf);
    try {
      // get a local file system
      File blkfile = new File(pathinfo.getBlockPath());
      dataIn = new FileInputStream(blkfile);

      if (LOG.isDebugEnabled()) {
        LOG.debug("New BlockReaderLocal for file " + blkfile + " of size "
            + blkfile.length() + " startOffset " + startOffset + " length "
            + length + " short circuit checksum " + skipChecksum);
      }

      if (!skipChecksum) {
        // get the metadata file
        File metafile = new File(pathinfo.getMetaPath());
        checksumIn = new FileInputStream(metafile);

        // read and handle the common header here. For now just a version
        BlockMetadataHeader header = BlockMetadataHeader
            .readHeader(new DataInputStream(checksumIn));
        short version = header.getVersion();
        if (version != FSDataset.METADATA_VERSION) {
          LOG.warn("Wrong version (" + version + ") for metadata file for "
              + blk + " ignoring ...");
        }
        DataChecksum checksum = header.getChecksum();
        localBlockReader = new BlockReaderLocal(conf, file, blk, token, startOffset, length,
            pathinfo, checksum, true, dataIn, checksumIn);
      } else {
        localBlockReader = new BlockReaderLocal(conf, file, blk, token, startOffset, length,
            pathinfo, dataIn);
      }
    } catch (IOException e) {
      // remove from cache
      localDatanodeInfo.removeBlockLocalPathInfo(blk);
      DFSClient.LOG.warn("BlockReaderLocal: Removing " + blk +
          " from cache because local file " + pathinfo.getBlockPath() +
          " could not be opened.");
      throw e;
    } finally {
      if (localBlockReader == null) {
        if (dataIn != null) {
          dataIn.close();
        }
        if (checksumIn != null) {
          checksumIn.close();
        }
      }  
    }
    return localBlockReader;
  }
  
  private static synchronized LocalDatanodeInfo getLocalDatanodeInfo(int port) {
    LocalDatanodeInfo ldInfo = localDatanodeInfoMap.get(port);
    if (ldInfo == null) {
      ldInfo = new LocalDatanodeInfo();
      localDatanodeInfoMap.put(port, ldInfo);
    }
    return ldInfo;
  }
  
  private static BlockLocalPathInfo getBlockPathInfo(Block blk,
      DatanodeInfo node, Configuration conf, int timeout,
      Token<BlockTokenIdentifier> token, boolean connectToDnViaHostname) 
      throws IOException {
    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node.ipcPort);
    BlockLocalPathInfo pathinfo = null;
    ClientDatanodeProtocol proxy = localDatanodeInfo.getDatanodeProxy(node,
        conf, timeout, connectToDnViaHostname);
    try {
      // make RPC to local datanode to find local pathnames of blocks
      pathinfo = proxy.getBlockLocalPathInfo(blk, token);
      if (pathinfo != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cached location of block " + blk + " as " + pathinfo);
        }
        localDatanodeInfo.setBlockLocalPathInfo(blk, pathinfo);
      }
    } catch (IOException e) {
      localDatanodeInfo.resetDatanodeProxy(); // Reset proxy on error
      throw e;
    }
    return pathinfo;
  }
  
  private static boolean shouldSkipChecksum(Configuration conf) {
    return conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT);
  }
  
  private BlockReaderLocal(Configuration conf, String hdfsfile, Block block,
      Token<BlockTokenIdentifier> token, long startOffset, long length,
      BlockLocalPathInfo pathinfo, FileInputStream dataIn) throws IOException {
    super(
        new Path("/blk_" + block.getBlockId() + ":of:" + hdfsfile) /*too non path-like?*/,
        1);
    this.dataIn = dataIn;
    this.startOffset = startOffset;
    long toSkip = startOffset;
    while (toSkip > 0) {
      long skipped = dataIn.skip(toSkip);
      if (skipped == 0) {
        throw new IOException("Couldn't initialize input stream");
      }
      toSkip -= skipped;
    }
  }

  private BlockReaderLocal(Configuration conf, String hdfsfile, Block block,
      Token<BlockTokenIdentifier> token, long startOffset, long length,
      BlockLocalPathInfo pathinfo, DataChecksum checksum, boolean verifyChecksum,
      FileInputStream dataIn, FileInputStream checksumIn) throws IOException {
    super(
        new Path("/blk_" + block.getBlockId() + ":of:" + hdfsfile) /*too non path-like?*/,
        1,
        verifyChecksum,
        checksum.getChecksumSize() > 0? checksum : null,
            checksum.getBytesPerChecksum(),
            checksum.getChecksumSize());
    this.dataIn = dataIn;
    this.startOffset = startOffset;
    this.checksumIn = checksumIn;
    this.checksum = checksum;

    long blockLength = pathinfo.getNumBytes();

    /* If bytesPerChecksum is very large, then the metadata file
     * is mostly corrupted. For now just truncate bytesPerchecksum to
     * blockLength.
     */
    bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
      checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
          Math.max((int) blockLength, 10 * 1024 * 1024));
      bytesPerChecksum = checksum.getBytesPerChecksum();
    }

    checksumSize = checksum.getChecksumSize();

    if (startOffset < 0 || startOffset > blockLength
        || (length + startOffset) > blockLength) {
      String msg = " Offset " + startOffset + " and length " + length
      + " don't match block " + block + " ( blockLen " + blockLength + " )";
      LOG.warn("BlockReaderLocal requested with incorrect offset: " + msg);
      throw new IOException(msg);
    }

    firstChunkOffset = (startOffset - (startOffset % bytesPerChecksum));

    if (firstChunkOffset > 0) {
      dataIn.getChannel().position(firstChunkOffset);

      long checksumSkip = (firstChunkOffset / bytesPerChecksum) * checksumSize;
      if (checksumSkip > 0) {
        checksumIn.skip(checksumSkip);
      }
    }

    lastChunkOffset = firstChunkOffset;
    lastChunkLen = -1;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read off " + off + " len " + len);
    }
    if (checksum == null) {
      return dataIn.read(buf, off, len);
    }
    // For the first read, skip the extra bytes at the front.
    if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
      // Skip these bytes. But don't call this.skip()!
      int toSkip = (int)(startOffset - firstChunkOffset);
      if (skipBuf == null) {
        skipBuf = new byte[bytesPerChecksum];
      }
      if (super.read(skipBuf, 0, toSkip) != toSkip) {
        // Should never happen
        throw new IOException("Could not skip " + toSkip + " bytes");
      }
    }
    return super.read(buf, off, len);
  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return readFully(this, buf, offset, len);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("skip " + n);
    }
    if (checksum == null) {
      return dataIn.skip(n);
    }
    // Skip by reading the data so we stay in sync with checksums.
    // This could be implemented more efficiently in the future to
    // skip to the beginning of the appropriate checksum chunk
    // and then only read to the middle of that chunk.
    if (skipBuf == null) {
      skipBuf = new byte[bytesPerChecksum]; 
    }
    long nSkipped = 0;
    while (nSkipped < n) {
      int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
      int ret = read(skipBuf, 0, toSkip);
      if (ret <= 0) {
        return nSkipped;
      }
      nSkipped += ret;
    }
    return nSkipped;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("read() is not expected to be invoked. " +
                          "Use read(buf, off, len) instead.");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    // Checksum errors are handled outside BlockReaderLocal 
    return false;
  }

  @Override
  public synchronized void seek(long n) throws IOException {
    throw new IOException("Seek() is not supported in BlockReaderLocal");
  }

  @Override
  protected long getChunkPosition(long pos) {
    throw new RuntimeException("getChunkPosition() is not supported, " +
      "since seek is not implemented");
  }

  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset,
      int len, byte[] checksumBuf) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading chunk from position " + pos + " at offset " +
          offset + " with length " + len);
    }

    if (eos) {
      startOffset = -1;
      return -1;
    }

    if (checksumBuf.length != checksumSize) {
      throw new IOException("Cannot read checksum into buffer. "
          + "The buffer must be exactly '" + checksumSize
          + "' bytes long to hold the checksum bytes.");
    }

    if ((pos + firstChunkOffset) != lastChunkOffset) {
      throw new IOException("Mismatch in pos : " + pos + " + "
          + firstChunkOffset + " != " + lastChunkOffset);
    }

    int nRead = dataIn.read(buf, offset, bytesPerChecksum);
    if (nRead < bytesPerChecksum) {
      eos = true;
    }

    lastChunkOffset += nRead;
    lastChunkLen = nRead;

    // If verifyChecksum is false, we omit reading the checksum
    if (checksumIn != null) {
      int nChecksumRead = checksumIn.read(checksumBuf);
      if (nChecksumRead != checksumSize) {
        throw new IOException("Could not read checksum at offset " +
            checksumIn.getChannel().position() + " from the meta file.");
      }
    }

    return nRead;
  }

  @Override
  public synchronized void close() throws IOException {
    IOUtils.closeStream(dataIn);
    IOUtils.closeStream(checksumIn);
  }
}