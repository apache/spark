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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (0%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for five consecutive iterations;
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for 3 iterations. Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

public class Balancer implements Tool {
  private static final Log LOG = 
    LogFactory.getLog(Balancer.class.getName());
  final private static long MAX_BLOCKS_SIZE_TO_FETCH = 2*1024*1024*1024L; //2GB

  /** The maximum number of concurrent blocks moves for 
   * balancing purpose at a datanode
   */
  public static final int MAX_NUM_CONCURRENT_MOVES = 5;
  
  private Configuration conf;

  private double threshold = 10D;
  private NamenodeProtocol namenode;
  private ClientProtocol client;
  private FileSystem fs;
  private boolean isBlockTokenEnabled;
  private boolean shouldRun;
  private long keyUpdaterInterval;
  private BlockTokenSecretManager blockTokenSecretManager;
  private Daemon keyupdaterthread = null; // AccessKeyUpdater thread
  private final static Random rnd = new Random();
  
  // all data node lists
  private Collection<Source> overUtilizedDatanodes
                               = new LinkedList<Source>();
  private Collection<Source> aboveAvgUtilizedDatanodes
                               = new LinkedList<Source>();
  private Collection<BalancerDatanode> belowAvgUtilizedDatanodes
                               = new LinkedList<BalancerDatanode>();
  private Collection<BalancerDatanode> underUtilizedDatanodes
                               = new LinkedList<BalancerDatanode>();
  
  private Collection<Source> sources
                               = new HashSet<Source>();
  private Collection<BalancerDatanode> targets
                               = new HashSet<BalancerDatanode>();
  
  private Map<Block, BalancerBlock> globalBlockList
                 = new HashMap<Block, BalancerBlock>();
  private MovedBlocks movedBlocks = new MovedBlocks();
  private Map<String, BalancerDatanode> datanodes
                 = new HashMap<String, BalancerDatanode>();
  
  private NetworkTopology cluster = new NetworkTopology();
  
  private double avgUtilization = 0.0D;
  
  final static private int MOVER_THREAD_POOL_SIZE = 1000;
  final private ExecutorService moverExecutor = 
    Executors.newFixedThreadPool(MOVER_THREAD_POOL_SIZE);
  final static private int DISPATCHER_THREAD_POOL_SIZE = 200;
  final private ExecutorService dispatcherExecutor =
    Executors.newFixedThreadPool(DISPATCHER_THREAD_POOL_SIZE);
  
  /* This class keeps track of a scheduled block move */
  private class PendingBlockMove {
    private BalancerBlock block;
    private Source source;
    private BalancerDatanode proxySource;
    private BalancerDatanode target;
    
    /** constructor */
    private PendingBlockMove() {
    }
    
    /* choose a block & a proxy source for this pendingMove 
     * whose source & target have already been chosen.
     * 
     * Return true if a block and its proxy are chosen; false otherwise
     */
    private boolean chooseBlockAndProxy() {
      // iterate all source's blocks until find a good one    
      for (Iterator<BalancerBlock> blocks=
        source.getBlockIterator(); blocks.hasNext();) {
        if (markMovedIfGoodBlock(blocks.next())) {
          blocks.remove();
          return true;
        }
      }
      return false;
    }
    
    /* Return true if the given block is good for the tentative move;
     * If it is good, add it to the moved list to marked as "Moved".
     * A block is good if
     * 1. it is a good candidate; see isGoodBlockCandidate
     * 2. can find a proxy source that's not busy for this move
     */
    private boolean markMovedIfGoodBlock(BalancerBlock block) {
      synchronized(block) {
        synchronized(movedBlocks) {
          if (isGoodBlockCandidate(source, target, block)) {
            this.block = block;
            if ( chooseProxySource() ) {
              movedBlocks.add(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move block "+ block.getBlockId()
                    +" with a length of "+StringUtils.byteDesc(block.getNumBytes())
                    + " bytes from " + source.getName() 
                    + " to " + target.getName()
                    + " using proxy source " + proxySource.getName() );
              }
              return true;
            }
          }
        }
      }
      return false;
    }
    
    /* Now we find out source, target, and block, we need to find a proxy
     * 
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      // check if there is replica which is on the same rack with the target
      for (BalancerDatanode loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanode(), target.getDatanode())) {
          if (loc.addPendingBlock(this)) {
            proxySource = loc;
            return true;
          }
        }
      }
      // find out a non-busy replica
      for (BalancerDatanode loc : block.getLocations()) {
        if (loc.addPendingBlock(this)) {
          proxySource = loc;
          return true;
        }
      }
      return false;
    }
    
    /* Dispatch the block move task to the proxy source & wait for the response
     */
    private void dispatch() {
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(NetUtils.createSocketAddr(
            target.datanode.getName()), HdfsConstants.READ_TIMEOUT);
        sock.setKeepAlive(true);
        out = new DataOutputStream( new BufferedOutputStream(
            sock.getOutputStream(), FSConstants.BUFFER_SIZE));
        sendRequest(out);
        in = new DataInputStream( new BufferedInputStream(
            sock.getInputStream(), FSConstants.BUFFER_SIZE));
        receiveResponse(in);
        bytesMoved.inc(block.getNumBytes());
        LOG.info( "Moving block " + block.getBlock().getBlockId() +
              " from "+ source.getName() + " to " +
              target.getName() + " through " +
              proxySource.getName() +
              " is succeeded." );
      } catch (IOException e) {
        LOG.warn("Error moving block "+block.getBlockId()+
            " from " + source.getName() + " to " +
            target.getName() + " through " +
            proxySource.getName() +
            ": "+e.getMessage());
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
        
        proxySource.removePendingBlock(this);
        synchronized(target) {
          target.removePendingBlock(this);
        }

        synchronized (this ) {
          reset();
        }
        synchronized (Balancer.this) {
          Balancer.this.notifyAll();
        }
      }
    }
    
    /* Send a block replace request to the output stream*/
    private void sendRequest(DataOutputStream out) throws IOException {
      out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
      out.writeByte(DataTransferProtocol.OP_REPLACE_BLOCK);
      out.writeLong(block.getBlock().getBlockId());
      out.writeLong(block.getBlock().getGenerationStamp());
      Text.writeString(out, source.getStorageID());
      proxySource.write(out);
      Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
      if (isBlockTokenEnabled) {
        accessToken = blockTokenSecretManager.generateToken(null, block.getBlock(), 
            EnumSet.of(BlockTokenSecretManager.AccessMode.REPLACE,
                BlockTokenSecretManager.AccessMode.COPY));
      }
      accessToken.write(out);
      out.flush();
    }
    
    /* Receive a block copy response from the input stream */ 
    private void receiveResponse(DataInputStream in) throws IOException {
      short status = in.readShort();
      if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
        if (status == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN)
          throw new IOException("block move failed due to access token error");
        throw new IOException("block move is failed");
      }
    }

    /* reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }
    
    /* start a thread to dispatch the block move */
    private void scheduleBlockMove() {
      moverExecutor.execute(new Runnable() {
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting moving "+ block.getBlockId() +
                " from " + proxySource.getName() + " to " + target.getName());
          }
          dispatch();
        }
      });
    }
  }
  
  /* A class for keeping track of blocks in the Balancer */
  static private class BalancerBlock {
    private Block block; // the block
    private List<BalancerDatanode> locations
            = new ArrayList<BalancerDatanode>(3); // its locations
    
    /* Constructor */
    private BalancerBlock(Block block) {
      this.block = block;
    }
    
    /* clean block locations */
    private synchronized void clearLocations() {
      locations.clear();
    }
    
    /* add a location */
    private synchronized void addLocation(BalancerDatanode datanode) {
      if (!locations.contains(datanode)) {
        locations.add(datanode);
      }
    }
    
    /* Return if the block is located on <code>datanode</code> */
    private synchronized boolean isLocatedOnDatanode(
        BalancerDatanode datanode) {
      return locations.contains(datanode);
    }
    
    /* Return its locations */
    private synchronized List<BalancerDatanode> getLocations() {
      return locations;
    }
    
    /* Return the block */
    private Block getBlock() {
      return block;
    }
    
    /* Return the block id */
    private long getBlockId() {
      return block.getBlockId();
    }
    
    /* Return the length of the block */
    private long getNumBytes() {
      return block.getNumBytes();
    }
  }
  
  /* The class represents a desired move of bytes between two nodes 
   * and the target.
   * An object of this class is stored in a source node. 
   */
  static private class NodeTask {
    private BalancerDatanode datanode; //target node
    private long size;  //bytes scheduled to move
    
    /* constructor */
    private NodeTask(BalancerDatanode datanode, long size) {
      this.datanode = datanode;
      this.size = size;
    }
    
    /* Get the node */
    private BalancerDatanode getDatanode() {
      return datanode;
    }
    
    /* Get the number of bytes that need to be moved */
    private long getSize() {
      return size;
    }
  }
  
  /* Return the utilization of a datanode */
  static private double getUtilization(DatanodeInfo datanode) {
    return ((double)datanode.getDfsUsed())/datanode.getCapacity()*100;
  }
  
  /* A class that keeps track of a datanode in Balancer */
  private static class BalancerDatanode implements Writable {
    final private static long MAX_SIZE_TO_MOVE = 10*1024*1024*1024L; //10GB
    protected DatanodeInfo datanode;
    private double utilization;
    protected long maxSizeToMove;
    protected long scheduledSize = 0L;
    //  blocks being moved but not confirmed yet
    private List<PendingBlockMove> pendingBlocks = 
      new ArrayList<PendingBlockMove>(MAX_NUM_CONCURRENT_MOVES); 
    
    /* Constructor 
     * Depending on avgutil & threshold, calculate maximum bytes to move 
     */
    private BalancerDatanode(
        DatanodeInfo node, double avgUtil, double threshold) {
      datanode = node;
      utilization = Balancer.getUtilization(node);
        
      if (utilization >= avgUtil+threshold
          || utilization <= avgUtil-threshold) { 
        maxSizeToMove = (long)(threshold*datanode.getCapacity()/100);
      } else {
        maxSizeToMove = 
          (long)(Math.abs(avgUtil-utilization)*datanode.getCapacity()/100);
      }
      if (utilization < avgUtil ) {
        maxSizeToMove = Math.min(datanode.getRemaining(), maxSizeToMove);
      }
      maxSizeToMove = Math.min(MAX_SIZE_TO_MOVE, maxSizeToMove);
    }
    
    /** Get the datanode */
    protected DatanodeInfo getDatanode() {
      return datanode;
    }
    
    /** Get the name of the datanode */
    protected String getName() {
      return datanode.getName();
    }
    
    /* Get the storage id of the datanode */
    protected String getStorageID() {
      return datanode.getStorageID();
    }
    
    /** Decide if still need to move more bytes */
    protected boolean isMoveQuotaFull() {
      return scheduledSize<maxSizeToMove;
    }

    /** Return the total number of bytes that need to be moved */
    protected long availableSizeToMove() {
      return maxSizeToMove-scheduledSize;
    }
    
    /* increment scheduled size */
    protected void incScheduledSize(long size) {
      scheduledSize += size;
    }
    
    /* Check if the node can schedule more blocks to move */
    synchronized private boolean isPendingQNotFull() {
      if ( pendingBlocks.size() < MAX_NUM_CONCURRENT_MOVES ) {
        return true;
      }
      return false;
    }
    
    /* Check if all the dispatched moves are done */
    synchronized private boolean isPendingQEmpty() {
      return pendingBlocks.isEmpty();
    }
    
    /* Add a scheduled block move to the node */
    private synchronized boolean addPendingBlock(
        PendingBlockMove pendingBlock) {
      if (isPendingQNotFull()) {
        return pendingBlocks.add(pendingBlock);
      }
      return false;
    }
    
    /* Remove a scheduled block move from the node */
    private synchronized boolean  removePendingBlock(
        PendingBlockMove pendingBlock) {
      return pendingBlocks.remove(pendingBlock);
    }

    /** The following two methods support the Writable interface */
    /** Deserialize */
    public void readFields(DataInput in) throws IOException {
      datanode.readFields(in);
    }

    /** Serialize */
    public void write(DataOutput out) throws IOException {
      datanode.write(out);
    }
  }
  
  /** A node that can be the sources of a block move */
  private class Source extends BalancerDatanode {
    
    /* A thread that initiates a block move 
     * and waits for block move to complete */
    private class BlockMoveDispatcher implements Runnable {
      public void run() {
        dispatchBlocks();
      }
    }
    
    private ArrayList<NodeTask> nodeTasks = new ArrayList<NodeTask>(2);
    private long blocksToReceive = 0L;
    /* source blocks point to balancerBlocks in the global list because
     * we want to keep one copy of a block in balancer and be aware that
     * the locations are changing over time.
     */
    private List<BalancerBlock> srcBlockList
            = new ArrayList<BalancerBlock>();
    
    /* constructor */
    private Source(DatanodeInfo node, double avgUtil, double threshold) {
      super(node, avgUtil, threshold);
    }
    
    /** Add a node task */
    private void addNodeTask(NodeTask task) {
      assert (task.datanode != this) :
        "Source and target are the same " + datanode.getName();
      incScheduledSize(task.getSize());
      nodeTasks.add(task);
    }
    
    /* Return an iterator to this source's blocks */
    private Iterator<BalancerBlock> getBlockIterator() {
      return srcBlockList.iterator();
    }
    
    /* fetch new blocks of this source from namenode and
     * update this source's block list & the global block list
     * Return the total size of the received blocks in the number of bytes.
     */
    private long getBlockList() throws IOException {
      BlockWithLocations[] newBlocks = namenode.getBlocks(datanode, 
        (long)Math.min(MAX_BLOCKS_SIZE_TO_FETCH, blocksToReceive)).getBlocks();
      long bytesReceived = 0;
      for (BlockWithLocations blk : newBlocks) {
        bytesReceived += blk.getBlock().getNumBytes();
        BalancerBlock block;
        synchronized(globalBlockList) {
          block = globalBlockList.get(blk.getBlock());
          if (block==null) {
            block = new BalancerBlock(blk.getBlock());
            globalBlockList.put(blk.getBlock(), block);
          } else {
            block.clearLocations();
          }
        
          synchronized (block) {
            // update locations
            for ( String location : blk.getDatanodes() ) {
              BalancerDatanode datanode = datanodes.get(location);
              if (datanode != null) { // not an unknown datanode
                block.addLocation(datanode);
              }
            }
          }
          if (!srcBlockList.contains(block) && isGoodBlockCandidate(block)) {
            // filter bad candidates
            srcBlockList.add(block);
          }
        }
      }
      return bytesReceived;
    }

    /* Decide if the given block is a good candidate to move or not */
    private boolean isGoodBlockCandidate(BalancerBlock block) {
      for (NodeTask nodeTask : nodeTasks) {
        if (Balancer.this.isGoodBlockCandidate(this, nodeTask.datanode, block)) {
          return true;
        }
      }
      return false;
    }

    /* Return a block that's good for the source thread to dispatch immediately
     * The block's source, target, and proxy source are determined too.
     * When choosing proxy and target, source & target throttling
     * has been considered. They are chosen only when they have the capacity
     * to support this block move.
     * The block should be dispatched immediately after this method is returned.
     */
    private PendingBlockMove chooseNextBlockToMove() {
      for ( Iterator<NodeTask> tasks=nodeTasks.iterator(); tasks.hasNext(); ) {
        NodeTask task = tasks.next();
        BalancerDatanode target = task.getDatanode();
        PendingBlockMove pendingBlock = new PendingBlockMove();
        if ( target.addPendingBlock(pendingBlock) ) { 
          // target is not busy, so do a tentative block allocation
          pendingBlock.source = this;
          pendingBlock.target = target;
          if ( pendingBlock.chooseBlockAndProxy() ) {
            long blockSize = pendingBlock.block.getNumBytes(); 
            scheduledSize -= blockSize;
            task.size -= blockSize;
            if (task.size == 0) {
              tasks.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }

    /* iterate all source's blocks to remove moved ones */    
    private void filterMovedBlocks() {
      for (Iterator<BalancerBlock> blocks=getBlockIterator();
            blocks.hasNext();) {
        if (movedBlocks.contains(blocks.next())) {
          blocks.remove();
        }
      }
    }
    
    private static final int SOURCE_BLOCK_LIST_MIN_SIZE=5;
    /* Return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return srcBlockList.size()<SOURCE_BLOCK_LIST_MIN_SIZE &&
                 blocksToReceive>0;
    }
    
    /* This method iteratively does the following:
     * it first selects a block to move,
     * then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks
     * the namenode for more blocks.
     * It terminates when it has dispatch enough block move tasks or
     * it has received enough blocks from the namenode, or 
     * the elapsed time of the iteration has exceeded the max time limit.
     */ 
    private static final long MAX_ITERATION_TIME = 20*60*1000L; //20 mins
    private void dispatchBlocks() {
      long startTime = Util.now();
      this.blocksToReceive = 2*scheduledSize;
      boolean isTimeUp = false;
      while(!isTimeUp && scheduledSize>0 &&
          (!srcBlockList.isEmpty() || blocksToReceive>0)) {
        PendingBlockMove pendingBlock = chooseNextBlockToMove();
        if (pendingBlock != null) {
          // move the block
          pendingBlock.scheduleBlockMove();
          continue;
        }
        
        /* Since we can not schedule any block to move,
         * filter any moved blocks from the source block list and
         * check if we should fetch more blocks from the namenode
         */
        filterMovedBlocks(); // filter already moved blocks
        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            blocksToReceive -= getBlockList();
            continue;
          } catch (IOException e) {
            LOG.warn(StringUtils.stringifyException(e));
            return;
          }
        } 
        
        // check if time is up or not
        if (Util.now()-startTime > MAX_ITERATION_TIME) {
          isTimeUp = true;
          continue;
        }
        
        /* Now we can not schedule any block to move and there are
         * no new blocks added to the source block list, so we wait. 
         */
        try {
          synchronized(Balancer.this) {
            Balancer.this.wait(1000);  // wait for targets/sources to be idle
          }
        } catch (InterruptedException ignored) {
        }
      }
    }
  }
  
  /** Default constructor */
  Balancer() {
  }
  
  /** Construct a balancer from the given configuration */
  Balancer(Configuration conf) {
    setConf(conf);
  } 

  /** Construct a balancer from the given configuration and threshold */
  Balancer(Configuration conf, double threshold) {
    setConf(conf);
    this.threshold = threshold;
  }

  /**
   * Run a balancer
   * @param args
   */
  public static void main(String[] args) {
    try {
      System.exit( ToolRunner.run(null, new Balancer(), args) );
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }

  }

  private static void printUsage() {
    System.out.println("Usage: java Balancer");
    System.out.println("          [-threshold <threshold>]\t" 
        +"percentage of disk capacity");
  }

  /* parse argument to get the threshold */
  private double parseArgs(String[] args) {
    double threshold=0;
    int argsLen = (args == null) ? 0 : args.length;
    if (argsLen==0) {
      threshold = 10;
    } else {
      if (argsLen != 2 || !"-threshold".equalsIgnoreCase(args[0])) {
        printUsage();
        throw new IllegalArgumentException(Arrays.toString(args));
      } else {
        try {
          threshold = Double.parseDouble(args[1]);
          if (threshold < 0 || threshold >100) {
            throw new NumberFormatException();
          }
          LOG.info( "Using a threshold of " + threshold );
        } catch(NumberFormatException e) {
          System.err.println(
              "Expect a double parameter in the range of [0, 100]: "+ args[1]);
          printUsage();
          throw e;
        }
      }
    }
    return threshold;
  }
  
  /* Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  private void init(double threshold) throws IOException {
    this.threshold = threshold;
    this.namenode = createNamenode(conf);
    this.client = DFSClient.createNamenode(conf);
    this.fs = FileSystem.get(conf);
    ExportedBlockKeys keys = namenode.getBlockKeys();
    this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
    if (isBlockTokenEnabled) {
      long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
      long blockTokenLifetime = keys.getTokenLifetime();
      LOG.info("Block token params received from NN: keyUpdateInterval="
          + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
          + blockTokenLifetime / (60 * 1000) + " min(s)");
      this.blockTokenSecretManager = new BlockTokenSecretManager(false,
          blockKeyUpdateInterval, blockTokenLifetime);
      this.blockTokenSecretManager.setKeys(keys);
      /*
       * Balancer should sync its block keys with NN more frequently than NN
       * updates its block keys
       */
      this.keyUpdaterInterval = blockKeyUpdateInterval / 4;
      LOG.info("Balancer will update its block keys every "
          + keyUpdaterInterval / (60 * 1000) + " minute(s)");
      this.keyupdaterthread = new Daemon(new BlockKeyUpdater());
      this.shouldRun = true;
      this.keyupdaterthread.start();
    }
  }
  
  /**
   * Periodically updates access keys.
   */
  class BlockKeyUpdater implements Runnable {

    public void run() {
      while (shouldRun) {
        try {
          blockTokenSecretManager.setKeys(namenode.getBlockKeys());
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(keyUpdaterInterval);
        } catch (InterruptedException ie) {
        }
      }
    }
  }
  
  /* Build a NamenodeProtocol connection to the namenode and
   * set up the retry policy */ 
  private static NamenodeProtocol createNamenode(Configuration conf)
    throws IOException {
    InetSocketAddress nameNodeAddr = NameNode.getServiceAddress(conf, true);
    RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(
        5, 200, TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        timeoutPolicy, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap =
        new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("getBlocks", methodPolicy);
    methodNameToPolicyMap.put("getAccessKeys", methodPolicy);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    return (NamenodeProtocol) RetryProxy.create(
        NamenodeProtocol.class,
        RPC.getProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID,
            nameNodeAddr,
            ugi,
            conf,
            NetUtils.getDefaultSocketFactory(conf)),
        methodNameToPolicyMap);
  }
  
  /* Shuffle datanode array */
  static private void shuffleArray(DatanodeInfo[] datanodes) {
    for (int i=datanodes.length; i>1; i--) {
      int randomIndex = rnd.nextInt(i);
      DatanodeInfo tmp = datanodes[randomIndex];
      datanodes[randomIndex] = datanodes[i-1];
      datanodes[i-1] = tmp;
    }
  }
  
  /* get all live datanodes of a cluster and their disk usage
   * decide the number of bytes need to be moved
   */
  private long initNodes() throws IOException {
    return initNodes(client.getDatanodeReport(DatanodeReportType.LIVE));
  }
  
  /* Given a data node set, build a network topology and decide
   * over-utilized datanodes, above average utilized datanodes, 
   * below average utilized datanodes, and underutilized datanodes. 
   * The input data node set is shuffled before the datanodes 
   * are put into the over-utilized datanodes, above average utilized
   * datanodes, below average utilized datanodes, and
   * underutilized datanodes lists. This will add some randomness
   * to the node matching later on.
   * 
   * @return the total number of bytes that are 
   *                needed to move to make the cluster balanced.
   * @param datanodes a set of datanodes
   */
  private long initNodes(DatanodeInfo[] datanodes) {
    // compute average utilization
    long totalCapacity=0L, totalUsedSpace=0L;
    for (DatanodeInfo datanode : datanodes) {
      if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
        continue; // ignore decommissioning or decommissioned nodes
      }
      totalCapacity += datanode.getCapacity();
      totalUsedSpace += datanode.getDfsUsed();
    }
    this.avgUtilization = ((double)totalUsedSpace)/totalCapacity*100;

    /*create network topology and all data node lists: 
     * overloaded, above-average, below-average, and underloaded
     * we alternates the accessing of the given datanodes array either by
     * an increasing order or a decreasing order.
     */  
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    shuffleArray(datanodes);
    for (DatanodeInfo datanode : datanodes) {
      if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
        continue; // ignore decommissioning or decommissioned nodes
      }
      cluster.add(datanode);
      BalancerDatanode datanodeS;
      if (getUtilization(datanode) > avgUtilization) {
        datanodeS = new Source(datanode, avgUtilization, threshold);
        if (isAboveAvgUtilized(datanodeS)) {
          this.aboveAvgUtilizedDatanodes.add((Source)datanodeS);
        } else {
          assert(isOverUtilized(datanodeS)) :
            datanodeS.getName()+ "is not an overUtilized node";
          this.overUtilizedDatanodes.add((Source)datanodeS);
          overLoadedBytes += (long)((datanodeS.utilization-avgUtilization
              -threshold)*datanodeS.datanode.getCapacity()/100.0);
        }
      } else {
        datanodeS = new BalancerDatanode(datanode, avgUtilization, threshold);
        if ( isBelowAvgUtilized(datanodeS)) {
          this.belowAvgUtilizedDatanodes.add(datanodeS);
        } else {
// Assert fails TestBalancer.testBalancer0(); disabled until fixed upstream.
//          assert (isUnderUtilized(datanodeS)) :
//            datanodeS.getName()+ "is not an underUtilized node"; 
          this.underUtilizedDatanodes.add(datanodeS);
          underLoadedBytes += (long)((avgUtilization-threshold-
              datanodeS.utilization)*datanodeS.datanode.getCapacity()/100.0);
        }
      }
      this.datanodes.put(datanode.getStorageID(), datanodeS);
    }

    //logging
    logImbalancedNodes();
    
    assert (this.datanodes.size() == 
      overUtilizedDatanodes.size()+underUtilizedDatanodes.size()+
      aboveAvgUtilizedDatanodes.size()+belowAvgUtilizedDatanodes.size())
      : "Mismatched number of datanodes";
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  /* log the over utilized & under utilized nodes */
  private void logImbalancedNodes() {
    StringBuilder msg = new StringBuilder();
    msg.append(overUtilizedDatanodes.size());
    msg.append(" over utilized nodes:");
    for (Source node : overUtilizedDatanodes) {
      msg.append( " " );
      msg.append( node.getName() );
    }
    LOG.info(msg);
    msg = new StringBuilder();
    msg.append(underUtilizedDatanodes.size());
    msg.append(" under utilized nodes: ");
    for (BalancerDatanode node : underUtilizedDatanodes) {
      msg.append( " " );
      msg.append( node.getName() );
    }
    LOG.info(msg);
  }
  
  /* Decide all <source, target> pairs and
   * the number of bytes to move from a source to a target
   * Maximum bytes to be moved per node is
   * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
   * Return total number of bytes to move in this iteration
   */
  private long chooseNodes() {
    // Match nodes on the same rack first
    chooseNodes(true);
    // Then match nodes on different racks
    chooseNodes(false);
    
//    Assert fails in CDH build TestBalancer.testBalancer0()
//    disabled until root cause fixed upstream.
//    assert (datanodes.size() == 
//      overUtilizedDatanodes.size()+underUtilizedDatanodes.size()+
//      aboveAvgUtilizedDatanodes.size()+belowAvgUtilizedDatanodes.size()+
//      sources.size()+targets.size())
//      : "Mismatched number of datanodes";

    long bytesToMove = 0L;
    for (Source src : sources) {
      bytesToMove += src.scheduledSize;
    }
    return bytesToMove;
  }

  /* if onRack is true, decide all <source, target> pairs
   * where source and target are on the same rack; Otherwise
   * decide all <source, target> pairs where source and target are
   * on different racks
   */
  private void chooseNodes(boolean onRack) {
    /* first step: match each overUtilized datanode (source) to
     * one or more underUtilized datanodes (targets).
     */
    chooseTargets(underUtilizedDatanodes.iterator(), onRack);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    chooseTargets(belowAvgUtilizedDatanodes.iterator(), onRack);

    /* match each remaining underutilized datanode to 
     * above average utilized datanodes.
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    chooseSources(aboveAvgUtilizedDatanodes.iterator(), onRack);
  }
   
  /* choose targets from the target candidate list for each over utilized
   * source datanode. OnRackTarget determines if the chosen target 
   * should be on the same rack as the source
   */
  private void chooseTargets(  
      Iterator<BalancerDatanode> targetCandidates, boolean onRackTarget ) {
    for (Iterator<Source> srcIterator = overUtilizedDatanodes.iterator();
        srcIterator.hasNext();) {
      Source source = srcIterator.next();
      while (chooseTarget(source, targetCandidates, onRackTarget)) {
      }
      if (!source.isMoveQuotaFull()) {
        srcIterator.remove();
      }
    }
    return;
  }
  
  /* choose sources from the source candidate list for each under utilized
   * target datanode. onRackSource determines if the chosen source 
   * should be on the same rack as the target
   */
  private void chooseSources(
      Iterator<Source> sourceCandidates, boolean onRackSource) {
    for (Iterator<BalancerDatanode> targetIterator = 
      underUtilizedDatanodes.iterator(); targetIterator.hasNext();) {
      BalancerDatanode target = targetIterator.next();
      while (chooseSource(target, sourceCandidates, onRackSource)) {
      }
      if (!target.isMoveQuotaFull()) {
        targetIterator.remove();
      }
    }
    return;
  }

  /* For the given source, choose targets from the target candidate list.
   * OnRackTarget determines if the chosen target 
   * should be on the same rack as the source
   */
  private boolean chooseTarget(Source source,
      Iterator<BalancerDatanode> targetCandidates, boolean onRackTarget) {
    if (!source.isMoveQuotaFull()) {
      return false;
    }
    boolean foundTarget = false;
    BalancerDatanode target = null;
    while (!foundTarget && targetCandidates.hasNext()) {
      target = targetCandidates.next();
      if (!target.isMoveQuotaFull()) {
        targetCandidates.remove();
        continue;
      }
      if (onRackTarget) {
        // choose from on-rack nodes
        if (cluster.isOnSameRack(source.datanode, target.datanode)) {
          foundTarget = true;
        }
      } else {
        // choose from off-rack nodes
        if (!cluster.isOnSameRack(source.datanode, target.datanode)) {
          foundTarget = true;
        }
      }
    }
    if (foundTarget) {
      assert(target != null):"Choose a null target";
      long size = Math.min(source.availableSizeToMove(),
          target.availableSizeToMove());
      NodeTask nodeTask = new NodeTask(target, size);
      source.addNodeTask(nodeTask);
      target.incScheduledSize(nodeTask.getSize());
      sources.add(source);
      targets.add(target);
      if (!target.isMoveQuotaFull()) {
        targetCandidates.remove();
      }
      LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
          +source.datanode.getName() + " to " + target.datanode.getName());
      return true;
    }
    return false;
  }
  
  /* For the given target, choose sources from the source candidate list.
   * OnRackSource determines if the chosen source 
   * should be on the same rack as the target
   */
  private boolean chooseSource(BalancerDatanode target,
      Iterator<Source> sourceCandidates, boolean onRackSource) {
    if (!target.isMoveQuotaFull()) {
      return false;
    }
    boolean foundSource = false;
    Source source = null;
    while (!foundSource && sourceCandidates.hasNext()) {
      source = sourceCandidates.next();
      if (!source.isMoveQuotaFull()) {
        sourceCandidates.remove();
        continue;
      }
      if (onRackSource) {
        // choose from on-rack nodes
        if ( cluster.isOnSameRack(source.getDatanode(), target.getDatanode())) {
          foundSource = true;
        }
      } else {
        // choose from off-rack nodes
        if (!cluster.isOnSameRack(source.datanode, target.datanode)) {
          foundSource = true;
        }
      }
    }
    if (foundSource) {
      assert(source != null):"Choose a null source";
      long size = Math.min(source.availableSizeToMove(),
          target.availableSizeToMove());
      NodeTask nodeTask = new NodeTask(target, size);
      source.addNodeTask(nodeTask);
      target.incScheduledSize(nodeTask.getSize());
      sources.add(source);
      targets.add(target);
      if ( !source.isMoveQuotaFull()) {
        sourceCandidates.remove();
      }
      LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
          +source.datanode.getName() + " to " + target.datanode.getName());
      return true;
    }
    return false;
  }

  private static class BytesMoved {
    private long bytesMoved = 0L;;
    private synchronized void inc( long bytes ) {
      bytesMoved += bytes;
    }

    private long get() {
      return bytesMoved;
    }
  };
  private BytesMoved bytesMoved = new BytesMoved();
  private int notChangedIterations = 0;
  
  /* Start a thread to dispatch block moves for each source. 
   * The thread selects blocks to move & sends request to proxy source to
   * initiate block move. The process is flow controlled. Block selection is
   * blocked if there are too many un-confirmed block moves.
   * Return the total number of bytes successfully moved in this iteration.
   */
  private long dispatchBlockMoves() throws InterruptedException {
    long bytesLastMoved = bytesMoved.get();
    Future<?>[] futures = new Future<?>[sources.size()];
    int i=0;
    for (Source source : sources) {
      futures[i++] = dispatcherExecutor.submit(source.new BlockMoveDispatcher());
    }
    
    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }
    
    // wait for all block moving to be done
    waitForMoveCompletion();
    
    return bytesMoved.get()-bytesLastMoved;
  }
  
  // The sleeping period before checking if block move is completed again
  static private long blockMoveWaitTime = 30000L;
  
  /** set the sleeping period for block move completion check */
  static void setBlockMoveWaitTime(long time) {
    blockMoveWaitTime = time;
  }
  
  /* wait for all block move confirmations 
   * by checking each target's pendingMove queue 
   */
  private void waitForMoveCompletion() {
    boolean shouldWait;
    do {
      shouldWait = false;
      for (BalancerDatanode target : targets) {
        if (!target.isPendingQEmpty()) {
          shouldWait = true;
        }
      }
      if (shouldWait) {
        try {
          Thread.sleep(blockMoveWaitTime);
        } catch (InterruptedException ignored) {
        }
      }
    } while (shouldWait);
  }

  /** This window makes sure to keep blocks that have been moved within 1.5 hour.
   * Old window has blocks that are older;
   * Current window has blocks that are more recent;
   * Cleanup method triggers the check if blocks in the old window are
   * more than 1.5 hour old. If yes, purge the old window and then
   * move blocks in current window to old window.
   */ 
  private static class MovedBlocks {
    private long lastCleanupTime = System.currentTimeMillis();
    private static long winWidth = 5400*1000L; // 1.5 hour
    final private static int CUR_WIN = 0;
    final private static int OLD_WIN = 1;
    final private static int NUM_WINS = 2;
    final private List<HashMap<Block, BalancerBlock>> movedBlocks = 
      new ArrayList<HashMap<Block, BalancerBlock>>(NUM_WINS);
    
    /* initialize the moved blocks collection */
    private MovedBlocks() {
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
    }

    /* set the win width */
    private void setWinWidth(Configuration conf) {
      winWidth = conf.getLong(
          "dfs.balancer.movedWinWidth", 5400*1000L);
    }
    
    /* add a block thus marking a block to be moved */
    synchronized private void add(BalancerBlock block) {
      movedBlocks.get(CUR_WIN).put(block.getBlock(), block);
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(BalancerBlock block) {
      return contains(block.getBlock());
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(Block block) {
      return movedBlocks.get(CUR_WIN).containsKey(block) ||
        movedBlocks.get(OLD_WIN).containsKey(block);
    }

    /* remove old blocks */
    synchronized private void cleanup() {
      long curTime = System.currentTimeMillis();
      // check if old win is older than winWidth
      if (lastCleanupTime + winWidth <= curTime) {
        // purge the old window
        movedBlocks.set(OLD_WIN, movedBlocks.get(CUR_WIN));
        movedBlocks.set(CUR_WIN, new HashMap<Block, BalancerBlock>());
        lastCleanupTime = curTime;
      }
    }
  }

  /* Decide if it is OK to move the given block from source to target
   * A block is a good candidate if
   * 1. the block is not in the process of being moved/has not been moved;
   * 2. the block does not have a replica on the target;
   * 3. doing the move does not reduce the number of racks that the block has
   */
  private boolean isGoodBlockCandidate(Source source, 
      BalancerDatanode target, BalancerBlock block) {
    // check if the block is moved or not
    if (movedBlocks.contains(block)) {
        return false;
    }
    if (block.isLocatedOnDatanode(target)) {
      return false;
    }

    boolean goodBlock = false;
    if (cluster.isOnSameRack(source.getDatanode(), target.getDatanode())) {
      // good if source and target are on the same rack
      goodBlock = true;
    } else {
      boolean notOnSameRack = true;
      synchronized (block) {
        for (BalancerDatanode loc : block.locations) {
          if (cluster.isOnSameRack(loc.datanode, target.datanode)) {
            notOnSameRack = false;
            break;
          }
        }
      }
      if (notOnSameRack) {
        // good if target is target is not on the same rack as any replica
        goodBlock = true;
      } else {
        // good if source is on the same rack as on of the replicas
        for (BalancerDatanode loc : block.locations) {
          if (loc != source && 
              cluster.isOnSameRack(loc.datanode, source.datanode)) {
            goodBlock = true;
            break;
          }
        }
      }
    }
    return goodBlock;
  }
  
  /* reset all fields in a balancer preparing for the next iteration */
  private void resetData() {
    this.cluster = new NetworkTopology();
    this.overUtilizedDatanodes.clear();
    this.aboveAvgUtilizedDatanodes.clear();
    this.belowAvgUtilizedDatanodes.clear();
    this.underUtilizedDatanodes.clear();
    this.datanodes.clear();
    this.sources.clear();
    this.targets.clear();  
    this.avgUtilization = 0.0D;
    cleanGlobalBlockList();
    this.movedBlocks.cleanup();
  }
  
  /* Remove all blocks from the global block list except for the ones in the
   * moved list.
   */
  private void cleanGlobalBlockList() {
    for (Iterator<Block> globalBlockListIterator=globalBlockList.keySet().iterator();
    globalBlockListIterator.hasNext();) {
      Block block = globalBlockListIterator.next();
      if(!movedBlocks.contains(block)) {
        globalBlockListIterator.remove();
      }
    }
  }
  
  /* Return true if the given datanode is overUtilized */
  private boolean isOverUtilized(BalancerDatanode datanode) {
    return datanode.utilization > (avgUtilization+threshold);
  }
  
  /* Return true if the given datanode is above average utilized
   * but not overUtilized */
  private boolean isAboveAvgUtilized(BalancerDatanode datanode) {
    return (datanode.utilization <= (avgUtilization+threshold))
        && (datanode.utilization > avgUtilization);
  }
  
  /* Return true if the given datanode is underUtilized */
  private boolean isUnderUtilized(BalancerDatanode datanode) {
    return datanode.utilization < (avgUtilization-threshold);
  }

  /* Return true if the given datanode is below average utilized 
   * but not underUtilized */
  private boolean isBelowAvgUtilized(BalancerDatanode datanode) {
        return (datanode.utilization >= (avgUtilization-threshold))
                 && (datanode.utilization < avgUtilization);
  }

  // Exit status
  final public static int SUCCESS = 1;
  final public static int ALREADY_RUNNING = -1;
  final public static int NO_MOVE_BLOCK = -2;
  final public static int NO_MOVE_PROGRESS = -3;
  final public static int IO_EXCEPTION = -4;
  final public static int ILLEGAL_ARGS = -5;
  /** main method of Balancer
   * @param args arguments to a Balancer
   * @exception any exception occurs during datanode balancing
   */
  public int run(String[] args) throws Exception {
    long startTime = Util.now();
    OutputStream out = null;
    try {
      // initialize a balancer
      init(parseArgs(args));
      
      /* Check if there is another balancer running.
       * Exit if there is another one running.
       */
      out = checkAndMarkRunningBalancer(); 
      if (out == null) {
        System.out.println("Another balancer is running. Exiting...");
        return ALREADY_RUNNING;
      }

      Formatter formatter = new Formatter(System.out);
      System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
      int iterations = 0;
      while (true ) {
        /* get all live datanodes of a cluster and their disk usage
         * decide the number of bytes need to be moved
         */
        long bytesLeftToMove = initNodes();
        if (bytesLeftToMove == 0) {
          System.out.println("The cluster is balanced. Exiting...");
          return SUCCESS;
        } else {
          LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
              +" bytes to make the cluster balanced." );
        }
        
        /* Decide all the nodes that will participate in the block move and
         * the number of bytes that need to be moved from one node to another
         * in this iteration. Maximum bytes to be moved per node is
         * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
         */
        long bytesToMove = chooseNodes();
        if (bytesToMove == 0) {
          System.out.println("No block can be moved. Exiting...");
          return NO_MOVE_BLOCK;
        } else {
          LOG.info( "Will move " + StringUtils.byteDesc(bytesToMove) +
              "bytes in this iteration");
        }
   
        formatter.format("%-24s %10d  %19s  %18s  %17s\n", 
            DateFormat.getDateTimeInstance().format(new Date()),
            iterations,
            StringUtils.byteDesc(bytesMoved.get()),
            StringUtils.byteDesc(bytesLeftToMove),
            StringUtils.byteDesc(bytesToMove)
            );
        
        /* For each pair of <source, target>, start a thread that repeatedly 
         * decide a block to be moved and its proxy source, 
         * then initiates the move until all bytes are moved or no more block
         * available to move.
         * Exit no byte has been moved for 5 consecutive iterations.
         */
        if (dispatchBlockMoves() > 0) {
          notChangedIterations = 0;
        } else {
          notChangedIterations++;
          if (notChangedIterations >= 5) {
            System.out.println(
                "No block has been moved for 5 iterations. Exiting...");
            return NO_MOVE_PROGRESS;
          }
        }

        // clean all lists
        resetData();
        
        try {
          Thread.sleep(2*conf.getLong("dfs.heartbeat.interval", 3));
        } catch (InterruptedException ignored) {
        }
        
        iterations++;
      }
    } catch (IllegalArgumentException ae) {
      return ILLEGAL_ARGS;
    } catch (IOException e) {
      System.out.println("Received an IO exception: " + e.getMessage() +
          " . Exiting...");
      return IO_EXCEPTION;
    } finally {
      // shutdown thread pools
      dispatcherExecutor.shutdownNow();
      moverExecutor.shutdownNow();

      shouldRun = false;
      try {
        if (keyupdaterthread != null) keyupdaterthread.interrupt();
      } catch (Exception e) {
        LOG.warn("Exception shutting down access key updater thread", e);
      }
      // close the output file
      IOUtils.closeStream(out); 
      if (fs != null) {
        try {
          fs.delete(BALANCER_ID_PATH, true);
        } catch(IOException ignored) {
        }
      }
      System.out.println("Balancing took " + 
          time2Str(Util.now()-startTime));
    }
  }

  private Path BALANCER_ID_PATH = new Path("/system/balancer.id");
  /* The idea for making sure that there is no more than one balancer
   * running in an HDFS is to create a file in the HDFS, writes the IP address
   * of the machine on which the balancer is running to the file, but did not
   * close the file until the balancer exits. 
   * This prevents the second balancer from running because it can not
   * creates the file while the first one is running.
   * 
   * This method checks if there is any running balancer and 
   * if no, mark yes if no.
   * Note that this is an atomic operation.
   * 
   * Return null if there is a running balancer; otherwise the output stream
   * to the newly created file.
   */
  private OutputStream checkAndMarkRunningBalancer() throws IOException {
    try {
      DataOutputStream out = fs.create(BALANCER_ID_PATH);
      out. writeBytes(InetAddress.getLocalHost().getHostName());
      out.flush();
      return out;
    } catch(RemoteException e) {
      if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
        return null;
      } else {
        throw e;
      }
    }
  }
  
  /* Given elaspedTime in ms, return a printable string */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  /** return this balancer's configuration */
  public Configuration getConf() {
    return conf;
  }

  /** set this balancer's configuration */
  public void setConf(Configuration conf) {
    this.conf = conf;
    movedBlocks.setWinWidth(conf);
  }

}
