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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * Main class for a series of name-node benchmarks.
 * 
 * Each benchmark measures throughput and average execution time 
 * of a specific name-node operation, e.g. file creation or block reports.
 * 
 * The benchmark does not involve any other hadoop components
 * except for the name-node. Each operation is executed
 * by calling directly the respective name-node method.
 * The name-node here is real all other components are simulated.
 * 
 * Command line arguments for the benchmark include:
 * <ol>
 * <li>total number of operations to be performed,</li>
 * <li>number of threads to run these operations,</li>
 * <li>followed by operation specific input parameters.</li>
 * <li>-logLevel L specifies the logging level when the benchmark runs.
 * The default logging level is {@link Level#ERROR}.</li>
 * <li>-UGCacheRefreshCount G will cause the benchmark to call
 * {@link NameNode#refreshUserToGroupsMappings(Configuration)} after
 * every G operations, which purges the name-node's user group cache.
 * By default the refresh is never called.</li>
 * </ol>
 * 
 * The benchmark first generates inputs for each thread so that the
 * input generation overhead does not effect the resulting statistics.
 * The number of operations performed by threads is practically the same. 
 * Precisely, the difference between the number of operations 
 * performed by any two threads does not exceed 1.
 * 
 * Then the benchmark executes the specified number of operations using 
 * the specified number of threads and outputs the resulting stats.
 */
public class NNThroughputBenchmark {
  private static final Log LOG = LogFactory.getLog(NNThroughputBenchmark.class);
  private static final int BLOCK_SIZE = 16;
  private static final String GENERAL_OPTIONS_USAGE = 
    "    [-logLevel L] [-UGCacheRefreshCount G]";

  static Configuration config;
  static NameNode nameNode;

  NNThroughputBenchmark(Configuration conf) throws IOException, LoginException {
    config = conf;

    // We do not need many handlers, since each thread simulates a handler
    // by calling name-node methods directly
    config.setInt("dfs.namenode.handler.count", 1);
    // set exclude file
    config.set("dfs.hosts.exclude", "${hadoop.tmp.dir}/dfs/hosts/exclude");
    File excludeFile = new File(config.get("dfs.hosts.exclude", "exclude"));
    if(! excludeFile.exists()) {
      if(!excludeFile.getParentFile().mkdirs())
        throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
    }
    new FileOutputStream(excludeFile).close();
    // Start the NameNode
    String[] argv = new String[] {};
    nameNode = NameNode.createNameNode(argv, config);
  }

  void close() throws IOException {
    nameNode.stop();
  }

  static void setNameNodeLoggingLevel(Level logLevel) {
    LOG.fatal("Log level = " + logLevel.toString());
    // change log level to NameNode logs
    LogManager.getLogger(NameNode.class.getName()).setLevel(logLevel);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(logLevel);
    LogManager.getLogger(NetworkTopology.class.getName()).setLevel(logLevel);
    LogManager.getLogger(FSNamesystem.class.getName()).setLevel(logLevel);
    LogManager.getLogger(LeaseManager.class.getName()).setLevel(logLevel);
    LogManager.getLogger(Groups.class.getName()).setLevel(logLevel);
  }

  /**
   * Base class for collecting operation statistics.
   * 
   * Overload this class in order to run statistics for a 
   * specific name-node operation.
   */
  abstract class OperationStatsBase {
    protected static final String BASE_DIR_NAME = "/nnThroughputBenchmark";
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE = "-op all " +
                                  "<other ops options> [-keepResults]";

    protected String baseDir;
    protected short replication;
    protected int  numThreads = 0;        // number of threads
    protected int  numOpsRequired = 0;    // number of operations requested
    protected int  numOpsExecuted = 0;    // number of operations executed
    protected long cumulativeTime = 0;    // sum of times for each op
    protected long elapsedTime = 0;       // time from start to finish
    protected boolean keepResults = false;// don't clean base directory on exit
    protected Level logLevel;             // logging level, ERROR by default
    protected int ugcRefreshCount = 0;    // user group cache refresh count

    protected List<StatsDaemon> daemons;

    /**
     * Operation name.
     */
    abstract String getOpName();

    /**
     * Parse command line arguments.
     * 
     * @param args arguments
     * @throws IOException
     */
    abstract void parseArguments(List<String> args) throws IOException;

    /**
     * Generate inputs for each daemon thread.
     * 
     * @param opsPerThread number of inputs for each thread.
     * @throws IOException
     */
    abstract void generateInputs(int[] opsPerThread) throws IOException;

    /**
     * This corresponds to the arg1 argument of 
     * {@link #executeOp(int, int, String)}, which can have different meanings
     * depending on the operation performed.
     * 
     * @param daemonId
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute name-node operation.
     * 
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual name-node call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1) throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      baseDir = BASE_DIR_NAME + "/" + getOpName();
      replication = (short) config.getInt("dfs.replication", 3);
      numOpsRequired = 10;
      numThreads = 3;
      logLevel = Level.ERROR;
      ugcRefreshCount = Integer.MAX_VALUE;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if(numThreads < 1)
          return;
        int tIdx = 0; // thread index < nrThreads
        int opsPerThread[] = new int[numThreads];
        for(int opsScheduled = 0; opsScheduled < numOpsRequired; 
                                  opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] = (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if(opsPerThread[tIdx] == 0)
            opsPerThread[tIdx] = 1;
        }
        // if numThreads > numOpsRequired then the remaining threads will do nothing
        for(; tIdx < numThreads; tIdx++)
          opsPerThread[tIdx] = 0;
        generateInputs(opsPerThread);
        setNameNodeLoggingLevel(logLevel);
        for(tIdx=0; tIdx < numThreads; tIdx++)
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        start = System.currentTimeMillis();
        LOG.info("Starting " + numOpsRequired + " " + getOpName() + "(s).");
        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInPorgress()) {
          // try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
        elapsedTime = System.currentTimeMillis() - start;
        for(StatsDaemon d : daemons) {
          incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          // System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
        }
      }
    }

    private boolean isInPorgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
      return false;
    }

    void cleanUp() throws IOException {
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      if(!keepResults)
        nameNode.delete(getBaseDir(), true);
    }

    int getNumOpsExecuted() {
      return numOpsExecuted;
    }

    long getCumulativeTime() {
      return cumulativeTime;
    }

    long getElapsedTime() {
      return elapsedTime;
    }

    long getAverageTime() {
      return numOpsExecuted == 0 ? 0 : cumulativeTime / numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    String getBaseDir() {
      return baseDir;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     * 
     * @param args
     * @return true if operation is all, which means that options not related
     * to this operation should be ignored, or false otherwise, meaning
     * that usage should be printed when an unrelated option is encountered.
     * @throws IOException
     */
    protected boolean verifyOpArgument(List<String> args) {
      if(args.size() < 2 || ! args.get(0).startsWith("-op"))
        printUsage();

      // process common options
      int krIndex = args.indexOf("-keepResults");
      keepResults = (krIndex >= 0);
      if(keepResults) {
        args.remove(krIndex);
      }

      int llIndex = args.indexOf("-logLevel");
      if(llIndex >= 0) {
        if(args.size() <= llIndex + 1)
          printUsage();
        logLevel = Level.toLevel(args.get(llIndex+1), Level.ERROR);
        args.remove(llIndex+1);
        args.remove(llIndex);
      }

      int ugrcIndex = args.indexOf("-UGCacheRefreshCount");
      if(ugrcIndex >= 0) {
        if(args.size() <= ugrcIndex + 1)
          printUsage();
        int g = Integer.parseInt(args.get(ugrcIndex+1));
        if(g > 0) ugcRefreshCount = g;
        args.remove(ugrcIndex+1);
        args.remove(ugrcIndex);
      }

      String type = args.get(1);
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if(!getOpName().equals(type))
        printUsage();
      return false;
    }

    void printStats() {
      LOG.info("--- " + getOpName() + " stats  ---");
      LOG.info("# operations: " + getNumOpsExecuted());
      LOG.info("Elapsed Time: " + getElapsedTime());
      LOG.info(" Ops per sec: " + getOpsPerSecond());
      LOG.info("Average Time: " + getAverageTime());
    }
  }

  /**
   * One of the threads that perform stats operations.
   */
  private class StatsDaemon extends Thread {
    private int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private OperationStatsBase statsOp;

    StatsDaemon(int daemonId, int nrOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nrOps;
      this.statsOp = op;
      setName(toString());
    }

    public void run() {
      localNumOpsExecuted = 0;
      localCumulativeTime = 0;
      arg1 = statsOp.getExecutionArgument(daemonId);
      try {
        benchmarkOne();
      } catch(IOException ex) {
        LOG.error("StatsDaemon " + daemonId + " failed: \n" 
            + StringUtils.stringifyException(ex));
      }
    }

    public String toString() {
      return "StatsDaemon-" + daemonId;
    }

    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
          nameNode.refreshUserToGroupsMappings();
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
      }
    }

    boolean isInProgress() {
      return localNumOpsExecuted < opsPerThread;
    }

    /**
     * Schedule to stop this daemon.
     */
    void terminate() {
      opsPerThread = localNumOpsExecuted;
    }
  }

  /**
   * Clean all benchmark result directories.
   */
  class CleanAllStats extends OperationStatsBase {
    // Operation types
    static final String OP_CLEAN_NAME = "clean";
    static final String OP_CLEAN_USAGE = "-op clean";

    CleanAllStats(List<String> args) {
      super();
      parseArguments(args);
      numOpsRequired = 1;
      numThreads = 1;
      keepResults = true;
    }

    String getOpName() {
      return OP_CLEAN_NAME;
    }

    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      if(args.size() > 2 && !ignoreUnrelatedOptions)
        printUsage();
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      // do nothing
    }

    /**
     * Does not require the argument
     */
    String getExecutionArgument(int daemonId) {
      return null;
    }

    /**
     * Remove entire benchmark directory.
     */
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      long start = System.currentTimeMillis();
      nameNode.delete(BASE_DIR_NAME, true);
      long end = System.currentTimeMillis();
      return end-start;
    }

    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("Remove directory " + BASE_DIR_NAME);
      printStats();
    }
  }

  /**
   * File creation statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class CreateFileStats extends OperationStatsBase {
    // Operation types
    static final String OP_CREATE_NAME = "create";
    static final String OP_CREATE_USAGE = 
      "-op create [-threads T] [-files N] [-filesPerDir P] [-close]";

    protected FileNameGenerator nameGenerator;
    protected String[][] fileNames;
    private boolean closeUponCreate;

    CreateFileStats(List<String> args) {
      super();
      parseArguments(args);
    }

    String getOpName() {
      return OP_CREATE_NAME;
    }

    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrFilesPerDir = 4;
      closeUponCreate = false;
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-files")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-filesPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrFilesPerDir = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-close")) {
          closeUponCreate = true;
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDir(), nrFilesPerDir);
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length"; 
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      // int generatedFileIdx = 0;
      LOG.info("Generate " + numOpsRequired + " intputs for " + getOpName());
      fileNames = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        fileNames[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          fileNames[idx][jdx] = nameGenerator.
                                  getNextFileName("ThroughputBench");
      }
    }

    void dummyActionNoSynch(int daemonId, int fileIdx) {
      for(int i=0; i < 2000; i++)
        fileNames[daemonId][fileIdx].contains(""+i);
    }

    /**
     * returns client name
     */
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do file create.
     */
    long executeOp(int daemonId, int inputIdx, String clientName) 
    throws IOException {
      long start = System.currentTimeMillis();
      // dummyActionNoSynch(fileIdx);
      nameNode.create(fileNames[daemonId][inputIdx], FsPermission.getDefault(),
                      clientName, true, true, replication, BLOCK_SIZE);
      long end = System.currentTimeMillis();
      for(boolean written = !closeUponCreate; !written; 
        written = nameNode.complete(fileNames[daemonId][inputIdx], clientName));
      return end-start;
    }

    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrFiles = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrFilesPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  /**
   * Open file statistics.
   * 
   * Measure how many open calls (getBlockLocations()) 
   * the name-node can handle per second.
   */
  class OpenFileStats extends CreateFileStats {
    // Operation types
    static final String OP_OPEN_NAME = "open";
    static final String OP_USAGE_ARGS = 
      " [-threads T] [-files N] [-filesPerDir P] [-useExisting]";
    static final String OP_OPEN_USAGE = 
      "-op " + OP_OPEN_NAME + OP_USAGE_ARGS;

    private boolean useExisting;  // do not generate files, use existing ones

    OpenFileStats(List<String> args) {
      super(args);
    }

    String getOpName() {
      return OP_OPEN_NAME;
    }

    void parseArguments(List<String> args) {
      int ueIndex = args.indexOf("-useExisting");
      useExisting = (ueIndex >= 0);
      if(useExisting) {
        args.remove(ueIndex);
      }
      super.parseArguments(args);
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      // create files using opsPerThread
      String[] createArgs = new String[] {
              "-op", "create", 
              "-threads", String.valueOf(this.numThreads), 
              "-files", String.valueOf(numOpsRequired),
              "-filesPerDir", 
              String.valueOf(nameGenerator.getFilesPerDirectory()),
              "-close"};
      CreateFileStats opCreate =  new CreateFileStats(Arrays.asList(createArgs));

      if(!useExisting) {  // create files if they were not created before
        opCreate.benchmark();
        LOG.info("Created " + numOpsRequired + " files.");
      } else {
        LOG.info("useExisting = true. Assuming " 
            + numOpsRequired + " files have been created before.");
      }
      // use the same files for open
      super.generateInputs(opsPerThread);
      if(nameNode.getFileInfo(opCreate.getBaseDir()) != null
          && nameNode.getFileInfo(getBaseDir()) == null) {
        nameNode.rename(opCreate.getBaseDir(), getBaseDir());
      }
      if(nameNode.getFileInfo(getBaseDir()) == null) {
        throw new IOException(getBaseDir() + " does not exist.");
      }
    }

    /**
     * Do file open.
     */
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = System.currentTimeMillis();
      nameNode.getBlockLocations(fileNames[daemonId][inputIdx], 0L, BLOCK_SIZE);
      long end = System.currentTimeMillis();
      return end-start;
    }
  }

  /**
   * Delete file statistics.
   * 
   * Measure how many delete calls the name-node can handle per second.
   */
  class DeleteFileStats extends OpenFileStats {
    // Operation types
    static final String OP_DELETE_NAME = "delete";
    static final String OP_DELETE_USAGE = 
      "-op " + OP_DELETE_NAME + OP_USAGE_ARGS;

    DeleteFileStats(List<String> args) {
      super(args);
    }

    String getOpName() {
      return OP_DELETE_NAME;
    }

    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = System.currentTimeMillis();
      nameNode.delete(fileNames[daemonId][inputIdx], false);
      long end = System.currentTimeMillis();
      return end-start;
    }
  }

  /**
   * Rename file statistics.
   * 
   * Measure how many rename calls the name-node can handle per second.
   */
  class RenameFileStats extends OpenFileStats {
    // Operation types
    static final String OP_RENAME_NAME = "rename";
    static final String OP_RENAME_USAGE = 
      "-op " + OP_RENAME_NAME + OP_USAGE_ARGS;

    protected String[][] destNames;

    RenameFileStats(List<String> args) {
      super(args);
    }

    String getOpName() {
      return OP_RENAME_NAME;
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      super.generateInputs(opsPerThread);
      destNames = new String[fileNames.length][];
      for(int idx=0; idx < numThreads; idx++) {
        int nrNames = fileNames[idx].length;
        destNames[idx] = new String[nrNames];
        for(int jdx=0; jdx < nrNames; jdx++)
          destNames[idx][jdx] = fileNames[idx][jdx] + ".r";
      }
    }

    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = System.currentTimeMillis();
      nameNode.rename(fileNames[daemonId][inputIdx],
                      destNames[daemonId][inputIdx]);
      long end = System.currentTimeMillis();
      return end-start;
    }
  }

  /**
   * Minimal data-node simulator.
   */
  private static class TinyDatanode implements Comparable<String> {
    private static final long DF_CAPACITY = 100*1024*1024;
    private static final long DF_USED = 0;
    
    NamespaceInfo nsInfo;
    DatanodeRegistration dnRegistration;
    Block[] blocks;
    int nrBlocks; // actual number of blocks

    /**
     * Get data-node in the form 
     * <host name> : <port>
     * where port is a 6 digit integer.
     * This is necessary in order to provide lexocographic ordering.
     * Host names are all the same, the ordering goes by port numbers.
     */
    private static String getNodeName(int port) throws IOException {
      String machineName = DNS.getDefaultHost("default", "default");
      String sPort = String.valueOf(100000 + port);
      if(sPort.length() > 6)
        throw new IOException("Too many data-nodes.");
      return machineName + ":" + sPort;
    }

    TinyDatanode(int dnIdx, int blockCapacity) throws IOException {
      dnRegistration = new DatanodeRegistration(getNodeName(dnIdx));
      this.blocks = new Block[blockCapacity];
      this.nrBlocks = 0;
    }

    String getName() {
      return dnRegistration.getName();
    }

    void register() throws IOException {
      // get versions from the namenode
      nsInfo = nameNode.versionRequest();
      dnRegistration.setStorageInfo(new DataStorage(nsInfo, ""));
      DataNode.setNewStorageID(dnRegistration);
      // register datanode
      dnRegistration = nameNode.register(dnRegistration);
    }

    /**
     * Send a heartbeat to the name-node.
     * Ignore reply commands.
     */
    void sendHeartbeat() throws IOException {
      // register datanode
      DatanodeCommand[] cmds = nameNode.sendHeartbeat(
          dnRegistration, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, 0, 0, 0);
      if(cmds != null) {
        for (DatanodeCommand cmd : cmds ) {
          LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
        }
      }
    }

    boolean addBlock(Block blk) {
      if(nrBlocks == blocks.length) {
        LOG.debug("Cannot add block: datanode capacity = " + blocks.length);
        return false;
      }
      blocks[nrBlocks] = blk;
      nrBlocks++;
      return true;
    }

    void formBlockReport() {
      // fill remaining slots with blocks that do not exist
      for(int idx = blocks.length-1; idx >= nrBlocks; idx--)
        blocks[idx] = new Block(blocks.length - idx, 0, 0);
    }

    public int compareTo(String name) {
      return getName().compareTo(name);
    }

    /**
     * Send a heartbeat to the name-node and replicate blocks if requested.
     */
    int replicateBlocks() throws IOException {
      // register datanode
      DatanodeCommand[] cmds = nameNode.sendHeartbeat(
          dnRegistration, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, 0, 0, 0);
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          if (cmd.getAction() == DatanodeProtocol.DNA_TRANSFER) {
            // Send a copy of a block to another datanode
            BlockCommand bcmd = (BlockCommand)cmd;
            return transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
          }
        }
      }
      return 0;
    }

    /**
     * Transfer blocks to another data-node.
     * Just report on behalf of the other data-node
     * that the blocks have been received.
     */
    private int transferBlocks( Block blocks[], 
                                DatanodeInfo xferTargets[][] 
                              ) throws IOException {
      for(int i = 0; i < blocks.length; i++) {
        DatanodeInfo blockTargets[] = xferTargets[i];
        for(int t = 0; t < blockTargets.length; t++) {
          DatanodeInfo dnInfo = blockTargets[t];
          DatanodeRegistration receivedDNReg;
          receivedDNReg = new DatanodeRegistration(dnInfo.getName());
          receivedDNReg.setStorageInfo(
                          new DataStorage(nsInfo, dnInfo.getStorageID()));
          receivedDNReg.setInfoPort(dnInfo.getInfoPort());
          nameNode.blockReceived( receivedDNReg, 
                                  new Block[] {blocks[i]},
                                  new String[] {DataNode.EMPTY_DEL_HINT});
        }
      }
      return blocks.length;
    }
  }

  /**
   * Block report statistics.
   * 
   * Each thread here represents its own data-node.
   * Data-nodes send the same block report each time.
   * The block report may contain missing or non-existing blocks.
   */
  class BlockReportStats extends OperationStatsBase {
    static final String OP_BLOCK_REPORT_NAME = "blockReport";
    static final String OP_BLOCK_REPORT_USAGE = 
      "-op blockReport [-datanodes T] [-reports N] " +
      "[-blocksPerReport B] [-blocksPerFile F]";

    private int blocksPerReport;
    private int blocksPerFile;
    private TinyDatanode[] datanodes; // array of data-nodes sorted by name

    BlockReportStats(List<String> args) {
      super();
      this.blocksPerReport = 100;
      this.blocksPerFile = 10;
      // set heartbeat interval to 3 min, so that expiration were 40 min
      config.setLong("dfs.heartbeat.interval", 3 * 60);
      parseArguments(args);
      // adjust replication to the number of data-nodes
      this.replication = (short)Math.min((int)replication, getNumDatanodes());
    }

    /**
     * Each thread pretends its a data-node here.
     */
    private int getNumDatanodes() {
      return numThreads;
    }

    String getOpName() {
      return OP_BLOCK_REPORT_NAME;
    }

    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-reports")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-datanodes")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-blocksPerReport")) {
          if(i+1 == args.size())  printUsage();
          blocksPerReport = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-blocksPerFile")) {
          if(i+1 == args.size())  printUsage();
          blocksPerFile = Integer.parseInt(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    void generateInputs(int[] ignore) throws IOException {
      int nrDatanodes = getNumDatanodes();
      int nrBlocks = (int)Math.ceil((double)blocksPerReport * nrDatanodes 
                                    / replication);
      int nrFiles = (int)Math.ceil((double)nrBlocks / blocksPerFile);
      datanodes = new TinyDatanode[nrDatanodes];
      // create data-nodes
      String prevDNName = "";
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx] = new TinyDatanode(idx, blocksPerReport);
        datanodes[idx].register();
        assert datanodes[idx].getName().compareTo(prevDNName) > 0
          : "Data-nodes must be sorted lexicographically.";
        datanodes[idx].sendHeartbeat();
        prevDNName = datanodes[idx].getName();
      }

      // create files 
      LOG.info("Creating " + nrFiles + " with " + blocksPerFile + " blocks each.");
      FileNameGenerator nameGenerator;
      nameGenerator = new FileNameGenerator(getBaseDir(), 100);
      String clientName = getClientName(007);
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      for(int idx=0; idx < nrFiles; idx++) {
        String fileName = nameGenerator.getNextFileName("ThroughputBench");
        nameNode.create(fileName, FsPermission.getDefault(),
                        clientName, true, true, replication, BLOCK_SIZE);
        addBlocks(fileName, clientName);
        nameNode.complete(fileName, clientName);
      }
      // prepare block reports
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx].formBlockReport();
      }
    }

    private void addBlocks(String fileName, String clientName) throws IOException {
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = nameNode.addBlock(fileName, clientName);
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getName());
          datanodes[dnIdx].addBlock(loc.getBlock());
          nameNode.blockReceived(
              datanodes[dnIdx].dnRegistration, 
              new Block[] {loc.getBlock()},
              new String[] {""});
        }
      }
    }

    /**
     * Does not require the argument
     */
    String getExecutionArgument(int daemonId) {
      return null;
    }

    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      TinyDatanode dn = datanodes[daemonId];
      long start = System.currentTimeMillis();
      nameNode.blockReport(dn.dnRegistration,
          BlockListAsLongs.convertToArrayLongs(dn.blocks));
      long end = System.currentTimeMillis();
      return end-start;
    }

    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      for(int idx=0; idx < getNumDatanodes(); idx++) {
        blockDistribution += delim + datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("reports = " + numOpsRequired);
      LOG.info("datanodes = " + numThreads + " " + blockDistribution);
      LOG.info("blocksPerReport = " + blocksPerReport);
      LOG.info("blocksPerFile = " + blocksPerFile);
      printStats();
    }
  }   // end BlockReportStats

  /**
   * Measures how fast replication monitor can compute data-node work.
   * 
   * It runs only one thread until no more work can be scheduled.
   */
  class ReplicationStats extends OperationStatsBase {
    static final String OP_REPLICATION_NAME = "replication";
    static final String OP_REPLICATION_USAGE = 
      "-op replication [-datanodes T] [-nodesToDecommission D] " +
      "[-nodeReplicationLimit C] [-totalBlocks B] [-replication R]";

    private BlockReportStats blockReportObject;
    private int numDatanodes;
    private int nodesToDecommission;
    private int nodeReplicationLimit;
    private int totalBlocks;
    private int numDecommissionedBlocks;
    private int numPendingBlocks;

    ReplicationStats(List<String> args) {
      super();
      numThreads = 1;
      numDatanodes = 3;
      nodesToDecommission = 1;
      nodeReplicationLimit = 100;
      totalBlocks = 100;
      parseArguments(args);
      // number of operations is 4 times the number of decommissioned
      // blocks divided by the number of needed replications scanned 
      // by the replication monitor in one iteration
      numOpsRequired = (totalBlocks*replication*nodesToDecommission*2)
            / (numDatanodes*numDatanodes);

      String[] blkReportArgs = {
        "-op", "blockReport",
        "-datanodes", String.valueOf(numDatanodes),
        "-blocksPerReport", String.valueOf(totalBlocks*replication/numDatanodes),
        "-blocksPerFile", String.valueOf(numDatanodes)};
      blockReportObject = new BlockReportStats(Arrays.asList(blkReportArgs));
      numDecommissionedBlocks = 0;
      numPendingBlocks = 0;
    }

    String getOpName() {
      return OP_REPLICATION_NAME;
    }

    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-datanodes")) {
          if(i+1 == args.size())  printUsage();
          numDatanodes = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-nodesToDecommission")) {
          if(i+1 == args.size())  printUsage();
          nodesToDecommission = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-nodeReplicationLimit")) {
          if(i+1 == args.size())  printUsage();
          nodeReplicationLimit = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-totalBlocks")) {
          if(i+1 == args.size())  printUsage();
          totalBlocks = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-replication")) {
          if(i+1 == args.size())  printUsage();
          replication = Short.parseShort(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    void generateInputs(int[] ignore) throws IOException {
      // start data-nodes; create a bunch of files; generate block reports.
      blockReportObject.generateInputs(ignore);
      // stop replication monitor
      nameNode.namesystem.replthread.interrupt();
      try {
        nameNode.namesystem.replthread.join();
      } catch(InterruptedException ei) {
        return;
      }
      // report blocks once
      int nrDatanodes = blockReportObject.getNumDatanodes();
      for(int idx=0; idx < nrDatanodes; idx++) {
        blockReportObject.executeOp(idx, 0, null);
      }
      // decommission data-nodes
      decommissionNodes();
      // set node replication limit
      nameNode.namesystem.setNodeReplicationLimit(nodeReplicationLimit);
    }

    private void decommissionNodes() throws IOException {
      String excludeFN = config.get("dfs.hosts.exclude", "exclude");
      FileOutputStream excludeFile = new FileOutputStream(excludeFN);
      excludeFile.getChannel().truncate(0L);
      int nrDatanodes = blockReportObject.getNumDatanodes();
      numDecommissionedBlocks = 0;
      for(int i=0; i < nodesToDecommission; i++) {
        TinyDatanode dn = blockReportObject.datanodes[nrDatanodes-1-i];
        numDecommissionedBlocks += dn.nrBlocks;
        excludeFile.write(dn.getName().getBytes());
        excludeFile.write('\n');
        LOG.info("Datanode " + dn.getName() + " is decommissioned.");
      }
      excludeFile.close();
      nameNode.refreshNodes();
    }

    /**
     * Does not require the argument
     */
    String getExecutionArgument(int daemonId) {
      return null;
    }

    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      long start = System.currentTimeMillis();
      // compute data-node work
      int work = nameNode.namesystem.computeDatanodeWork();
      long end = System.currentTimeMillis();
      numPendingBlocks += work;
      if(work == 0)
        daemons.get(daemonId).terminate();
      return end-start;
    }

    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      int totalReplicas = 0;
      for(int idx=0; idx < blockReportObject.getNumDatanodes(); idx++) {
        totalReplicas += blockReportObject.datanodes[idx].nrBlocks;
        blockDistribution += delim + blockReportObject.datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("numOpsRequired = " + numOpsRequired);
      LOG.info("datanodes = " + numDatanodes + " " + blockDistribution);
      LOG.info("decommissioned datanodes = " + nodesToDecommission);
      LOG.info("datanode replication limit = " + nodeReplicationLimit);
      LOG.info("total blocks = " + totalBlocks);
      printStats();
      LOG.info("decommissioned blocks = " + numDecommissionedBlocks);
      LOG.info("pending replications = " + numPendingBlocks);
      LOG.info("replications per sec: " + getBlocksPerSecond());
    }

    private double getBlocksPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numPendingBlocks / elapsedTime;
    }

  }   // end ReplicationStats

  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + CreateFileStats.OP_CREATE_USAGE
        + " | \n\t" + OpenFileStats.OP_OPEN_USAGE
        + " | \n\t" + DeleteFileStats.OP_DELETE_USAGE
        + " | \n\t" + RenameFileStats.OP_RENAME_USAGE
        + " | \n\t" + BlockReportStats.OP_BLOCK_REPORT_USAGE
        + " | \n\t" + ReplicationStats.OP_REPLICATION_USAGE
        + " | \n\t" + CleanAllStats.OP_CLEAN_USAGE
        + " | \n\t" + GENERAL_OPTIONS_USAGE
    );
    System.exit(-1);
  }

  /**
   * Main method of the benchmark.
   * @param args command line parameters
   */
  public static void runBenchmark(Configuration conf, List<String> args) throws Exception {
    if(args.size() < 2 || ! args.get(0).startsWith("-op"))
      printUsage();

    String type = args.get(1);
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    NNThroughputBenchmark bench = null;
    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
      bench = new NNThroughputBenchmark(conf);
      if(runAll || CreateFileStats.OP_CREATE_NAME.equals(type)) {
        opStat = bench.new CreateFileStats(args);
        ops.add(opStat);
      }
      if(runAll || OpenFileStats.OP_OPEN_NAME.equals(type)) {
        opStat = bench.new OpenFileStats(args);
        ops.add(opStat);
      }
      if(runAll || DeleteFileStats.OP_DELETE_NAME.equals(type)) {
        opStat = bench.new DeleteFileStats(args);
        ops.add(opStat);
      }
      if(runAll || RenameFileStats.OP_RENAME_NAME.equals(type)) {
        opStat = bench.new RenameFileStats(args);
        ops.add(opStat);
      }
      if(runAll || BlockReportStats.OP_BLOCK_REPORT_NAME.equals(type)) {
        opStat = bench.new BlockReportStats(args);
        ops.add(opStat);
      }
      if(runAll || ReplicationStats.OP_REPLICATION_NAME.equals(type)) {
        opStat = bench.new ReplicationStats(args);
        ops.add(opStat);
      }
      if(runAll || CleanAllStats.OP_CLEAN_NAME.equals(type)) {
        opStat = bench.new CleanAllStats(args);
        ops.add(opStat);
      }
      if(ops.size() == 0)
        printUsage();
      // run each benchmark
      for(OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for(OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  public static void main(String[] args) throws Exception {
    runBenchmark(new Configuration(), 
                  new ArrayList<String>(Arrays.asList(args)));
  }
}
