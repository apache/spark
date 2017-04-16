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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

/*
 * This keeps track of blocks and their last verification times.
 * Currently it does not modify the metadata for block.
 */

class DataBlockScanner implements Runnable {
  
  public static final Log LOG = LogFactory.getLog(DataBlockScanner.class);
  
  private static final int MAX_SCAN_RATE = 8 * 1024 * 1024; // 8MB per sec
  private static final int MIN_SCAN_RATE = 1 * 1024 * 1024; // 1MB per sec
  
  static final long DEFAULT_SCAN_PERIOD_HOURS = 21*24L; // three weeks
  private static final long ONE_DAY = 24*3600*1000L;
  
  static final DateFormat dateFormat = 
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  
  static final String verificationLogFile = "dncp_block_verification.log";
  static final int verficationLogLimit = 5; // * numBlocks.

  private long scanPeriod = DEFAULT_SCAN_PERIOD_HOURS * 3600 * 1000;
  DataNode datanode;
  FSDataset dataset;
  
  // sorted set
  TreeSet<BlockScanInfo> blockInfoSet;
  HashMap<Block, BlockScanInfo> blockMap;
  
  long totalScans = 0;
  long totalVerifications = 0; // includes remote verification by clients.
  long totalScanErrors = 0;
  long totalTransientErrors = 0;
  
  long currentPeriodStart = System.currentTimeMillis();
  long bytesLeft = 0; // Bytes to scan in this period
  long totalBytesToScan = 0;
  
  private LogFileHandler verificationLog;
  
  Random random = new Random();
  
  BlockTransferThrottler throttler = null;
  
  private static enum ScanType {
    REMOTE_READ,           // Verified when a block read by a client etc
    VERIFICATION_SCAN,     // scanned as part of periodic verfication
    NONE,
  }
  
  static class BlockScanInfo implements Comparable<BlockScanInfo> {
    Block block;
    long lastScanTime = 0;
    long lastLogTime = 0;
    ScanType lastScanType = ScanType.NONE; 
    boolean lastScanOk = true;
    
    BlockScanInfo(Block block) {
      this.block = block;
    }
    
    public int hashCode() {
      return block.hashCode();
    }
    
    public boolean equals(Object other) {
      return other instanceof BlockScanInfo &&
             compareTo((BlockScanInfo)other) == 0;
    }
    
    long getLastScanTime() {
      return ( lastScanType == ScanType.NONE) ? 0 : lastScanTime;
    }
    
    public int compareTo(BlockScanInfo other) {
      long t1 = lastScanTime;
      long t2 = other.lastScanTime;
      return ( t1 < t2 ) ? -1 : 
                          (( t1 > t2 ) ? 1 : block.compareTo(other.block)); 
    }
  }
  
  DataBlockScanner(DataNode datanode, FSDataset dataset, Configuration conf) {
    this.datanode = datanode;
    this.dataset = dataset;
    scanPeriod = conf.getInt("dfs.datanode.scan.period.hours", 0);
    if ( scanPeriod <= 0 ) {
      scanPeriod = DEFAULT_SCAN_PERIOD_HOURS;
    }
    scanPeriod *= 3600 * 1000;
    // initialized when the scanner thread is started.
  }
  
  private synchronized boolean isInitialized() {
    return throttler != null;
  }
  
  private void updateBytesToScan(long len, long lastScanTime) {
    // len could be negative when a block is deleted.
    totalBytesToScan += len;
    if ( lastScanTime < currentPeriodStart ) {
      bytesLeft += len;
    }
    // Should we change throttler bandwidth every time bytesLeft changes?
    // not really required.
  }
  
  private synchronized void addBlockInfo(BlockScanInfo info) {
    boolean added = blockInfoSet.add(info);
    blockMap.put(info.block, info);
    
    if ( added ) {
      LogFileHandler log = verificationLog;
      if (log != null) {
        log.setMaxNumLines(blockMap.size() * verficationLogLimit);
      }
      updateBytesToScan(info.block.getNumBytes(), info.lastScanTime);
    }
  }
  
  private synchronized void delBlockInfo(BlockScanInfo info) {
    boolean exists = blockInfoSet.remove(info);
    blockMap.remove(info.block);
    if ( exists ) {
      LogFileHandler log = verificationLog;
      if (log != null) {
        log.setMaxNumLines(blockMap.size() * verficationLogLimit);
      }
      updateBytesToScan(-info.block.getNumBytes(), info.lastScanTime);
    }
  }
  
  /** Update blockMap by the given LogEntry */
  private synchronized void updateBlockInfo(LogEntry e) {
    BlockScanInfo info = blockMap.get(new Block(e.blockId, 0, e.genStamp));
    
    if(info != null && e.verificationTime > 0 && 
        info.lastScanTime < e.verificationTime) {
      delBlockInfo(info);
      info.lastScanTime = e.verificationTime;
      info.lastScanType = ScanType.VERIFICATION_SCAN;
      addBlockInfo(info);
    }
  }

  private void init() throws InterruptedException {
    
    // get the list of blocks and arrange them in random order
    Block arr[] = dataset.getBlockReport();
    Collections.shuffle(Arrays.asList(arr));
    
    blockInfoSet = new TreeSet<BlockScanInfo>();
    blockMap = new HashMap<Block, BlockScanInfo>();
    
    long scanTime = -1;
    for (Block block : arr) {
      BlockScanInfo info = new BlockScanInfo( block );
      info.lastScanTime = scanTime--; 
      //still keep 'info.lastScanType' to NONE.
      addBlockInfo(info);
    }

    /* Pick the first directory that has any existing scanner log.
     * otherwise, pick the first directory.
     */
    File dir = null;
    FSDataset.FSVolume[] volumes = dataset.volumes.volumes;
    for(FSDataset.FSVolume vol : volumes) {
      if (LogFileHandler.isFilePresent(vol.getDir(), verificationLogFile)) {
        dir = vol.getDir();
        break;
      }
    }
    if (dir == null) {
      dir = volumes[0].getDir();
    }
    
    try {
      // max lines will be updated later during initialization.
      verificationLog = new LogFileHandler(dir, verificationLogFile, 100);
    } catch (IOException e) {
      LOG.warn("Could not open verfication log. " +
               "Verification times are not stored.");
    }
    
    synchronized (this) {
      throttler = new BlockTransferThrottler(200, MAX_SCAN_RATE);
    }
  }

  private synchronized long getNewBlockScanTime() {
    /* If there are a lot of blocks, this returns a random time with in 
     * the scan period. Otherwise something sooner.
     */
    long period = Math.min(scanPeriod, 
                           Math.max(blockMap.size(),1) * 600 * 1000L);
    int periodInt = Math.abs((int)period);
    return System.currentTimeMillis() - scanPeriod + 
           random.nextInt(periodInt);
  }

  /** Adds block to list of blocks */
  synchronized void addBlock(Block block) {
    if (!isInitialized()) {
      return;
    }
    
    BlockScanInfo info = blockMap.get(block);
    if ( info != null ) {
      LOG.warn("Adding an already existing block " + block);
      delBlockInfo(info);
    }
    
    info = new BlockScanInfo(block);    
    info.lastScanTime = getNewBlockScanTime();
    
    addBlockInfo(info);
    adjustThrottler();
  }
  
  /** Deletes the block from internal structures */
  synchronized void deleteBlock(Block block) {
    if (!isInitialized()) {
      return;
    }
    BlockScanInfo info = blockMap.get(block);
    if ( info != null ) {
      delBlockInfo(info);
    }
  }

  /** @return the last scan time */
  synchronized long getLastScanTime(Block block) {
    if (!isInitialized()) {
      return 0;
    }
    BlockScanInfo info = blockMap.get(block);
    return info == null? 0: info.lastScanTime;
  }

  /** Deletes blocks from internal structures */
  void deleteBlocks(Block[] blocks) {
    for ( Block b : blocks ) {
      deleteBlock(b);
    }
  }
  
  /*
    A reader will try to indicate a block is verified and will add blocks 
    to the DataBlockScanner before they are finished (due to concurrent 
    readers).
    
    fixed so a read verification can't add the block
  */
  synchronized void verifiedByClient(Block block) {
    updateScanStatusInternal(block, ScanType.REMOTE_READ, true, true);
  }
  
  private synchronized void updateScanStatus(
    Block block, 
    ScanType type,
    boolean scanOk
  ) {
    updateScanStatusInternal(block, type, scanOk, false);
  }

  /**
   * @param block  - block to update status for 
   * @param type - client, DN, ...
   * @param scanOk - result of scan
   * @param updateOnly - if true, cannot add a block, but only update an
   *                     existing block
   */
  private synchronized void updateScanStatusInternal(
    Block block, 
    ScanType type,
    boolean scanOk,
    boolean updateOnly
  ) {
    if (!isInitialized()) {
      return;
    }

    BlockScanInfo info = blockMap.get(block);
    
    if ( info != null ) {
      delBlockInfo(info);
    } else {
      if (updateOnly) {
        return;
      }
      // It might already be removed. Thats ok, it will be caught next time.
      info = new BlockScanInfo(block);
    }
    
    long now = System.currentTimeMillis();
    info.lastScanType = type;
    info.lastScanTime = now;
    info.lastScanOk = scanOk;
    addBlockInfo(info);
    
    if (type == ScanType.REMOTE_READ) {
      totalVerifications++;
    }
        
    // Don't update meta data too often in case of REMOTE_READ
    // of if the verification failed.
    long diff = now - info.lastLogTime;
    if (!scanOk || (type == ScanType.REMOTE_READ &&
                    diff < scanPeriod/3 && diff < ONE_DAY)) {
      return;
    }
    
    info.lastLogTime = now;
    LogFileHandler log = verificationLog;
    if (log != null) {
      log.appendLine(LogEntry.newEnry(block, now));
    }
  }
  
  private void handleScanFailure(Block block) {
    
    LOG.info("Reporting bad block " + block + " to namenode.");
    
    try {
      DatanodeInfo[] dnArr = { new DatanodeInfo(datanode.dnRegistration) };
      LocatedBlock[] blocks = { new LocatedBlock(block, dnArr) }; 
      datanode.namenode.reportBadBlocks(blocks);
    } catch (IOException e){
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block " + block + " to namenode : " +
               " Exception : " + StringUtils.stringifyException(e));
    }
  }
  
  static private class LogEntry {
    long blockId = -1;
    long verificationTime = -1;
    long genStamp = Block.GRANDFATHER_GENERATION_STAMP;
    
    /**
     * The format consists of single line with multiple entries. each 
     * entry is in the form : name="value".
     * This simple text and easily extendable and easily parseable with a
     * regex.
     */
    private static Pattern entryPattern = 
      Pattern.compile("\\G\\s*([^=\\p{Space}]+)=\"(.*?)\"\\s*");
    
    static String newEnry(Block block, long time) {
      return "date=\"" + dateFormat.format(new Date(time)) + "\"\t " +
             "time=\"" + time + "\"\t " +
             "genstamp=\"" + block.getGenerationStamp() + "\"\t " +
             "id=\"" + block.getBlockId() +"\"";
    }
    
    static LogEntry parseEntry(String line) {
      LogEntry entry = new LogEntry();
      
      Matcher matcher = entryPattern.matcher(line);
      while (matcher.find()) {
        String name = matcher.group(1);
        String value = matcher.group(2);
        
        try {
          if (name.equals("id")) {
            entry.blockId = Long.valueOf(value);
          } else if (name.equals("time")) {
            entry.verificationTime = Long.valueOf(value);
          } else if (name.equals("genstamp")) {
            entry.genStamp = Long.valueOf(value);
          }
        } catch(NumberFormatException nfe) {
          LOG.warn("Cannot parse line: " + line, nfe);
          return null;
        }
      }
      
      return entry;
    }
  }
  
  private synchronized void adjustThrottler() {
    long timeLeft = currentPeriodStart+scanPeriod - System.currentTimeMillis();
    long bw = Math.max(bytesLeft*1000/timeLeft, MIN_SCAN_RATE);
    throttler.setBandwidth(Math.min(bw, MAX_SCAN_RATE));
  }
  
  private void verifyBlock(Block block) {
    
    BlockSender blockSender = null;

    /* In case of failure, attempt to read second time to reduce
     * transient errors. How do we flush block data from kernel 
     * buffers before the second read? 
     */
    for (int i=0; i<2; i++) {
      boolean second = (i > 0);
      
      try {
        adjustThrottler();
        
        blockSender = new BlockSender(block, 0, -1, false, 
                                               false, true, datanode);

        DataOutputStream out = 
                new DataOutputStream(new IOUtils.NullOutputStream());
        
        blockSender.sendBlock(out, null, throttler);

        LOG.info((second ? "Second " : "") +
                 "Verification succeeded for " + block);
        
        if ( second ) {
          totalTransientErrors++;
        }
        
        updateScanStatus(block, ScanType.VERIFICATION_SCAN, true);

        return;
      } catch (IOException e) {

        totalScanErrors++;
        updateScanStatus(block, ScanType.VERIFICATION_SCAN, false);

        // If the block does not exists anymore, then its not an error
        if ( dataset.getFile(block) == null ) {
          LOG.info("Verification failed for " + block + ". Its ok since " +
          "it not in datanode dataset anymore.");
          deleteBlock(block);
          return;
        }

        LOG.warn((second ? "Second " : "First ") + 
                 "Verification failed for " + block + ". Exception : " +
                 StringUtils.stringifyException(e));
        
        if (second) {
          datanode.getMetrics().blockVerificationFailures.inc(); 
          handleScanFailure(block);
          return;
        } 
      } finally {
        IOUtils.closeStream(blockSender);
        datanode.getMetrics().blocksVerified.inc();
        totalScans++;
        totalVerifications++;
      }
    }
  }
  
  private synchronized long getEarliestScanTime() {
    if ( blockInfoSet.size() > 0 ) {
      return blockInfoSet.first().lastScanTime;
    }
    return Long.MAX_VALUE; 
  }
  
  // Picks one block and verifies it
  private void verifyFirstBlock() {
    Block block = null;
    synchronized (this) {
      if ( blockInfoSet.size() > 0 ) {
        block = blockInfoSet.first().block;
      }
    }
    
    if ( block != null ) {
      verifyBlock(block);
    }
  }
  
  /** returns false if the process was interrupted
   * because the thread is marked to exit.
   */
  private boolean assignInitialVerificationTimes() {
    int numBlocks = 1;
    synchronized (this) {
      numBlocks = Math.max(blockMap.size(), 1);
    }
    
    //First udpates the last verification times from the log file.
    LogFileHandler.Reader logReader = null;
    try {
      if (verificationLog != null) {
        logReader = verificationLog.new Reader(false);
      }
    } catch (IOException e) {
      LOG.warn("Could not read previous verification times : " +
               StringUtils.stringifyException(e));
    }
    
    if (verificationLog != null) {
      verificationLog.updateCurNumLines();
    }
    
    try {
    // update verification times from the verificationLog.
    while (logReader != null && logReader.hasNext()) {
      if (!datanode.shouldRun || Thread.interrupted()) {
        return false;
      }
      LogEntry entry = LogEntry.parseEntry(logReader.next());
      if (entry != null) {
        updateBlockInfo(entry);
      }
    }
    } finally {
      IOUtils.closeStream(logReader);
    }
    
    /* Initially spread the block reads over half of 
     * MIN_SCAN_PERIOD so that we don't keep scanning the 
     * blocks too quickly when restarted.
     */
    long verifyInterval = (long) (Math.min( scanPeriod/2.0/numBlocks,
                                            10*60*1000 ));
    long lastScanTime = System.currentTimeMillis() - scanPeriod;
    
    /* Before this loop, entries in blockInfoSet that are not
     * updated above have lastScanTime of <= 0 . Loop until first entry has
     * lastModificationTime > 0.
     */    
    synchronized (this) {
      if (blockInfoSet.size() > 0 ) {
        BlockScanInfo info;
        while ((info =  blockInfoSet.first()).lastScanTime < 0) {
          delBlockInfo(info);        
          info.lastScanTime = lastScanTime;
          lastScanTime += verifyInterval;
          addBlockInfo(info);
        }
      }
    }
    
    return true;
  }
  
  private synchronized void startNewPeriod() {
    LOG.info("Starting a new period : work left in prev period : " +
             String.format("%.2f%%", (bytesLeft * 100.0)/totalBytesToScan));
    // reset the byte counts :
    bytesLeft = totalBytesToScan;
    currentPeriodStart = System.currentTimeMillis();
  }
  
  public void run() {
    try {
      
      init();
      
      //Read last verification times
      if (!assignInitialVerificationTimes()) {
        return;
      }
      
      adjustThrottler();
      
      while (datanode.shouldRun && !Thread.interrupted()) {
        long now = System.currentTimeMillis();
        synchronized (this) {
          if ( now >= (currentPeriodStart + scanPeriod)) {
            startNewPeriod();
          }
        }
        if ( (now - getEarliestScanTime()) >= scanPeriod ) {
          verifyFirstBlock();
        } else {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {}
        }
      }
    } catch (InterruptedException ie) {
      LOG.info("DataBlockScanner interrupted");
    } catch (RuntimeException e) {
      LOG.warn("RuntimeException during DataBlockScanner.run() : " +
          e.getMessage() + "  " + StringUtils.stringifyException(e));
      throw e;
    } finally {
      shutdown();
      LOG.info("Exiting DataBlockScanner thread.");
    }
  }
  
  synchronized void shutdown() {
    LogFileHandler log = verificationLog;
    verificationLog = null;
    if (log != null) {
      log.close();
    }
  }

  synchronized void printBlockReport(StringBuilder buffer, 
                                     boolean summaryOnly) {
    long oneHour = 3600*1000;
    long oneDay = 24*oneHour;
    long oneWeek = 7*oneDay;
    long fourWeeks = 4*oneWeek;
    
    int inOneHour = 0;
    int inOneDay = 0;
    int inOneWeek = 0;
    int inFourWeeks = 0;
    int inScanPeriod = 0;
    int neverScanned = 0;
    
    int total = blockInfoSet.size();
    
    long now = System.currentTimeMillis();
    
    Date date = new Date();
    
    for(Iterator<BlockScanInfo> it = blockInfoSet.iterator(); it.hasNext();) {
      BlockScanInfo info = it.next();
      
      long scanTime = info.getLastScanTime();
      long diff = now - scanTime;
      
      if (diff <= oneHour) inOneHour++;
      if (diff <= oneDay) inOneDay++;
      if (diff <= oneWeek) inOneWeek++;
      if (diff <= fourWeeks) inFourWeeks++;
      if (diff <= scanPeriod) inScanPeriod++;      
      if (scanTime <= 0) neverScanned++;
      
      if (!summaryOnly) {
        date.setTime(scanTime);
        String scanType = 
          (info.lastScanType == ScanType.REMOTE_READ) ? "remote" : 
            ((info.lastScanType == ScanType.VERIFICATION_SCAN) ? "local" :
              "none");
        buffer.append(String.format("%-26s : status : %-6s type : %-6s" +
        		                        " scan time : " +
                                    "%-15d %s\n", info.block, 
                                    (info.lastScanOk ? "ok" : "failed"),
                                    scanType, scanTime,
                                    (scanTime <= 0) ? "not yet verified" : 
                                      dateFormat.format(date)));
      }
    }
    
    double pctPeriodLeft = (scanPeriod + currentPeriodStart - now)
                           *100.0/scanPeriod;
    double pctProgress = (totalBytesToScan == 0) ? 100 :
                         (totalBytesToScan-bytesLeft)*10000.0/totalBytesToScan/
                         (100-pctPeriodLeft+1e-10);
    
    buffer.append(String.format("\nTotal Blocks                 : %6d" +
                                "\nVerified in last hour        : %6d" +
                                "\nVerified in last day         : %6d" +
                                "\nVerified in last week        : %6d" +
                                "\nVerified in last four weeks  : %6d" +
                                "\nVerified in SCAN_PERIOD      : %6d" +
                                "\nNot yet verified             : %6d" +
                                "\nVerified since restart       : %6d" +
                                "\nScans since restart          : %6d" +
                                "\nScan errors since restart    : %6d" +
                                "\nTransient scan errors        : %6d" +
                                "\nCurrent scan rate limit KBps : %6d" +
                                "\nProgress this period         : %6.0f%%" +
                                "\nTime left in cur period      : %6.2f%%" +
                                "\n", 
                                total, inOneHour, inOneDay, inOneWeek,
                                inFourWeeks, inScanPeriod, neverScanned,
                                totalVerifications, totalScans, 
                                totalScanErrors, totalTransientErrors, 
                                Math.round(throttler.getBandwidth()/1024.0),
                                pctProgress, pctPeriodLeft));
  }
  
  /**
   * This class takes care of log file used to store the last verification
   * times of the blocks. It rolls the current file when it is too big etc.
   * If there is an error while writing, it stops updating with an error
   * message.
   */
  private static class LogFileHandler {
    
    private static final String curFileSuffix = ".curr";
    private static final String prevFileSuffix = ".prev";
    
    // Don't roll files more often than this
    private static final long minRollingPeriod = 6 * 3600 * 1000L; // 6 hours
    private static final long minWarnPeriod = minRollingPeriod;
    private static final int minLineLimit = 1000;
    
    
    static boolean isFilePresent(File dir, String filePrefix) {
      return new File(dir, filePrefix + curFileSuffix).exists() ||
             new File(dir, filePrefix + prevFileSuffix).exists();
    }
    private File curFile;
    private File prevFile;
    
    private int maxNumLines = -1; // not very hard limit on number of lines.
    private int curNumLines = -1;
    
    long lastWarningTime = 0;
    
    private PrintStream out;
    
    int numReaders = 0;
        
    /**
     * Opens the log file for appending.
     * Note that rolling will happen only after "updateLineCount()" is 
     * called. This is so that line count could be updated in a separate
     * thread without delaying start up.
     * 
     * @param dir where the logs files are located.
     * @param filePrefix prefix of the file.
     * @param maxNumLines max lines in a file (its a soft limit).
     * @throws IOException
     */
    LogFileHandler(File dir, String filePrefix, int maxNumLines) 
                                                throws IOException {
      curFile = new File(dir, filePrefix + curFileSuffix);
      prevFile = new File(dir, filePrefix + prevFileSuffix);
      openCurFile();
      curNumLines = -1;
      setMaxNumLines(maxNumLines);
    }
    
    // setting takes affect when next entry is added.
    synchronized void setMaxNumLines(int maxNumLines) {
      this.maxNumLines = Math.max(maxNumLines, minLineLimit);
    }
    
    /**
     * Append "\n" + line.
     * If the log file need to be rolled, it will done after 
     * appending the text.
     * This does not throw IOException when there is an error while 
     * appending. Currently does not throw an error even if rolling 
     * fails (may be it should?).
     * return true if append was successful.
     */
    synchronized boolean appendLine(String line) {
      out.println();
      out.print(line);
      curNumLines += (curNumLines < 0) ? -1 : 1;
      try {
        rollIfRequired();
      } catch (IOException e) {
        warn("Rolling failed for " + curFile + " : " + e.getMessage());
        return false;
      }
      return true;
    }
    
    //warns only once in a while
    synchronized private void warn(String msg) {
      long now = System.currentTimeMillis();
      if ((now - lastWarningTime) >= minWarnPeriod) {
        lastWarningTime = now;
        LOG.warn(msg);
      }
    }
    
    private synchronized void openCurFile() throws FileNotFoundException {
      close();
      out = new PrintStream(new FileOutputStream(curFile, true));
    }
    
    //This reads the current file and updates the count.
    void updateCurNumLines() {
      int count = 0;
      Reader it = null;
      try {
        for(it = new Reader(true); it.hasNext(); count++) {
          it.next();
        }
      } catch (IOException e) {
        
      } finally {
        synchronized (this) {
          curNumLines = count;
        }
        IOUtils.closeStream(it);
      }
    }
    
    private void rollIfRequired() throws IOException {
      if (curNumLines < maxNumLines || numReaders > 0) {
        return;
      }
      
      long now = System.currentTimeMillis();
      if (now < minRollingPeriod) {
        return;
      }
      
      if (!prevFile.delete() && prevFile.exists()) {
        throw new IOException("Could not delete " + prevFile);
      }
      
      close();

      if (!curFile.renameTo(prevFile)) {
        openCurFile();
        throw new IOException("Could not rename " + curFile + 
                              " to " + prevFile);
      }
      
      openCurFile();
      updateCurNumLines();
    }
    
    synchronized void close() {
      if (out != null) {
        out.close();
        out = null;
      }
    }
    
    /**
     * This is used to read the lines in order.
     * If the data is not read completely (i.e, untill hasNext() returns
     * false), it needs to be explicitly 
     */
    private class Reader implements Iterator<String>, Closeable {
      
      BufferedReader reader;
      File file;
      String line;
      boolean closed = false;
      
      private Reader(boolean skipPrevFile) throws IOException {
        synchronized (LogFileHandler.this) {
          numReaders++; 
        }
        reader = null;
        file = (skipPrevFile) ? curFile : prevFile;
        readNext();        
      }
      
      private boolean openFile() throws IOException {

        for(int i=0; i<2; i++) {
          if (reader != null || i > 0) {
            // move to next file
            file = (file == prevFile) ? curFile : null;
          }
          if (file == null) {
            return false;
          }
          if (file.exists()) {
            break;
          }
        }
        
        if (reader != null ) {
          reader.close();
          reader = null;
        }
        
        reader = new BufferedReader(new FileReader(file));
        return true;
      }
      
      // read next line if possible.
      private void readNext() throws IOException {
        line = null;
        try {
          if (reader != null && (line = reader.readLine()) != null) {
            return;
          }
          if (line == null) {
            // move to the next file.
            if (openFile()) {
              readNext();
            }
          }
        } finally {
          if (!hasNext()) {
            close();
          }
        }
      }
      
      public boolean hasNext() {
        return line != null;
      }

      public String next() {
        String curLine = line;
        try {
          readNext();
        } catch (IOException e) {
          LOG.info("Could not reade next line in LogHandler : " +
                   StringUtils.stringifyException(e));
        }
        return curLine;
      }

      public void remove() {
        throw new RuntimeException("remove() is not supported.");
      }

      public void close() throws IOException {
        if (!closed) {
          try {
            if (reader != null) {
              reader.close();
            }
          } finally {
            file = null;
            reader = null;
            closed = true;
            synchronized (LogFileHandler.this) {
              numReaders--;
              assert(numReaders >= 0);
            }
          }
        }
      }
    }    
  }
  
  public static class Servlet extends HttpServlet {
    
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response) throws IOException {
      
      response.setContentType("text/plain");
      
      DataBlockScanner blockScanner = (DataBlockScanner)  
          getServletContext().getAttribute("datanode.blockScanner");
      
      boolean summary = (request.getParameter("listblocks") == null);
      
      StringBuilder buffer = new StringBuilder(8*1024);
      if (blockScanner == null) {
        buffer.append("Periodic block scanner is not running. " +
                      "Please check the datanode log if this is unexpected.");
      } else if (blockScanner.isInitialized()) {
        blockScanner.printBlockReport(buffer, summary);
      } else {
        buffer.append("Periodic block scanner is not yet initialized. " +
                      "Please check back again after some time.");
      }
      response.getWriter().write(buffer.toString()); // extra copy!
    }
  }
}
