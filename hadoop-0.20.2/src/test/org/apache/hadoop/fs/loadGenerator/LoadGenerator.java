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

package org.apache.hadoop.fs.loadGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** The load generator is a tool for testing NameNode behavior under
 * different client loads.
 * It allows the user to generate different mixes of read, write,
 * and list requests by specifying the probabilities of read and
 * write. The user controls the intensity of the load by
 * adjusting parameters for the number of worker threads and the delay
 * between operations. While load generators are running, the user
 * can profile and monitor the running of the NameNode. When a load
 * generator exits, it print some NameNode statistics like the average
 * execution time of each kind of operations and the NameNode
 * throughput.
 * 
 * After command line argument parsing and data initialization,
 * the load generator spawns the number of worker threads 
 * as specified by the user.
 * Each thread sends a stream of requests to the NameNode.
 * For each iteration, it first decides if it is going to read a file,
 * create a file, or listing a directory following the read and write 
 * probabilities specified by the user.
 * When reading, it randomly picks a file in the test space and reads
 * the entire file. When writing, it randomly picks a directory in the
 * test space and creates a file whose name consists of the current 
 * machine's host name and the thread id. The length of the file
 * follows Gaussian distribution with an average size of 2 blocks and
 * the standard deviation of 1 block. The new file is filled with 'a'.
 * Immediately after the file creation completes, the file is deleted
 * from the test space.
 * While listing, it randomly picks a directory in the test space and
 * list the directory content.
 * Between two consecutive operations, the thread pauses for a random
 * amount of time in the range of [0, maxDelayBetweenOps] 
 * if the specified max delay is not zero.
 * All threads are stopped when the specified elapsed time is passed.
 * Before exiting, the program prints the average execution for 
 * each kind of NameNode operations, and the number of requests
 * served by the NameNode.
 *
 * The synopsis of the command is
 * java LoadGenerator
 *   -readProbability <read probability>: read probability [0, 1]
 *                                        with a default value of 0.3333. 
 *   -writeProbability <write probability>: write probability [0, 1]
 *                                         with a default value of 0.3333.
 *   -root <root>: test space with a default value of /testLoadSpace
 *   -maxDelayBetweenOps <maxDelayBetweenOpsInMillis>: 
 *      Max delay in the unit of milliseconds between two operations with a 
 *      default value of 0 indicating no delay.
 *   -numOfThreads <numOfThreads>: 
 *      number of threads to spawn with a default value of 200.
 *   -elapsedTime <elapsedTimeInSecs>: 
 *      the elapsed time of program with a default value of 0 
 *      indicating running forever
 *   -startTime <startTimeInMillis> : when the threads start to run.
 */
public class LoadGenerator extends Configured implements Tool {
  private volatile boolean shouldRun = true;
  private Path root = DataGenerator.DEFAULT_ROOT;
  private FileSystem fs;
  private int maxDelayBetweenOps = 0;
  private int numOfThreads = 200;
  private double readPr = 0.3333;
  private double writePr = 0.3333;
  private long elapsedTime = 0;
  private long startTime = System.currentTimeMillis()+10000;
  final static private int BLOCK_SIZE = 10;
  private ArrayList<String> files = new ArrayList<String>();  // a table of file names
  private ArrayList<String> dirs = new ArrayList<String>(); // a table of directory names
  private Random r = null;
  final private static String USAGE = "java LoadGenerator\n" +
  	"-readProbability <read probability>\n" +
    "-writeProbability <write probability>\n" +
    "-root <root>\n" +
    "-maxDelayBetweenOps <maxDelayBetweenOpsInMillis>\n" +
    "-numOfThreads <numOfThreads>\n" +
    "-elapsedTime <elapsedTimeInSecs>\n" +
    "-startTime <startTimeInMillis>";
  final private String hostname;
  
  /** Constructor */
  public LoadGenerator() throws IOException, UnknownHostException {
    InetAddress addr = InetAddress.getLocalHost();
    hostname = addr.getHostName();
  }

  private final static int OPEN = 0;
  private final static int LIST = 1;
  private final static int CREATE = 2;
  private final static int WRITE_CLOSE = 3;
  private final static int DELETE = 4;
  private final static int TOTAL_OP_TYPES =5;
  private long [] executionTime = new long[TOTAL_OP_TYPES];
  private long [] totalNumOfOps = new long[TOTAL_OP_TYPES];
  
  /** A thread sends a stream of requests to the NameNode.
   * At each iteration, it first decides if it is going to read a file,
   * create a file, or listing a directory following the read
   * and write probabilities.
   * When reading, it randomly picks a file in the test space and reads
   * the entire file. When writing, it randomly picks a directory in the
   * test space and creates a file whose name consists of the current 
   * machine's host name and the thread id. The length of the file
   * follows Gaussian distribution with an average size of 2 blocks and
   * the standard deviation of 1 block. The new file is filled with 'a'.
   * Immediately after the file creation completes, the file is deleted
   * from the test space.
   * While listing, it randomly picks a directory in the test space and
   * list the directory content.
   * Between two consecutive operations, the thread pauses for a random
   * amount of time in the range of [0, maxDelayBetweenOps] 
   * if the specified max delay is not zero.
   * A thread runs for the specified elapsed time if the time isn't zero.
   * Otherwise, it runs forever.
   */
  private class DFSClientThread extends Thread {
    private int id;
    private long [] executionTime = new long[TOTAL_OP_TYPES];
    private long [] totalNumOfOps = new long[TOTAL_OP_TYPES];
    private byte[] buffer = new byte[1024];
    
    private DFSClientThread(int id) {
      this.id = id;
    }
    
    /** Main loop
     * Each iteration decides what's the next operation and then pauses.
     */
    public void run() {
      try {
        while (shouldRun) {
          nextOp();
          delay();
        }
      } catch (Exception ioe) {
        System.err.println(ioe.getLocalizedMessage());
        ioe.printStackTrace();
      }
    }
    
    /** Let the thread pause for a random amount of time in the range of
     * [0, maxDelayBetweenOps] if the delay is not zero. Otherwise, no pause.
     */
    private void delay() throws InterruptedException {
      if (maxDelayBetweenOps>0) {
        int delay = r.nextInt(maxDelayBetweenOps);
        Thread.sleep(delay);
      }
    }
    
    /** Perform the next operation. 
     * 
     * Depending on the read and write probabilities, the next
     * operation could be either read, write, or list.
     */
    private void nextOp() throws IOException {
      double rn = r.nextDouble();
      if (rn < readPr) {
        read();
      } else if (rn < readPr+writePr) {
        write();
      } else {
        list();
      }
    }
    
    /** Read operation randomly picks a file in the test space and reads
     * the entire file */
    private void read() throws IOException {
      String fileName = files.get(r.nextInt(files.size()));
      long startTime = System.currentTimeMillis();
      InputStream in = fs.open(new Path(fileName));
      executionTime[OPEN] += (System.currentTimeMillis()-startTime);
      totalNumOfOps[OPEN]++;
      while (in.read(buffer) != -1) {}
      in.close();
    }
    
    /** The write operation randomly picks a directory in the
     * test space and creates a file whose name consists of the current 
     * machine's host name and the thread id. The length of the file
     * follows Gaussian distribution with an average size of 2 blocks and
     * the standard deviation of 1 block. The new file is filled with 'a'.
     * Immediately after the file creation completes, the file is deleted
     * from the test space.
     */
    private void write() throws IOException {
      String dirName = dirs.get(r.nextInt(dirs.size()));
      Path file = new Path(dirName, hostname+id);
      double fileSize = 0;
      while ((fileSize = r.nextGaussian()+2)<=0) {}
      genFile(file, (long)(fileSize*BLOCK_SIZE));
      long startTime = System.currentTimeMillis();
      fs.delete(file, true);
      executionTime[DELETE] += (System.currentTimeMillis()-startTime);
      totalNumOfOps[DELETE]++;
    }
    
    /** The list operation randomly picks a directory in the test space and
     * list the directory content.
     */
    private void list() throws IOException {
      String dirName = dirs.get(r.nextInt(dirs.size()));
      long startTime = System.currentTimeMillis();
      fs.listStatus(new Path(dirName));
      executionTime[LIST] += (System.currentTimeMillis()-startTime);
      totalNumOfOps[LIST]++;
    }
  }
  
  /** Main function:
   * It first initializes data by parsing the command line arguments.
   * It then starts the number of DFSClient threads as specified by
   * the user.
   * It stops all the threads when the specified elapsed time is passed.
   * Before exiting, it prints the average execution for 
   * each operation and operation throughput.
   */
  public int run(String[] args) throws Exception {
    int exitCode = init(args);
    if (exitCode != 0) {
      return exitCode;
    }
    
    barrier();
    
    DFSClientThread[] threads = new DFSClientThread[numOfThreads];
    for (int i=0; i<numOfThreads; i++) {
      threads[i] = new DFSClientThread(i); 
      threads[i].start();
    }
    if (elapsedTime>0) {
      Thread.sleep(elapsedTime*1000);
      shouldRun = false;
    } 
    for (DFSClientThread thread : threads) {
      thread.join();
      for (int i=0; i<TOTAL_OP_TYPES; i++) {
        executionTime[i] += thread.executionTime[i];
        totalNumOfOps[i] += thread.totalNumOfOps[i];
      }
    }
    long totalOps = 0;
    for (int i=0; i<TOTAL_OP_TYPES; i++) {
      totalOps += totalNumOfOps[i];
    }
    
    if (totalNumOfOps[OPEN] != 0) {
      System.out.println("Average open execution time: " + 
          (double)executionTime[OPEN]/totalNumOfOps[OPEN] + "ms");
    }
    if (totalNumOfOps[LIST] != 0) {
      System.out.println("Average list execution time: " + 
          (double)executionTime[LIST]/totalNumOfOps[LIST] + "ms");
    }
    if (totalNumOfOps[DELETE] != 0) {
      System.out.println("Average deletion execution time: " + 
          (double)executionTime[DELETE]/totalNumOfOps[DELETE] + "ms");
      System.out.println("Average create execution time: " + 
          (double)executionTime[CREATE]/totalNumOfOps[CREATE] + "ms");
      System.out.println("Average write_close execution time: " + 
          (double)executionTime[WRITE_CLOSE]/totalNumOfOps[WRITE_CLOSE] + "ms");
    }
    if (elapsedTime != 0) { 
      System.out.println("Average operations per second: " + 
          (double)totalOps/elapsedTime +"ops/s");
    }
    System.out.println();
    return exitCode;
  }

  /** Parse the command line arguments and initialize the data */
  private int init(String[] args) throws IOException {
    try {
      fs = FileSystem.get(getConf());
    } catch (IOException ioe) {
      System.err.println("Can not initialize the file system: " + 
          ioe.getLocalizedMessage());
      return -1;
    }
    int hostHashCode = hostname.hashCode();
    try {
      for (int i = 0; i < args.length; i++) { // parse command line
        if (args[i].equals("-readProbability")) {
          readPr = Double.parseDouble(args[++i]);
          if (readPr<0 || readPr>1) {
            System.err.println( 
                "The read probability must be [0, 1]: " + readPr);
            return -1;
          }
        } else if (args[i].equals("-writeProbability")) {
          writePr = Double.parseDouble(args[++i]);
          if (writePr<0 || writePr>1) {
            System.err.println( 
                "The write probability must be [0, 1]: " + writePr);
            return -1;
          }
        } else if (args[i].equals("-root")) {
          root = new Path(args[++i]);
        } else if (args[i].equals("-maxDelayBetweenOps")) {
          maxDelayBetweenOps = Integer.parseInt(args[++i]); // in milliseconds
        } else if (args[i].equals("-numOfThreads")) {
          numOfThreads = Integer.parseInt(args[++i]);
          if (numOfThreads <= 0) {
            System.err.println(
                "Number of threads must be positive: " + numOfThreads);
            return -1;
          }
        } else if (args[i].equals("-startTime")) {
          startTime = Long.parseLong(args[++i]);
        } else if (args[i].equals("-elapsedTime")) {
          elapsedTime = Long.parseLong(args[++i]);
        } else if (args[i].equals("-seed")) {
          r = new Random(Long.parseLong(args[++i])+hostHashCode);
        } else {
          System.err.println(USAGE);
          ToolRunner.printGenericCommandUsage(System.err);
          return -1;
        }
      }
    } catch (NumberFormatException e) {
      System.err.println("Illegal parameter: " + e.getLocalizedMessage());
      System.err.println(USAGE);
      return -1;
    }

    if (readPr+writePr <0 || readPr+writePr>1) {
      System.err.println(
          "The sum of read probability and write probability must be [0, 1]: " +
          readPr + " "+writePr);
      return -1;
    }
    
    if (r==null) {
      r = new Random(System.currentTimeMillis()+hostHashCode);
    }
    
    return initFileDirTables();
  }
  
  /** Create a table that contains all directories under root and
   * another table that contains all files under root.
   */
  private int initFileDirTables() {
    try {
      initFileDirTables(root);
    } catch (IOException e) {
      System.err.println(e.getLocalizedMessage());
      e.printStackTrace();
      return -1;
    }
    if (dirs.isEmpty()) {
      System.err.println("The test space " + root + " is empty");
      return -1;
    }
    if (files.isEmpty()) {
      System.err.println("The test space " + root + 
          " does not have any file");
      return -1;
    }
    return 0;
  }
  
  /** Create a table that contains all directories under the specified path and
   * another table that contains all files under the specified path and
   * whose name starts with "_file_".
   */
  private void initFileDirTables(Path path) throws IOException {
    FileStatus[] stats = fs.listStatus(path);
    if (stats != null) { 
      for (FileStatus stat : stats) {
        if (stat.isDir()) {
          dirs.add(stat.getPath().toString());
          initFileDirTables(stat.getPath());
        } else {
          Path filePath = stat.getPath();
          if (filePath.getName().startsWith(StructureGenerator.FILE_NAME_PREFIX)) {
            files.add(filePath.toString());
          }
        }
      }
    }
  }
  
  /** Returns when the current number of seconds from the epoch equals
   * the command line argument given by <code>-startTime</code>.
   * This allows multiple instances of this program, running on clock
   * synchronized nodes, to start at roughly the same time.
   */
  private void barrier() {
    long sleepTime;
    while ((sleepTime = startTime - System.currentTimeMillis()) > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
      }
    }
  }

  /** Create a file with a length of <code>fileSize</code>.
   * The file is filled with 'a'.
   */
  private void genFile(Path file, long fileSize) throws IOException {
    long startTime = System.currentTimeMillis();
    FSDataOutputStream out = fs.create(file, true, 
        getConf().getInt("io.file.buffer.size", 4096),
        (short)getConf().getInt("dfs.replication", 3),
        fs.getDefaultBlockSize());
    executionTime[CREATE] += (System.currentTimeMillis()-startTime);
    totalNumOfOps[CREATE]++;

    for (long i=0; i<fileSize; i++) {
      out.writeByte('a');
    }
    startTime = System.currentTimeMillis();
    out.close();
    executionTime[WRITE_CLOSE] += (System.currentTimeMillis()-startTime);
    totalNumOfOps[WRITE_CLOSE]++;
  }
  
  /** Main program
   * 
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
        new LoadGenerator(), args);
    System.exit(res);
  }

}
