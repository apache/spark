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

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program executes a specified operation that applies load to 
 * the NameNode. Possible operations include create/writing files,
 * opening/reading files, renaming files, and deleting files.
 * 
 * When run simultaneously on multiple nodes, this program functions 
 * as a stress-test and benchmark for namenode, especially when 
 * the number of bytes written to each file is small.
 * 
 * This version does not use the map reduce framework
 * 
 */
public class NNBenchWithoutMR extends Configured implements Tool {
  
  private static final Log LOG = LogFactory.getLog(
                                            "org.apache.hadoop.hdfs.NNBench");
  
  // variable initialzed from command line arguments
  private static long startTime = 0;
  private static int numFiles = 0;
  private static long bytesPerBlock = 1;
  private static long blocksPerFile = 0;
  private static long bytesPerFile = 1;
  private static Path baseDir = null;
    
  // variables initialized in main()
  private static FileSystem fileSys = null;
  private static Path taskDir = null;
  private static byte[] buffer;
  private static long maxExceptionsPerFile = 200;
    
  /**
   * Returns when the current number of seconds from the epoch equals
   * the command line argument given by <code>-startTime</code>.
   * This allows multiple instances of this program, running on clock
   * synchronized nodes, to start at roughly the same time.
   */

  static void barrier() {
    long sleepTime;
    while ((sleepTime = startTime - System.currentTimeMillis()) > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
        //This left empty on purpose
      }
    }
  }
    
  static private void handleException(String operation, Throwable e, 
                                      int singleFileExceptions) {
    LOG.warn("Exception while " + operation + ": " +
             StringUtils.stringifyException(e));
    if (singleFileExceptions >= maxExceptionsPerFile) {
      throw new RuntimeException(singleFileExceptions + 
        " exceptions for a single file exceeds threshold. Aborting");
    }
  }
  
  /**
   * Create and write to a given number of files.  Repeat each remote
   * operation until is suceeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int createWrite() {
    int totalExceptions = 0;
    FSDataOutputStream out = null;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // create file until is succeeds or max exceptions reached
        try {
          out = fileSys.create(
                  new Path(taskDir, "" + index), false, 512,
                  (short)1, bytesPerBlock);
          success = true;
        } catch (IOException ioe) { 
          success=false; 
          totalExceptions++;
          handleException("creating file #" + index, ioe,
                  ++singleFileExceptions);
        }
      } while (!success);
      long toBeWritten = bytesPerFile;
      while (toBeWritten > 0) {
        int nbytes = (int) Math.min(buffer.length, toBeWritten);
        toBeWritten -= nbytes;
        try { // only try once
          out.write(buffer, 0, nbytes);
        } catch (IOException ioe) {
          totalExceptions++;
          handleException("writing to file #" + index, ioe,
                  ++singleFileExceptions);
        }
      }
      do { // close file until is succeeds
        try {
          out.close();
          success = true;
        } catch (IOException ioe) {
          success=false; 
          totalExceptions++;
          handleException("closing file #" + index, ioe,
                  ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
    
  /**
   * Open and read a given number of files.
   *
   * @return the number of exceptions caught
   */
  static int openRead() {
    int totalExceptions = 0;
    FSDataInputStream in;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      try {
        in = fileSys.open(new Path(taskDir, "" + index), 512);
        long toBeRead = bytesPerFile;
        while (toBeRead > 0) {
          int nbytes = (int) Math.min(buffer.length, toBeRead);
          toBeRead -= nbytes;
          try { // only try once && we don't care about a number of bytes read
            in.read(buffer, 0, nbytes);
          } catch (IOException ioe) {
            totalExceptions++;
            handleException("reading from file #" + index, ioe,
                    ++singleFileExceptions);
          }
        }
        in.close();
      } catch (IOException ioe) { 
        totalExceptions++;
        handleException("opening file #" + index, ioe, ++singleFileExceptions);
      }
    }
    return totalExceptions;
  }
    
  /**
   * Rename a given number of files.  Repeat each remote
   * operation until is suceeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int rename() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // rename file until is succeeds
        try {
          // Possible result of this operation is at no interest to us for it
          // can return false only if the namesystem
          // could rename the path from the name
          // space (e.g. no Exception has been thrown)
          fileSys.rename(new Path(taskDir, "" + index),
              new Path(taskDir, "A" + index));
          success = true;
        } catch (IOException ioe) {
          success = false;
          totalExceptions++;
          handleException("creating file #" + index, ioe, ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
    
  /**
   * Delete a given number of files.  Repeat each remote
   * operation until is suceeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int delete() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // delete file until is succeeds
        try {
          // Possible result of this operation is at no interest to us for it
          // can return false only if namesystem
          // delete could remove the path from the name
          // space (e.g. no Exception has been thrown)
          fileSys.delete(new Path(taskDir, "A" + index), true);
          success = true;
        } catch (IOException ioe) {
          success=false; 
          totalExceptions++;
          handleException("creating file #" + index, ioe, ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
    
  /**
   * This launches a given namenode operation (<code>-operation</code>),
   * starting at a given time (<code>-startTime</code>).  The files used
   * by the openRead, rename, and delete operations are the same files
   * created by the createWrite operation.  Typically, the program
   * would be run four times, once for each operation in this order:
   * createWrite, openRead, rename, delete.
   *
   * <pre>
   * Usage: nnbench 
   *          -operation <one of createWrite, openRead, rename, or delete>
   *          -baseDir <base output/input DFS path>
   *          -startTime <time to start, given in seconds from the epoch>
   *          -numFiles <number of files to create, read, rename, or delete>
   *          -blocksPerFile <number of blocks to create per file>
   *         [-bytesPerBlock <number of bytes to write to each block, default is 1>]
   *         [-bytesPerChecksum <value for io.bytes.per.checksum>]
   * </pre>
   *
   * @param args is an array of the program command line arguments
   * @throws IOException indicates a problem with test startup
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new NNBenchWithoutMR(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    String version = "NameNodeBenchmark.0.3";
    System.out.println(version);
    int bytesPerChecksum = -1;
    
    String usage =
      "Usage: NNBenchWithoutMR " +
      "  -operation <one of createWrite, openRead, rename, or delete> " +
      "  -baseDir <base output/input DFS path> " +
      "  -startTime <time to start, given in seconds from the epoch> " +
      "  -numFiles <number of files to create> " +
      "  -blocksPerFile <number of blocks to create per file> " +
      "  [-bytesPerBlock <number of bytes to write to each block, default is 1>] " +
      "  [-bytesPerChecksum <value for io.bytes.per.checksum>]" +
      "Note: bytesPerBlock MUST be a multiple of bytesPerChecksum";
    
    String operation = null;
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-baseDir")) {
        baseDir = new Path(args[++i]);
      } else if (args[i].equals("-numFiles")) {
        numFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-blocksPerFile")) {
        blocksPerFile = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-bytesPerBlock")) {
        bytesPerBlock = Long.parseLong(args[++i]);
      } else if (args[i].equals("-bytesPerChecksum")) {
        bytesPerChecksum = Integer.parseInt(args[++i]);        
      } else if (args[i].equals("-startTime")) {
        startTime = Long.parseLong(args[++i]) * 1000;
      } else if (args[i].equals("-operation")) {
        operation = args[++i];
      } else {
        System.out.println(usage);
        return -1;
      }
    }
    bytesPerFile = bytesPerBlock * blocksPerFile;
    
    JobConf jobConf = new JobConf(new Configuration(), NNBench.class);

    if ( bytesPerChecksum < 0 ) { // if it is not set in cmdline
      bytesPerChecksum = jobConf.getInt("io.bytes.per.checksum", 512);
    }
    jobConf.set("io.bytes.per.checksum", Integer.toString(bytesPerChecksum));
    
    System.out.println("Inputs: ");
    System.out.println("   operation: " + operation);
    System.out.println("   baseDir: " + baseDir);
    System.out.println("   startTime: " + startTime);
    System.out.println("   numFiles: " + numFiles);
    System.out.println("   blocksPerFile: " + blocksPerFile);
    System.out.println("   bytesPerBlock: " + bytesPerBlock);
    System.out.println("   bytesPerChecksum: " + bytesPerChecksum);
    
    if (operation == null ||  // verify args
        baseDir == null ||
        numFiles < 1 ||
        blocksPerFile < 1 ||
        bytesPerBlock < 0 ||
        bytesPerBlock % bytesPerChecksum != 0)
      {
        System.err.println(usage);
        return -1;
      }
    
    fileSys = FileSystem.get(jobConf);
    String uniqueId = java.net.InetAddress.getLocalHost().getHostName();
    taskDir = new Path(baseDir, uniqueId);
    // initialize buffer used for writing/reading file
    buffer = new byte[(int) Math.min(bytesPerFile, 32768L)];
    
    Date execTime;
    Date endTime;
    long duration;
    int exceptions = 0;
    barrier(); // wait for coordinated start time
    execTime = new Date();
    System.out.println("Job started: " + startTime);
    if (operation.equals("createWrite")) {
      if (!fileSys.mkdirs(taskDir)) {
        throw new IOException("Mkdirs failed to create " + taskDir.toString());
      }
      exceptions = createWrite();
    } else if (operation.equals("openRead")) {
      exceptions = openRead();
    } else if (operation.equals("rename")) {
      exceptions = rename();
    } else if (operation.equals("delete")) {
      exceptions = delete();
    } else {
      System.err.println(usage);
      return -1;
    }
    endTime = new Date();
    System.out.println("Job ended: " + endTime);
    duration = (endTime.getTime() - execTime.getTime()) /1000;
    System.out.println("The " + operation + " job took " + duration + " seconds.");
    System.out.println("The job recorded " + exceptions + " exceptions.");

    return 0;
  }
}
