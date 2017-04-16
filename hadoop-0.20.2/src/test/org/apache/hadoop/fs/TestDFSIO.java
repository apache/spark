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

package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * Distributed i/o benchmark.
 * <p>
 * This test writes into or reads from a specified number of files.
 * File size is specified as a parameter to the test. 
 * Each file is accessed in a separate map task.
 * <p>
 * The reducer collects the following statistics:
 * <ul>
 * <li>number of tasks completed</li>
 * <li>number of bytes written/read</li>
 * <li>execution time</li>
 * <li>io rate</li>
 * <li>io rate squared</li>
 * </ul>
 *    
 * Finally, the following information is appended to a local file
 * <ul>
 * <li>read or write test</li>
 * <li>date and time the test finished</li>   
 * <li>number of files</li>
 * <li>total number of bytes processed</li>
 * <li>throughput in mb/sec (total number of bytes / sum of processing times)</li>
 * <li>average i/o rate in mb/sec per file</li>
 * <li>standard deviation of i/o rate </li>
 * </ul>
 */
public class TestDFSIO extends Configured implements Tool {
  // Constants
  private static final Log LOG = LogFactory.getLog(TestDFSIO.class);
  private static final int TEST_TYPE_READ = 0;
  private static final int TEST_TYPE_WRITE = 1;
  private static final int TEST_TYPE_CLEANUP = 2;
  private static final int TEST_TYPE_APPEND = 3;
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String BASE_FILE_NAME = "test_io_";
  private static final String DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log";
  
  private static final long MEGA = ByteMultiple.MB.value();
  private static final String USAGE =
                            "Usage: " + TestDFSIO.class.getSimpleName() +
                            " [genericOptions]" +
                            " -read | -write | -append | -clean [-nrFiles N]" +
                            " [-fileSize Size[B|KB|MB|GB|TB]]" +
                            " [-resFile resultFileName] [-bufferSize Bytes]" +
                            " [-rootDir]";

  private Configuration config;

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  static enum ByteMultiple {
    B(1L),
    KB(0x400L),
    MB(0x100000L),
    GB(0x40000000L),
    TB(0x10000000000L);

    private long multiplier;

    private ByteMultiple(long mult) {
      multiplier = mult;
    }

    long value() {
      return multiplier;
    }

    static ByteMultiple parseString(String sMultiple) {
      if(sMultiple == null || sMultiple.isEmpty()) // MB by default
        return MB;
      String sMU = sMultiple.toUpperCase();
      if(B.name().toUpperCase().endsWith(sMU))
        return B;
      if(KB.name().toUpperCase().endsWith(sMU))
        return KB;
      if(MB.name().toUpperCase().endsWith(sMU))
        return MB;
      if(GB.name().toUpperCase().endsWith(sMU))
        return GB;
      if(TB.name().toUpperCase().endsWith(sMU))
        return TB;
      throw new IllegalArgumentException("Unsupported ByteMultiple "+sMultiple);
    }
  }

  public TestDFSIO() {
    this.config = new Configuration();
  }

  private static String getBaseDir(Configuration conf) {
    return conf.get("test.build.data","/benchmarks/TestDFSIO");
  }
  private static Path getControlDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_control");
  }
  private static Path getWriteDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_write");
  }
  private static Path getReadDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_read");
  }
  private static Path getAppendDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_append");
  }
  private static Path getDataDir(Configuration conf) {
    return new Path(getBaseDir(conf), "io_data");
   }

  /**
   * Run the test with default parameters.
   * 
   * @throws Exception
   */
  @Test
  public void testIOs() throws Exception {
    TestDFSIO bench = new TestDFSIO();
    bench.testIOs(1, 4);
  }

  /**
   * Run the test with the specified parameters.
   * 
   * @param fileSize file size
   * @param nrFiles number of files
   * @throws IOException
   */

  public void testIOs(int fileSize, int nrFiles)
    throws IOException {
    config.setBoolean("dfs.support.broken.append", true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(config, 2, true, null);
      FileSystem fs = cluster.getFileSystem();

      createControlFile(fs, fileSize, nrFiles);
      long tStart = System.currentTimeMillis();
      writeTest(fs);
      long execTime = System.currentTimeMillis() - tStart;
      analyzeResult(fs, TEST_TYPE_WRITE, execTime, DEFAULT_RES_FILE_NAME);

      tStart = System.currentTimeMillis();
      readTest(fs);
      execTime = System.currentTimeMillis() - tStart;
      analyzeResult(fs, TEST_TYPE_READ, execTime, DEFAULT_RES_FILE_NAME);

      tStart = System.currentTimeMillis();
      appendTest(fs);
      execTime = System.currentTimeMillis() - tStart;
      analyzeResult(fs, TEST_TYPE_APPEND, execTime, DEFAULT_RES_FILE_NAME);

      cleanup(fs);
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }

  private void createControlFile(FileSystem fs,
                                  long fileSize, // in bytes
                                  int nrFiles
                                ) throws IOException {
    LOG.info("creating control file: "+fileSize+" bytes, "+nrFiles+" files");

    Path controlDir = getControlDir(config);
    fs.delete(controlDir, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(controlDir, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, config, controlFile,
                                           Text.class, LongWritable.class,
                                           CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(fileSize));
      } catch(Exception e) {
        throw new IOException(e.getLocalizedMessage());
      } finally {
    	if (writer != null)
          writer.close();
    	writer = null;
      }
    }
    LOG.info("created control files for: "+nrFiles+" files");
  }

  private static String getFileName(int fIdx) {
    return BASE_FILE_NAME + Integer.toString(fIdx);
  }
  
  /**
   * Write/Read mapper base class.
   * <p>
   * Collects the following statistics per task:
   * <ul>
   * <li>number of tasks completed</li>
   * <li>number of bytes written/read</li>
   * <li>execution time</li>
   * <li>i/o rate</li>
   * <li>i/o rate squared</li>
   * </ul>
   */
  private abstract static class IOStatMapper<T> extends IOMapperBase<T> {
    IOStatMapper() { 
    }
    
    void collectStats(OutputCollector<Text, Text> output, 
                      String name,
                      long execTime, 
                      Long objSize) throws IOException {
      long totalSize = objSize.longValue();
      float ioRateMbSec = (float)totalSize * 1000 / (execTime * MEGA);
      LOG.info("Number of bytes processed = " + totalSize);
      LOG.info("Exec time = " + execTime);
      LOG.info("IO rate = " + ioRateMbSec);
      
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"),
          new Text(String.valueOf(1)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"),
          new Text(String.valueOf(totalSize)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"),
          new Text(String.valueOf(execTime)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
          new Text(String.valueOf(ioRateMbSec*1000)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
          new Text(String.valueOf(ioRateMbSec*ioRateMbSec*1000)));
    }
  }

  /**
   * Write mapper class.
   */
  public static class WriteMapper extends IOStatMapper<Long> {

    public WriteMapper() { 
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    @Override
    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize // in bytes
                     ) throws IOException {
      // create file
      OutputStream out;
      out = fs.create(new Path(getDataDir(getConf()), name), true, bufferSize);
      
      try {
        // write to the file
        long nrRemaining;
        for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
          int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining; 
          out.write(buffer, 0, curSize);
          reporter.setStatus("writing " + name + "@" + 
                             (totalSize - nrRemaining) + "/" + totalSize 
                             + " ::host = " + hostName);
        }
      } finally {
        out.close();
      }
      return Long.valueOf(totalSize);
    }
  }

  private void writeTest(FileSystem fs) throws IOException {
    Path writeDir = getWriteDir(config);
    fs.delete(getDataDir(config), true);
    fs.delete(writeDir, true);
    
    runIOTest(WriteMapper.class, writeDir);
  }
  
  @SuppressWarnings("deprecation")
  private void runIOTest(
          Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass, 
          Path outputDir) throws IOException {
    JobConf job = new JobConf(config, TestDFSIO.class);

    FileInputFormat.setInputPaths(job, getControlDir(config));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  /**
   * Append mapper class.
   */
  public static class AppendMapper extends IOStatMapper<Long> {

    public AppendMapper() {
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    public Long doIO(Reporter reporter,
                       String name,
                       long totalSize // in bytes
                     ) throws IOException {
      // create file
      OutputStream out;
      out = fs.append(new Path(getDataDir(getConf()), name), bufferSize);

      try {
        // write to the file
        long nrRemaining;
        for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
          int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining;
          out.write(buffer, 0, curSize);
          reporter.setStatus("writing " + name + "@" +
                             (totalSize - nrRemaining) + "/" + totalSize
                             + " ::host = " + hostName);
        }
      } finally {
        out.close();
      }
      return Long.valueOf(totalSize);
    }
  }

  private void appendTest(FileSystem fs) throws IOException {
    Path appendDir = getAppendDir(config);
    fs.delete(appendDir, true);
    runIOTest(AppendMapper.class, appendDir);
  }

  /**
   * Read mapper class.
   */
  public static class ReadMapper extends IOStatMapper<Long> {

    public ReadMapper() { 
    }

    public Long doIO(Reporter reporter, 
                       String name, 
		       long totalSize // in bytes
                       ) throws IOException {

      DataInputStream in = fs.open(new Path(getDataDir(getConf()), name));
      long actualSize = 0;
      try {
        while (actualSize < totalSize) {
          int curSize = in.read(buffer, 0, bufferSize);
          if (curSize < 0) break;
          actualSize += curSize;
          reporter.setStatus("reading " + name + "@" + 
                             actualSize + "/" + totalSize 
                             + " ::host = " + hostName);
        }
      } finally {
        in.close();
      }
      return actualSize;
    }
  }

  private void readTest(FileSystem fs) throws IOException {
    Path readDir = getReadDir(config);
    fs.delete(readDir, true);
    runIOTest(ReadMapper.class, readDir);
  }

  private void sequentialTest(FileSystem fs,
                              int testType,
                              long fileSize, // in bytes
                              int nrFiles
                              ) throws Exception {
    IOStatMapper<Long> ioer = null;
    if (testType == TEST_TYPE_READ)
      ioer = new ReadMapper();
    else if (testType == TEST_TYPE_WRITE)
      ioer = new WriteMapper();
    else if (testType == TEST_TYPE_APPEND)
      ioer = new AppendMapper();
    else
      return;
    for(int i=0; i < nrFiles; i++)
      ioer.doIO(Reporter.NULL,
                BASE_FILE_NAME+Integer.toString(i), 
                MEGA*fileSize);
  }

  public static void main(String[] args) throws Exception{
    TestDFSIO bench = new TestDFSIO();
    int res = -1;
    try {
      res = ToolRunner.run(bench, args);
    } catch(Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      res = -2;
    }
    if(res == -1)
      System.err.print(USAGE);
    System.exit(res);
  }
  
  private void analyzeResult(FileSystem fs,
                             int testType,
                             long execTime,
                             String resFileName
                            ) throws IOException {
    Path reduceFile;
    if (testType == TEST_TYPE_WRITE)
      reduceFile = new Path(getWriteDir(config), "part-00000");
    else if (testType == TEST_TYPE_APPEND)
      reduceFile = new Path(getAppendDir(config), "part-00000");
    else
      reduceFile = new Path(getReadDir(config), "part-00000");
    long tasks = 0;
    long size = 0;
    long time = 0;
    float rate = 0;
    float sqrate = 0;
    DataInputStream in = null;
    BufferedReader lines = null;
    try {
      in = new DataInputStream(fs.open(reduceFile));
      lines = new BufferedReader(new InputStreamReader(in));
      String line;
      while((line = lines.readLine()) != null) {
        StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
        String attr = tokens.nextToken(); 
        if (attr.endsWith(":tasks"))
          tasks = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":size"))
          size = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":time"))
          time = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":rate"))
          rate = Float.parseFloat(tokens.nextToken());
        else if (attr.endsWith(":sqrate"))
          sqrate = Float.parseFloat(tokens.nextToken());
      }
    } finally {
      if(in != null) in.close();
      if(lines != null) lines.close();
    }
    
    double med = rate / 1000 / tasks;
    double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med*med));
    String resultLines[] = {
      "----- TestDFSIO ----- : " + ((testType == TEST_TYPE_WRITE) ? "write" :
                                    (testType == TEST_TYPE_READ) ? "read" : 
                                    (testType == TEST_TYPE_APPEND) ? "append" :
                                    "unknown"),
      "           Date & time: " + new Date(System.currentTimeMillis()),
      "       Number of files: " + tasks,
      "Total MBytes processed: " + toMB(size),
      "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
      "Average IO rate mb/sec: " + med,
      " IO rate std deviation: " + stdDev,
      "    Test exec time sec: " + (float)execTime / 1000,
      "" };

    PrintStream res = null;
    try {
      res = new PrintStream(new FileOutputStream(new File(resFileName), true)); 
      for(int i = 0; i < resultLines.length; i++) {
        LOG.info(resultLines[i]);
        res.println(resultLines[i]);
      }
    } finally {
      if(res != null) res.close();
    }
  }

  private void cleanup(FileSystem fs)
  throws IOException {
    LOG.info("Cleaning up test files");
    fs.delete(new Path(getBaseDir(config)), true);
  }

  @Override
  public int run(String[] args) throws Exception {
    int testType = TEST_TYPE_READ;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    long fileSize = 1*MEGA;
    int nrFiles = 1;
    String resFileName = DEFAULT_RES_FILE_NAME;
    boolean isSequential = false;
    String version = TestDFSIO.class.getSimpleName() + ".0.0.6";
    
    LOG.info(version);
    if (args.length == 0) {
      System.err.println("Missing arguments.");
      return -1;
    }
    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].startsWith("-read")) {
        testType = TEST_TYPE_READ;
      } else if (args[i].equals("-write")) {
        testType = TEST_TYPE_WRITE;
      } else if (args[i].equals("-append")) {
        testType = TEST_TYPE_APPEND;
      } else if (args[i].equals("-clean")) {
        testType = TEST_TYPE_CLEANUP;
      } else if (args[i].startsWith("-seq")) {
        isSequential = true;
      } else if (args[i].equals("-nrFiles")) {
        nrFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fileSize")) {
        fileSize = parseSize(args[++i]);
      } else if (args[i].equals("-bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-resFile")) {
        resFileName = args[++i];
      } else {
        System.err.println("Illegal argument: " + args[i]);
        return -1;
       }
    }

    LOG.info("nrFiles = " + nrFiles);
    LOG.info("fileSize (MB) = " + toMB(fileSize));
    LOG.info("bufferSize = " + bufferSize);
    LOG.info("baseDir = " + getBaseDir(config));

    config.setInt("test.io.file.buffer.size", bufferSize);
    config.setBoolean("dfs.support.broken.append", true);
    FileSystem fs = FileSystem.get(config);

    if (isSequential) {
      long tStart = System.currentTimeMillis();
      sequentialTest(fs, testType, fileSize, nrFiles);
      long execTime = System.currentTimeMillis() - tStart;
      String resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
      LOG.info(resultLine);
      return 0;
    }

    if (testType == TEST_TYPE_CLEANUP) {
      cleanup(fs);
      return 0;
    }
    createControlFile(fs, fileSize, nrFiles);
    long tStart = System.currentTimeMillis();
    if (testType == TEST_TYPE_WRITE)
      writeTest(fs);
    if (testType == TEST_TYPE_READ)
      readTest(fs);
    if (testType == TEST_TYPE_APPEND)
      appendTest(fs);

    long execTime = System.currentTimeMillis() - tStart;

    analyzeResult(fs, testType, execTime, resFileName);
    return 0;
  }

  /**
   * Returns size in bytes.
   *
   * @param arg = {d}[B|KB|MB|GB|TB]
   * @return
   */
  static long parseSize(String arg) {
    String[] args = arg.split("\\D", 2);  // get digits
    assert args.length <= 2;
    long fileSize = Long.parseLong(args[0]);
    String bytesMult = arg.substring(args[0].length()); // get byte multiple
    return fileSize * ByteMultiple.parseString(bytesMult).value();
  }

  static float toMB(long bytes) {
    return ((float)bytes)/MEGA;
  }

  @Override // Configurable
  public Configuration getConf() {
    return this.config;
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    this.config = conf;
  }
}
