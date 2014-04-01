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

package org.apache.hadoop.mapred;

import static org.junit.Assert.fail;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.UserLogEvent;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.UserLogManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** 
 * Utilities used in unit test.
 *  
 */
public class UtilsForTests {

  static final Log LOG = LogFactory.getLog(UtilsForTests.class);
  final static long KB = 1024L * 1;
  final static long MB = 1024L * KB;
  final static long GB = 1024L * MB;
  final static long TB = 1024L * GB;
  final static long PB = 1024L * TB;
  final static Object waitLock = new Object();

  static DecimalFormat dfm = new DecimalFormat("####.000");
  static DecimalFormat ifm = new DecimalFormat("###,###,###,###,###");

  public static String dfmt(double d) {
    return dfm.format(d);
  }

  public static String ifmt(double d) {
    return ifm.format(d);
  }

  public static String formatBytes(long numBytes) {
    StringBuffer buf = new StringBuffer();
    boolean bDetails = true;
    double num = numBytes;

    if (numBytes < KB) {
      buf.append(numBytes + " B");
      bDetails = false;
    } else if (numBytes < MB) {
      buf.append(dfmt(num / KB) + " KB");
    } else if (numBytes < GB) {
      buf.append(dfmt(num / MB) + " MB");
    } else if (numBytes < TB) {
      buf.append(dfmt(num / GB) + " GB");
    } else if (numBytes < PB) {
      buf.append(dfmt(num / TB) + " TB");
    } else {
      buf.append(dfmt(num / PB) + " PB");
    }
    if (bDetails) {
      buf.append(" (" + ifmt(numBytes) + " bytes)");
    }
    return buf.toString();
  }

  public static String formatBytes2(long numBytes) {
    StringBuffer buf = new StringBuffer();
    long u = 0;
    if (numBytes >= TB) {
      u = numBytes / TB;
      numBytes -= u * TB;
      buf.append(u + " TB ");
    }
    if (numBytes >= GB) {
      u = numBytes / GB;
      numBytes -= u * GB;
      buf.append(u + " GB ");
    }
    if (numBytes >= MB) {
      u = numBytes / MB;
      numBytes -= u * MB;
      buf.append(u + " MB ");
    }
    if (numBytes >= KB) {
      u = numBytes / KB;
      numBytes -= u * KB;
      buf.append(u + " KB ");
    }
    buf.append(u + " B"); //even if zero
    return buf.toString();
  }

  static final String regexpSpecials = "[]()?*+|.!^-\\~@";

  public static String regexpEscape(String plain) {
    StringBuffer buf = new StringBuffer();
    char[] ch = plain.toCharArray();
    int csup = ch.length;
    for (int c = 0; c < csup; c++) {
      if (regexpSpecials.indexOf(ch[c]) != -1) {
        buf.append("\\");
      }
      buf.append(ch[c]);
    }
    return buf.toString();
  }

  public static String safeGetCanonicalPath(File f) {
    try {
      String s = f.getCanonicalPath();
      return (s == null) ? f.toString() : s;
    } catch (IOException io) {
      return f.toString();
    }
  }

  public static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  static String slurpHadoop(Path p, FileSystem fs) throws IOException {
    int len = (int) fs.getLength(p);
    byte[] buf = new byte[len];
    InputStream in = fs.open(p);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  public static String rjustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = getSpace(width - s.length()) + s;
    }
    return s;
  }

  public static String ljustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = s + getSpace(width - s.length());
    }
    return s;
  }

  static char[] space;
  static {
    space = new char[300];
    Arrays.fill(space, '\u0020');
  }

  public static String getSpace(int len) {
    if (len > space.length) {
      space = new char[Math.max(len, 2 * space.length)];
      Arrays.fill(space, '\u0020');
    }
    return new String(space, 0, len);
  }
  
  /**
   * Gets job status from the jobtracker given the jobclient and the job id
   */
  static JobStatus getJobStatus(JobClient jc, JobID id) throws IOException {
    JobStatus[] statuses = jc.getAllJobs();
    for (JobStatus jobStatus : statuses) {
      if (jobStatus.getJobID().equals(id)) {
        return jobStatus;
      }
    }
    return null;
  }
  
  /**
   * A utility that waits for specified amount of time
   */
  public static void waitFor(long duration) {
    try {
      synchronized (waitLock) {
        waitLock.wait(duration);
      }
    } catch (InterruptedException ie) {}
  }
  
  /**
   * Wait for the jobtracker to be RUNNING.
   */
  static void waitForJobTracker(JobClient jobClient) {
    while (true) {
      try {
        ClusterStatus status = jobClient.getClusterStatus();
        while (status.getJobTrackerState() != JobTracker.State.RUNNING) {
          waitFor(100);
          status = jobClient.getClusterStatus();
        }
        break; // means that the jt is ready
      } catch (IOException ioe) {}
    }
  }
  
  /**
   * Waits until all the jobs at the jobtracker complete.
   */
  static void waitTillDone(JobClient jobClient) throws IOException {
    // Wait for the last job to complete
    while (true) {
      boolean shouldWait = false;
      for (JobStatus jobStatuses : jobClient.getAllJobs()) {
        if (jobStatuses.getRunState() != JobStatus.SUCCEEDED
            && jobStatuses.getRunState() != JobStatus.FAILED
            && jobStatuses.getRunState() != JobStatus.KILLED) {
          shouldWait = true;
          break;
        }
      }
      if (shouldWait) {
        waitFor(1000);
      } else {
        break;
      }
    }
  }
  
  /**
   * Configure a waiting job
   */
  static void configureWaitingJobConf(JobConf jobConf, Path inDir,
                                      Path outputPath, int numMaps, int numRed,
                                      String jobName, String mapSignalFilename,
                                      String redSignalFilename)
  throws IOException {
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    jobConf.setMapperClass(UtilsForTests.HalfWaitingMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    jobConf.setInputFormat(RandomInputFormat.class);
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(numRed);
    jobConf.setJar("build/test/testjar/testjob.jar");
    jobConf.set(getTaskSignalParameter(true), mapSignalFilename);
    jobConf.set(getTaskSignalParameter(false), redSignalFilename);
  }

  /**
   * Commonly used map and reduce classes 
   */
  
  /** 
   * Map is a Mapper that just waits for a file to be created on the dfs. The 
   * file creation is a signal to the mappers and hence acts as a waiting job. 
   */

  static class WaitingMapper 
  extends MapReduceBase 
  implements Mapper<WritableComparable, Writable, 
                    WritableComparable, Writable> {

    FileSystem fs = null;
    Path signal;
    int id = 0;
    int totalMaps = 0;

    /**
     * Checks if the map task needs to wait. By default all the maps will wait.
     * This method needs to be overridden to make a custom waiting mapper. 
     */
    public boolean shouldWait(int id) {
      return true;
    }
    
    /**
     * Returns a signal file on which the map task should wait. By default all 
     * the maps wait on a single file passed as test.mapred.map.waiting.target.
     * This method needs to be overridden to make a custom waiting mapper
     */
    public Path getSignalFile(int id) {
      return signal;
    }
    
    /** The waiting function.  The map exits once it gets a signal. Here the 
     * signal is the file existence. 
     */
    public void map(WritableComparable key, Writable val, 
                    OutputCollector<WritableComparable, Writable> output,
                    Reporter reporter)
    throws IOException {
      if (shouldWait(id)) {
        if (fs != null) {
          while (!fs.exists(getSignalFile(id))) {
            try {
              reporter.progress();
              synchronized (this) {
                this.wait(1000); // wait for 1 sec
              }
            } catch (InterruptedException ie) {
              System.out.println("Interrupted while the map was waiting for "
                                 + " the signal.");
              break;
            }
          }
        } else {
          throw new IOException("Could not get the DFS!!");
        }
      }
    }

    public void configure(JobConf conf) {
      try {
        String taskId = conf.get("mapred.task.id");
        id = Integer.parseInt(taskId.split("_")[4]);
        totalMaps = Integer.parseInt(conf.get("mapred.map.tasks"));
        fs = FileSystem.get(conf);
        signal = new Path(conf.get(getTaskSignalParameter(true)));
      } catch (IOException ioe) {
        System.out.println("Got an exception while obtaining the filesystem");
      }
    }
  }
  
  /** Only the later half of the maps wait for the signal while the rest 
   * complete immediately.
   */
  static class HalfWaitingMapper extends WaitingMapper {
    @Override
    public boolean shouldWait(int id) {
      return id >= (totalMaps / 2);
    }
  }
  
  /** 
   * Reduce that just waits for a file to be created on the dfs. The 
   * file creation is a signal to the reduce.
   */

  static class WaitingReducer extends MapReduceBase 
  implements Reducer<WritableComparable, Writable, 
                     WritableComparable, Writable> {

    FileSystem fs = null;
    Path signal;
    
    /** The waiting function.  The reduce exits once it gets a signal. Here the
     * signal is the file existence. 
     */
    public void reduce(WritableComparable key, Iterator<Writable> val, 
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter)
    throws IOException {
      if (fs != null) {
        while (!fs.exists(signal)) {
          try {
            reporter.progress();
            synchronized (this) {
              this.wait(1000); // wait for 1 sec
            }
          } catch (InterruptedException ie) {
            System.out.println("Interrupted while the map was waiting for the"
                               + " signal.");
            break;
          }
        }
      } else {
        throw new IOException("Could not get the DFS!!");
      }
    }

    public void configure(JobConf conf) {
      try {
        fs = FileSystem.get(conf);
        signal = new Path(conf.get(getTaskSignalParameter(false)));
      } catch (IOException ioe) {
        System.out.println("Got an exception while obtaining the filesystem");
      }
    }
  }

  /**
   * Cleans up files/dirs inline. CleanupQueue deletes in a separate thread
   * asynchronously.
   */
  public static class InlineCleanupQueue extends CleanupQueue {
    List<Path> stalePaths = new ArrayList<Path>();

    public InlineCleanupQueue() {
      // do nothing
    }

    @Override
    public void addToQueue(PathDeletionContext... contexts) {
      // delete paths in-line
      for (PathDeletionContext context : contexts) {
        Exception exc = null;
        try {
          if (!deletePath(context)) {
            LOG.warn("Stale path " + context.fullPath);
            stalePaths.add(context.fullPath);
          }
        } catch (IOException e) {
          exc = e;
        } catch (InterruptedException ie) {
          exc = ie;
        }
        if (exc != null) {
          LOG.warn("Caught exception while deleting path "
              + context.fullPath);
          LOG.info(StringUtils.stringifyException(exc));
          stalePaths.add(context.fullPath);
        }
      }
    }
    static boolean deletePath(PathDeletionContext context) 
    throws IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to delete " + context.fullPath);
      }
//      FileSystem fs = context.fullPath.getFileSystem(context.conf);
//      if (fs.exists(context.fullPath)) {
//        return fs.delete(context.fullPath, true);
//      }
      context.deletePath();
      return true;
    }
  }

  static String getTaskSignalParameter(boolean isMap) {
    return isMap 
           ? "test.mapred.map.waiting.target" 
           : "test.mapred.reduce.waiting.target";
  }
  
  /**
   * Signal the maps/reduces to start.
   */
  static void signalTasks(MiniDFSCluster dfs, FileSystem fileSys, 
                          String mapSignalFile, 
                          String reduceSignalFile, int replication) 
  throws IOException {
    writeFile(dfs.getNameNode(), fileSys.getConf(), new Path(mapSignalFile), 
              (short)replication);
    writeFile(dfs.getNameNode(), fileSys.getConf(), new Path(reduceSignalFile), 
              (short)replication);
  }
  
  /**
   * Signal the maps/reduces to start.
   */
  static void signalTasks(MiniDFSCluster dfs, FileSystem fileSys, 
                          boolean isMap, String mapSignalFile, 
                          String reduceSignalFile)
  throws IOException {
    //  signal the maps to complete
    writeFile(dfs.getNameNode(), fileSys.getConf(),
              isMap 
              ? new Path(mapSignalFile)
              : new Path(reduceSignalFile), (short)1);
  }
  
  static String getSignalFile(Path dir) {
    return (new Path(dir, "signal")).toString();
  }
  
  static String getMapSignalFile(Path dir) {
    return (new Path(dir, "map-signal")).toString();
  }

  static String getReduceSignalFile(Path dir) {
    return (new Path(dir, "reduce-signal")).toString();
  }
  
  static void writeFile(NameNode namenode, Configuration conf, Path name, 
      short replication) throws IOException {
    FileSystem fileSys = FileSystem.get(conf);
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(fileSys, conf, name, 
                                BytesWritable.class, BytesWritable.class,
                                CompressionType.NONE);
    writer.append(new BytesWritable(), new BytesWritable());
    writer.close();
    fileSys.setReplication(name, replication);
    DFSTestUtil.waitReplication(fileSys, name, replication);
  }
  
  // Input formats
  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map. Using {@link RandomWriter} code. 
   */
  public static class RandomInputFormat implements InputFormat<Text, Text> {
    
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      InputSplit[] result = new InputSplit[numSplits];
      Path outDir = FileOutputFormat.getOutputPath(job);
      for(int i=0; i < result.length; ++i) {
        result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i),
                                  0, 1, (String[])null);
      }
      return result;
    }

    static class RandomRecordReader implements RecordReader<Text, Text> {
      Path name;
      public RandomRecordReader(Path p) {
        name = p;
      }
      public boolean next(Text key, Text value) {
        if (name != null) {
          key.set(name.getName());
          name = null;
          return true;
        }
        return false;
      }
      public Text createKey() {
        return new Text();
      }
      public Text createValue() {
        return new Text();
      }
      public long getPos() {
        return 0;
      }
      public void close() {}
      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                    JobConf job, 
                                                    Reporter reporter) 
    throws IOException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  // Start a job and return its RunningJob object
  static RunningJob runJob(JobConf conf, Path inDir, Path outDir)
                    throws IOException {
    return runJob(conf, inDir, outDir, conf.getNumMapTasks(), conf.getNumReduceTasks());
  }

  // Start a job and return its RunningJob object
  public static RunningJob runJob(JobConf conf, Path inDir, Path outDir, 
                                  int numMaps, int numReds) throws IOException {

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox\n" + "has many silly\n"
        + "red fox sox\n";
    for (int i = 0; i < numMaps; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
    }    

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);

    return job;
  }

  // Run a job that will be succeeded and wait until it completes
  static RunningJob runJobSucceed(JobConf conf, Path inDir, Path outDir)
         throws IOException {
    conf.setJobName("test-job-succeed");
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    
    RunningJob job = UtilsForTests.runJob(conf, inDir, outDir);
    while (!job.isComplete()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    return job;
  }

  // Run a job that will be failed and wait until it completes
  static RunningJob runJobFail(JobConf conf, Path inDir, Path outDir)
         throws IOException {
    conf.setJobName("test-job-fail");
    conf.setMapperClass(FailMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    
    RunningJob job = UtilsForTests.runJob(conf, inDir, outDir);
    while (!job.isComplete()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    return job;
  }

  // Run a job that will be killed and wait until it completes
  static RunningJob runJobKill(JobConf conf,  Path inDir, Path outDir)
         throws IOException {

    conf.setJobName("test-job-kill");
    conf.setMapperClass(KillMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    
    RunningJob job = UtilsForTests.runJob(conf, inDir, outDir);
    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    job.killJob();
    while (job.cleanupProgress() == 0.0f) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        break;
      }
    }

    return job;
  }

  static class FakeClock extends Clock {
    long time = 0;
    
    public void advance(long millis) {
      time += millis;
    }
     
      @Override
      long getTime() {
        return time;
      }
    }
  
  // Mapper that fails
  static class FailMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      throw new RuntimeException("failing map");
    }
  }

  // Mapper that sleeps for a long time.
  // Used for running a job that will be killed
  static class KillMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      try {
        Thread.sleep(1000000);
      } catch (InterruptedException e) {
        // Respond to interrupts
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This is an in-line {@link UserLogManager} to do all the actions in-line. 
   */
  static class InLineUserLogManager extends UserLogManager {
    public InLineUserLogManager(Configuration conf) throws IOException {
      super(conf);
      getUserLogCleaner().setCleanupQueue(new InlineCleanupQueue());
    }

    // do the action in-line
    public void addLogEvent(UserLogEvent event) {
      try {
        super.addLogEvent(event);
        super.monitor();
      } catch (Exception e) {
        fail("failed to process action " + event.getEventType());
      }
    }
  }

  static void setUpConfigFile(Properties confProps, File configFile)
    throws IOException {
    Configuration config = new Configuration(false);
    FileOutputStream fos = new FileOutputStream(configFile);

    for (Enumeration<?> e = confProps.propertyNames(); e.hasMoreElements();) {
      String key = (String) e.nextElement();
      config.set(key, confProps.getProperty(key));
    }

    config.writeXml(fos);
    fos.close();
  }

  
  /**
   * Get PID from a pid-file.
   * 
   * @param pidFileName
   *          Name of the pid-file.
   * @return the PID string read from the pid-file. Returns null if the
   *         pidFileName points to a non-existing file or if read fails from the
   *         file.
   */
  public static String getPidFromPidFile(String pidFileName) {
    BufferedReader pidFile = null;
    FileReader fReader = null;
    String pid = null;

    try {
      fReader = new FileReader(pidFileName);
      pidFile = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.debug("PidFile doesn't exist : " + pidFileName);
      return pid;
    }

    try {
      pid = pidFile.readLine();
    } catch (IOException i) {
      LOG.error("Failed to read from " + pidFileName);
    } finally {
      try {
        if (fReader != null) {
          fReader.close();
        }
        try {
          if (pidFile != null) {
            pidFile.close();
          }
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + pidFile);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
    return pid;
  }

  static JobTracker getJobTracker() {
    JobConf conf = new JobConf();
    conf.set("mapred.job.tracker", "localhost:0");
    conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
    JobTracker jt;
    try {
      jt = new JobTracker(conf);
      return jt;
    } catch (Exception e) {
      throw new RuntimeException("Could not start jt", e);
    }
  }

  /**
   * This creates a file in the dfs
   * @param dfs FileSystem Local File System where file needs to be picked
   * @param URIPATH Path dfs path where file needs to be copied
   * @param permission FsPermission File permission
   * @return returns the DataOutputStream
   */
  public static DataOutputStream
      createTmpFileDFS(FileSystem dfs, Path URIPATH,
      FsPermission permission, String input) throws Exception {
    //Creating the path with the file
    DataOutputStream file =
      FileSystem.create(dfs, URIPATH, permission);
    file.writeBytes(input);
    file.close();
    return file;
  }

  /**
   * This formats the long tasktracker name to just the FQDN
   * @param taskTrackerLong String The long format of the tasktracker string
   * @return String The FQDN of the tasktracker
   * @throws Exception
   */
  public static String getFQDNofTT (String taskTrackerLong) throws Exception {
    //Getting the exact FQDN of the tasktracker from the tasktracker string.
    String[] firstSplit = taskTrackerLong.split("_");
    String tmpOutput = firstSplit[1];
    String[] secondSplit = tmpOutput.split(":");
    String tmpTaskTracker = secondSplit[0];
    return tmpTaskTracker;
  }

  /**
   * Setup the given task controller
   * @param taskController to setup
   * @param localAlloc local allocator to use
   * @param dirs to use for local storage
   * @throws IOException
   */
  public static void setupTC(TaskController taskController, 
      LocalDirAllocator localAlloc, String[] dirs) throws IOException {
    // Hide LocalStorage from callers
    taskController.setup(localAlloc, new LocalStorage(dirs));
  }
}

