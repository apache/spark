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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.apache.hadoop.util.ResourceCalculatorPlugin.ProcResourceValues;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;

/** 
 * Base class for tasks.
 * 
 * This is NOT a public interface.
 */
abstract public class Task implements Writable, Configurable {
  private static final Log LOG =
    LogFactory.getLog(Task.class);

  // Counters used by Task subclasses
  public static enum Counter { 
    MAP_INPUT_RECORDS, 
    MAP_OUTPUT_RECORDS,
    MAP_SKIPPED_RECORDS,
    MAP_INPUT_BYTES, 
    MAP_OUTPUT_BYTES,
    COMBINE_INPUT_RECORDS,
    COMBINE_OUTPUT_RECORDS,
    REDUCE_INPUT_GROUPS,
    REDUCE_SHUFFLE_BYTES,
    REDUCE_INPUT_RECORDS,
    REDUCE_OUTPUT_RECORDS,
    REDUCE_SKIPPED_GROUPS,
    REDUCE_SKIPPED_RECORDS,
    SPILLED_RECORDS,
    SPLIT_RAW_BYTES,
    CPU_MILLISECONDS,
    PHYSICAL_MEMORY_BYTES,
    VIRTUAL_MEMORY_BYTES,
    COMMITTED_HEAP_BYTES
  }
  
  /**
   * Counters to measure the usage of the different file systems.
   * Always return the String array with two elements. First one is the name of  
   * BYTES_READ counter and second one is of the BYTES_WRITTEN counter.
   */
  protected static String[] getFileSystemCounterNames(String uriScheme) {
    String scheme = uriScheme.toUpperCase();
    return new String[]{scheme+"_BYTES_READ", scheme+"_BYTES_WRITTEN"};
  }
  
  /**
   * Name of the FileSystem counters' group
   */
  protected static final String FILESYSTEM_COUNTER_GROUP = "FileSystemCounters";

  ///////////////////////////////////////////////////////////
  // Helper methods to construct task-output paths
  ///////////////////////////////////////////////////////////
  
  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  static synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }

  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String user;                            // user running the job
  private TaskAttemptID taskId;                   // unique, includes job id
  private int partition;                          // id within job
  TaskStatus taskStatus;                          // current status of the task
  protected JobStatus.State jobRunStateForCleanup;
  protected boolean jobCleanup = false;
  protected boolean jobSetup = false;
  protected boolean taskCleanup = false;
  
  //skip ranges based on failed ranges from previous attempts
  private SortedRanges skipRanges = new SortedRanges();
  private boolean skipping = false;
  private boolean writeSkipRecs = true;
  
  //currently processing record start index
  private volatile long currentRecStartIndex; 
  private Iterator<Long> currentRecIndexIterator = 
    skipRanges.skipRangeIterator();
  
  private ResourceCalculatorPlugin resourceCalculator = null;
  private long initCpuCumulativeTime = 0;

  protected JobConf conf;
  protected MapOutputFile mapOutputFile = new MapOutputFile();
  protected LocalDirAllocator lDirAlloc;
  private final static int MAX_RETRIES = 10;
  protected JobContext jobContext;
  protected TaskAttemptContext taskContext;
  protected org.apache.hadoop.mapreduce.OutputFormat<?,?> outputFormat;
  protected org.apache.hadoop.mapreduce.OutputCommitter committer;
  protected final Counters.Counter spilledRecordsCounter;
  protected TaskUmbilicalProtocol umbilical;
  private int numSlotsRequired;
  protected SecretKey tokenSecret;
  protected JvmContext jvmContext;

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {
    taskStatus = TaskStatus.createTaskStatus(isMapTask());
    taskId = new TaskAttemptID();
    spilledRecordsCounter = counters.findCounter(Counter.SPILLED_RECORDS);
  }

  public Task(String jobFile, TaskAttemptID taskId, int partition, 
              int numSlotsRequired) {
    this.jobFile = jobFile;
    this.taskId = taskId;
     
    this.partition = partition;
    this.numSlotsRequired = numSlotsRequired;
    this.taskStatus = TaskStatus.createTaskStatus(isMapTask(), this.taskId, 
                                                  0.0f, numSlotsRequired,
                                                  TaskStatus.State.UNASSIGNED, 
                                                  "", "", "", 
                                                  isMapTask() ? 
                                                    TaskStatus.Phase.MAP : 
                                                    TaskStatus.Phase.SHUFFLE, 
                                                  counters);
    spilledRecordsCounter = counters.findCounter(Counter.SPILLED_RECORDS);
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public TaskAttemptID getTaskID() { return taskId; }
  public int getNumSlotsRequired() {
    return numSlotsRequired;
  }

  Counters getCounters() { return counters; }

  
  /**
   * Get the job name for this task.
   * @return the job name
   */
  public JobID getJobID() {
    return taskId.getJobID();
  }

  /**
   * Set the job token secret 
   * @param tokenSecret the secret
   */
  public void setJobTokenSecret(SecretKey tokenSecret) {
    this.tokenSecret = tokenSecret;
  }

  /**
   * Get the job token secret
   * @return the token secret
   */
  public SecretKey getJobTokenSecret() {
    return this.tokenSecret;
  }

  /**
   * Set the task JvmContext
   * @param jvmContext
   */
  public void setJvmContext(JvmContext jvmContext) {
    this.jvmContext = jvmContext;
  }
  
  /**
   * Gets the task JvmContext
   * @return the jvm context
   */
  public JvmContext getJvmContext() {
    return this.jvmContext;
  }
  
  /**
   * Get the index of this task within the job.
   * @return the integer part of the task id
   */
  public int getPartition() {
    return partition;
  }
  /**
   * Return current phase of the task. 
   * needs to be synchronized as communication thread sends the phase every second
   * @return the curent phase of the task
   */
  public synchronized TaskStatus.Phase getPhase(){
    return this.taskStatus.getPhase(); 
  }
  /**
   * Set current phase of the task. 
   * @param phase task phase 
   */
  protected synchronized void setPhase(TaskStatus.Phase phase){
    this.taskStatus.setPhase(phase); 
  }
  
  /**
   * Get whether to write skip records.
   */
  protected boolean toWriteSkipRecs() {
    return writeSkipRecs;
  }
      
  /**
   * Set whether to write skip records.
   */
  protected void setWriteSkipRecs(boolean writeSkipRecs) {
    this.writeSkipRecs = writeSkipRecs;
  }

  /**
   * Report a fatal error to the parent (task) tracker.
   */
  protected void reportFatalError(TaskAttemptID id, Throwable throwable, 
                                  String logMsg) {
    LOG.fatal(logMsg);
    Throwable tCause = throwable.getCause();
    String cause = tCause == null 
                   ? StringUtils.stringifyException(throwable)
                   : StringUtils.stringifyException(tCause);
    try {
      umbilical.fatalError(id, cause, jvmContext);
    } catch (IOException ioe) {
      LOG.fatal("Failed to contact the tasktracker", ioe);
      System.exit(-1);
    }
  }
  
  /**
   * Get skipRanges.
   */
  public SortedRanges getSkipRanges() {
    return skipRanges;
  }

  /**
   * Set skipRanges.
   */
  public void setSkipRanges(SortedRanges skipRanges) {
    this.skipRanges = skipRanges;
  }

  /**
   * Is Task in skipping mode.
   */
  public boolean isSkipping() {
    return skipping;
  }

  /**
   * Sets whether to run Task in skipping mode.
   * @param skipping
   */
  public void setSkipping(boolean skipping) {
    this.skipping = skipping;
  }

  /**
   * Return current state of the task. 
   * needs to be synchronized as communication thread 
   * sends the state every second
   * @return
   */
  synchronized TaskStatus.State getState(){
    return this.taskStatus.getRunState(); 
  }
  /**
   * Set current state of the task. 
   * @param state
   */
  synchronized void setState(TaskStatus.State state){
    this.taskStatus.setRunState(state); 
  }

  void setTaskCleanupTask() {
    taskCleanup = true;
  }
	   
  boolean isTaskCleanupTask() {
    return taskCleanup;
  }

  boolean isJobCleanupTask() {
    return jobCleanup;
  }

  boolean isJobAbortTask() {
    // the task is an abort task if its marked for cleanup and the final 
    // expected state is either failed or killed.
    return isJobCleanupTask() 
           && (jobRunStateForCleanup == JobStatus.State.KILLED 
               || jobRunStateForCleanup == JobStatus.State.FAILED);
  }
  
  boolean isJobSetupTask() {
    return jobSetup;
  }

  void setJobSetupTask() {
    jobSetup = true; 
  }

  void setJobCleanupTask() {
    jobCleanup = true; 
  }

  /**
   * Sets the task to do job abort in the cleanup.
   * @param status the final runstate of the job 
   */
  void setJobCleanupTaskState(JobStatus.State status) {
    jobRunStateForCleanup = status;
  }
  
  boolean isMapOrReduce() {
    return !jobSetup && !jobCleanup && !taskCleanup;
  }
  
  void setUser(String user) {
    this.user = user;
  }

  /**
   * Get the name of the user running the job/task. TaskTracker needs task's
   * user name even before it's JobConf is localized. So we explicitly serialize
   * the user name.
   * 
   * @return user
   */
  public String getUser() {
    return user;
  }
  
  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, jobFile);
    taskId.write(out);
    out.writeInt(partition);
    out.writeInt(numSlotsRequired);
    taskStatus.write(out);
    skipRanges.write(out);
    out.writeBoolean(skipping);
    out.writeBoolean(jobCleanup);
    if (jobCleanup) {
      WritableUtils.writeEnum(out, jobRunStateForCleanup);
    }
    out.writeBoolean(jobSetup);
    out.writeBoolean(writeSkipRecs);
    out.writeBoolean(taskCleanup); 
    Text.writeString(out, user);
  }
  
  public void readFields(DataInput in) throws IOException {
    jobFile = Text.readString(in);
    taskId = TaskAttemptID.read(in);
    partition = in.readInt();
    numSlotsRequired = in.readInt();
    taskStatus.readFields(in);
    skipRanges.readFields(in);
    currentRecIndexIterator = skipRanges.skipRangeIterator();
    currentRecStartIndex = currentRecIndexIterator.next();
    skipping = in.readBoolean();
    jobCleanup = in.readBoolean();
    if (jobCleanup) {
      jobRunStateForCleanup = 
        WritableUtils.readEnum(in, JobStatus.State.class);
    }
    jobSetup = in.readBoolean();
    writeSkipRecs = in.readBoolean();
    taskCleanup = in.readBoolean();
    if (taskCleanup) {
      setPhase(TaskStatus.Phase.CLEANUP);
    }
    user = Text.readString(in);
  }

  @Override
  public String toString() { return taskId.toString(); }

  /**
   * Localize the given JobConf to be specific for this task.
   */
  public void localizeConfiguration(JobConf conf) throws IOException {
    conf.set("mapred.tip.id", taskId.getTaskID().toString()); 
    conf.set("mapred.task.id", taskId.toString());
    conf.setBoolean("mapred.task.is.map", isMapTask());
    conf.setInt("mapred.task.partition", partition);
    conf.set("mapred.job.id", taskId.getJobID().toString());
  }
  
  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
      throws IOException, ClassNotFoundException, InterruptedException;

  /** Return an approprate thread runner for this task. 
   * @param tip TODO*/
  public abstract TaskRunner createRunner(TaskTracker tracker, 
                                          TaskTracker.TaskInProgress tip, 
                                          TaskTracker.RunningJob rjob
                                          ) throws IOException;

  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 3000;

  private transient Progress taskProgress = new Progress();

  // Current counters
  private transient Counters counters = new Counters();

  /* flag to track whether task is done */
  private AtomicBoolean taskDone = new AtomicBoolean(false);
  
  public abstract boolean isMapTask();

  public Progress getProgress() { return taskProgress; }

  public void initialize(JobConf job, JobID id, 
                         Reporter reporter,
                         boolean useNewApi) throws IOException, 
                                                   ClassNotFoundException,
                                                   InterruptedException {
    jobContext = new JobContext(job, id, reporter);
    taskContext = new TaskAttemptContext(job, taskId, reporter);
    if (getState() == TaskStatus.State.UNASSIGNED) {
      setState(TaskStatus.State.RUNNING);
    }
    if (useNewApi) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using new api for output committer");
      }
      outputFormat =
        ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), job);
      committer = outputFormat.getOutputCommitter(taskContext);
    } else {
      committer = conf.getOutputCommitter();
    }
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      if ((committer instanceof FileOutputCommitter)) {
        FileOutputFormat.setWorkOutputPath(conf, 
          ((FileOutputCommitter)committer).getTempTaskOutputPath(taskContext));
      } else {
        FileOutputFormat.setWorkOutputPath(conf, outputPath);
      }
    }
    committer.setupTask(taskContext);
    Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(TaskTracker.TT_RESOURCE_CALCULATOR_PLUGIN,
            null, ResourceCalculatorPlugin.class);
    resourceCalculator = ResourceCalculatorPlugin
            .getResourceCalculatorPlugin(clazz, conf);
    LOG.info(" Using ResourceCalculatorPlugin : " + resourceCalculator);
    if (resourceCalculator != null) {
      initCpuCumulativeTime =
        resourceCalculator.getProcResourceValues().getCumulativeCpuTime();
    }
  }
  
  protected class TaskReporter 
      extends org.apache.hadoop.mapreduce.StatusReporter
      implements Runnable, Reporter {
    private TaskUmbilicalProtocol umbilical;
    private InputSplit split = null;
    private Progress taskProgress;
    private JvmContext jvmContext;
    private Thread pingThread = null;
    private static final int PROGRESS_STATUS_LEN_LIMIT = 512;
    
    /**
     * flag that indicates whether progress update needs to be sent to parent.
     * If true, it has been set. If false, it has been reset. 
     * Using AtomicBoolean since we need an atomic read & reset method. 
     */  
    private AtomicBoolean progressFlag = new AtomicBoolean(false);
    
    TaskReporter(Progress taskProgress,
                 TaskUmbilicalProtocol umbilical, JvmContext jvmContext) {
      this.umbilical = umbilical;
      this.taskProgress = taskProgress;
      this.jvmContext = jvmContext;
    }
    // getters and setters for flag
    void setProgressFlag() {
      progressFlag.set(true);
    }
    boolean resetProgressFlag() {
      return progressFlag.getAndSet(false);
    }
    public void setStatus(String status) {
      //Check to see if the status string 
      // is too long and just concatenate it
      // to progress limit characters.
      if (status.length() > PROGRESS_STATUS_LEN_LIMIT) {
        status = status.substring(0, PROGRESS_STATUS_LEN_LIMIT);
      }
      taskProgress.setStatus(status);
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    public void setProgress(float progress) {
      taskProgress.set(progress);
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    public void progress() {
      // indicate that progress update needs to be sent
      setProgressFlag();
    }
    public Counters.Counter getCounter(String group, String name) {
      Counters.Counter counter = null;
      if (counters != null) {
        counter = counters.findCounter(group, name);
      }
      return counter;
    }
    public Counters.Counter getCounter(Enum<?> name) {
      return counters == null ? null : counters.findCounter(name);
    }
    public void incrCounter(Enum key, long amount) {
      if (counters != null) {
        counters.incrCounter(key, amount);
      }
      setProgressFlag();
    }
    public void incrCounter(String group, String counter, long amount) {
      if (counters != null) {
        counters.incrCounter(group, counter, amount);
      }
      if(skipping && SkipBadRecords.COUNTER_GROUP.equals(group) && (
          SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS.equals(counter) ||
          SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS.equals(counter))) {
        //if application reports the processed records, move the 
        //currentRecStartIndex to the next.
        //currentRecStartIndex is the start index which has not yet been 
        //finished and is still in task's stomach.
        for(int i=0;i<amount;i++) {
          currentRecStartIndex = currentRecIndexIterator.next();
        }
      }
      setProgressFlag();
    }
    public void setInputSplit(InputSplit split) {
      this.split = split;
    }
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      if (split == null) {
        throw new UnsupportedOperationException("Input only available on map");
      } else {
        return split;
      }
    }  
    /** 
     * The communication thread handles communication with the parent (Task Tracker). 
     * It sends progress updates if progress has been made or if the task needs to 
     * let the parent know that it's alive. It also pings the parent to see if it's alive. 
     */
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;
      // get current flag value and reset it as well
      boolean sendProgress = resetProgressFlag();
      while (!taskDone.get()) {
        try {
          boolean taskFound = true; // whether TT knows about this task
          // sleep for a bit
          try {
            Thread.sleep(PROGRESS_INTERVAL);
          } 
          catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(getTaskID() + " Progress/ping thread exiting " +
                "since it got interrupted");
            }
            break;
          }

          if (sendProgress) {
            // we need to send progress update
            updateCounters();
            taskStatus.statusUpdate(taskProgress.get(),
                                    taskProgress.toString(), 
                                    counters);
            taskFound = umbilical.statusUpdate(taskId, taskStatus, jvmContext);
            taskStatus.clearStatus();
          }
          else {
            // send ping 
            taskFound = umbilical.ping(taskId, jvmContext);
          }

          // if Task Tracker is not aware of our task ID (probably because it died and 
          // came back up), kill ourselves
          if (!taskFound) {
            LOG.warn("Parent died.  Exiting "+taskId);
            System.exit(66);
          }

          sendProgress = resetProgressFlag(); 
          remainingRetries = MAX_RETRIES;
        } 
        catch (Throwable t) {
          LOG.info("Communication exception: " + StringUtils.stringifyException(t));
          remainingRetries -=1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing "+taskId);
            System.exit(65);
          }
        }
      }
    }
    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(this, "communication thread");
        pingThread.setDaemon(true);
        pingThread.start();
      }
    }
    public void stopCommunicationThread() throws InterruptedException {
      // Updating resources specified in ResourceCalculatorPlugin
      if (pingThread != null) {
        pingThread.interrupt();
        pingThread.join();
      }
    }
  }
  
  /**
   *  Reports the next executing record range to TaskTracker.
   *  
   * @param umbilical
   * @param nextRecIndex the record index which would be fed next.
   * @throws IOException
   */
  protected void reportNextRecordRange(final TaskUmbilicalProtocol umbilical, 
      long nextRecIndex) throws IOException{
    //currentRecStartIndex is the start index which has not yet been finished 
    //and is still in task's stomach.
    long len = nextRecIndex - currentRecStartIndex +1;
    SortedRanges.Range range = 
      new SortedRanges.Range(currentRecStartIndex, len);
    taskStatus.setNextRecordRange(range);
    if (LOG.isDebugEnabled()) {
      LOG.debug("sending reportNextRecordRange " + range);
    }
    umbilical.reportNextRecordRange(taskId, range, jvmContext);
  }

  /**
   * An updater that tracks the last number reported for a given file
   * system and only creates the counters when they are needed.
   */
  class FileSystemStatisticUpdater {
    private long prevReadBytes = 0;
    private long prevWriteBytes = 0;
    private FileSystem.Statistics stats;
    private Counters.Counter readCounter = null;
    private Counters.Counter writeCounter = null;
    private String[] counterNames;
    
    FileSystemStatisticUpdater(String uriScheme, FileSystem.Statistics stats) {
      this.stats = stats;
      this.counterNames = getFileSystemCounterNames(uriScheme);
    }

    void updateCounters() {
      long newReadBytes = stats.getBytesRead();
      long newWriteBytes = stats.getBytesWritten();
      if (prevReadBytes != newReadBytes) {
        if (readCounter == null) {
          readCounter = counters.findCounter(FILESYSTEM_COUNTER_GROUP, 
              counterNames[0]);
        }
        readCounter.increment(newReadBytes - prevReadBytes);
        prevReadBytes = newReadBytes;
      }
      if (prevWriteBytes != newWriteBytes) {
        if (writeCounter == null) {
          writeCounter = counters.findCounter(FILESYSTEM_COUNTER_GROUP, 
              counterNames[1]);
        }
        writeCounter.increment(newWriteBytes - prevWriteBytes);
        prevWriteBytes = newWriteBytes;
      }
    }
  }
  
  /**
   * A Map where Key-> URIScheme and value->FileSystemStatisticUpdater
   */
  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
     new HashMap<String, FileSystemStatisticUpdater>();
  
  /**
   * Update resource information counters
   */
   void updateResourceCounters() {
     // Update generic resource counters
     updateHeapUsageCounter();
     
     if (resourceCalculator == null) {
       return;
     }
     ProcResourceValues res = resourceCalculator.getProcResourceValues();
     long cpuTime = res.getCumulativeCpuTime();
     long pMem = res.getPhysicalMemorySize();
     long vMem = res.getVirtualMemorySize();
     // Remove the CPU time consumed previously by JVM reuse
     cpuTime -= initCpuCumulativeTime;
     counters.findCounter(Counter.CPU_MILLISECONDS).setValue(cpuTime);
     counters.findCounter(Counter.PHYSICAL_MEMORY_BYTES).setValue(pMem);
     counters.findCounter(Counter.VIRTUAL_MEMORY_BYTES).setValue(vMem);
   }
  
  private synchronized void updateCounters() {
    for(Statistics stat: FileSystem.getAllStatistics()) {
      String uriScheme = stat.getScheme();
      FileSystemStatisticUpdater updater = statisticUpdaters.get(uriScheme);
      if(updater==null) {//new FileSystem has been found in the cache
        updater = new FileSystemStatisticUpdater(uriScheme, stat);
        statisticUpdaters.put(uriScheme, updater);
      }
      updater.updateCounters();      
    }
    // TODO Should CPU related counters be update only once i.e in the end
    updateResourceCounters();
  }

  /**
   * Updates the {@link TaskCounter#COMMITTED_HEAP_BYTES} counter to reflect the
   * current total committed heap space usage of this JVM.
   */
  @SuppressWarnings("deprecation")
  private void updateHeapUsageCounter() {
    long currentHeapUsage = Runtime.getRuntime().totalMemory();
    counters.findCounter(Counter.COMMITTED_HEAP_BYTES)
            .setValue(currentHeapUsage);
  }

  public void done(TaskUmbilicalProtocol umbilical,
                   TaskReporter reporter
                   ) throws IOException, InterruptedException {
    LOG.info("Task:" + taskId + " is done."
             + " And is in the process of commiting");
    updateCounters();

    boolean commitRequired = isCommitRequired();
    if (commitRequired) {
      int retries = MAX_RETRIES;
      setState(TaskStatus.State.COMMIT_PENDING);
      // say the task tracker that task is commit pending
      while (true) {
        try {
          umbilical.commitPending(taskId, taskStatus, jvmContext);
          break;
        } catch (InterruptedException ie) {
          // ignore
        } catch (IOException ie) {
          LOG.warn("Failure sending commit pending: " + 
                    StringUtils.stringifyException(ie));
          if (--retries == 0) {
            System.exit(67);
          }
        }
      }
      //wait for commit approval and commit
      commit(umbilical, reporter, committer);
    }
    taskDone.set(true);
    reporter.stopCommunicationThread();
    sendLastUpdate(umbilical);
    //signal the tasktracker that we are done
    sendDone(umbilical);
  }

  /**
   * Checks if this task has anything to commit, depending on the
   * type of task, as well as on whether the {@link OutputCommitter}
   * has anything to commit.
   * 
   * @return true if the task has to commit
   * @throws IOException
   */
  boolean isCommitRequired() throws IOException {
    boolean commitRequired = false;
    if (isMapOrReduce()) {
      commitRequired = committer.needsTaskCommit(taskContext);
    }
    return commitRequired;
  }

  protected void statusUpdate(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        if (!umbilical.statusUpdate(getTaskID(), taskStatus, jvmContext)) {
          LOG.warn("Parent died.  Exiting "+taskId);
          System.exit(66);
        }
        taskStatus.clearStatus();
        return;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt(); // interrupt ourself
      } catch (IOException ie) {
        LOG.warn("Failure sending status update: " + 
                  StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }
  
  /**
   * Sends last status update before sending umbilical.done(); 
   */
  private void sendLastUpdate(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    taskStatus.setOutputSize(calculateOutputSize());
    // send a final status report
    taskStatus.statusUpdate(taskProgress.get(),
                            taskProgress.toString(), 
                            counters);
    statusUpdate(umbilical);
  }

  /**
   * Calculates the size of output for this task.
   * 
   * @return -1 if it can't be found.
   */
   private long calculateOutputSize() throws IOException {
    if (!isMapOrReduce()) {
      return -1;
    }

    if (isMapTask() && conf.getNumReduceTasks() > 0) {
      try {
        Path mapOutput =  mapOutputFile.getOutputFile();
        FileSystem localFS = FileSystem.getLocal(conf);
        return localFS.getFileStatus(mapOutput).getLen();
      } catch (IOException e) {
        LOG.warn ("Could not find output size " , e);
      }
    }
    return -1;
  }

  private void sendDone(TaskUmbilicalProtocol umbilical) throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        umbilical.done(getTaskID(), jvmContext);
        LOG.info("Task '" + taskId + "' done.");
        return;
      } catch (IOException ie) {
        LOG.warn("Failure signalling completion: " + 
                 StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }

  private void commit(TaskUmbilicalProtocol umbilical,
                      TaskReporter reporter,
                      org.apache.hadoop.mapreduce.OutputCommitter committer
                      ) throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        while (!umbilical.canCommit(taskId, jvmContext)) {
          try {
            Thread.sleep(1000);
          } catch(InterruptedException ie) {
            //ignore
          }
          reporter.setProgressFlag();
        }
        break;
      } catch (IOException ie) {
        LOG.warn("Failure asking whether task can commit: " + 
            StringUtils.stringifyException(ie));
        if (--retries == 0) {
          //if it couldn't query successfully then delete the output
          discardOutput(taskContext);
          System.exit(68);
        }
      }
    }
    
    // task can Commit now  
    try {
      LOG.info("Task " + taskId + " is allowed to commit now");
      committer.commitTask(taskContext);
      return;
    } catch (IOException iee) {
      LOG.warn("Failure committing: " + 
        StringUtils.stringifyException(iee));
      //if it couldn't commit a successfully then delete the output
      discardOutput(taskContext);
      throw iee;
    }
  }

  private 
  void discardOutput(TaskAttemptContext taskContext) {
    try {
      committer.abortTask(taskContext);
    } catch (IOException ioe)  {
      LOG.warn("Failure cleaning up: " + 
               StringUtils.stringifyException(ioe));
    }
  }

  protected void runTaskCleanupTask(TaskUmbilicalProtocol umbilical,
                                TaskReporter reporter) 
  throws IOException, InterruptedException {
    taskCleanup(umbilical);
    done(umbilical, reporter);
  }

  void taskCleanup(TaskUmbilicalProtocol umbilical) 
  throws IOException {
    // set phase for this task
    setPhase(TaskStatus.Phase.CLEANUP);
    getProgress().setStatus("cleanup");
    statusUpdate(umbilical);
    LOG.info("Runnning cleanup for the task");
    // do the cleanup
    committer.abortTask(taskContext);
  }

  protected void runJobCleanupTask(TaskUmbilicalProtocol umbilical,
                               TaskReporter reporter
                              ) throws IOException, InterruptedException {
    // set phase for this task
    setPhase(TaskStatus.Phase.CLEANUP);
    getProgress().setStatus("cleanup");
    statusUpdate(umbilical);
    // do the cleanup
    LOG.info("Cleaning up job");
    if (jobRunStateForCleanup == JobStatus.State.FAILED 
        || jobRunStateForCleanup == JobStatus.State.KILLED) {
      LOG.info("Aborting job with runstate : " + jobRunStateForCleanup);
          committer.abortJob(jobContext, jobRunStateForCleanup);
    } else if (jobRunStateForCleanup == JobStatus.State.SUCCEEDED){
      LOG.info("Committing job");
      committer.commitJob(jobContext);
    } else {
      throw new IOException("Invalid state of the job for cleanup. State found "
                            + jobRunStateForCleanup + " expecting "
                            + JobStatus.State.SUCCEEDED + ", " 
                            + JobStatus.State.FAILED + " or "
                            + JobStatus.State.KILLED);
    }
    // delete the staging area for the job
    JobConf conf = new JobConf(jobContext.getConfiguration());
    if (!supportIsolationRunner(conf)) {
      String jobTempDir = conf.get("mapreduce.job.dir");
      Path jobTempDirPath = new Path(jobTempDir);
      FileSystem fs = jobTempDirPath.getFileSystem(conf);
      fs.delete(jobTempDirPath, true);
    }
    done(umbilical, reporter);
  }

  protected boolean supportIsolationRunner(JobConf conf) {
    return (conf.getKeepTaskFilesPattern() != null || conf
         .getKeepFailedTaskFiles());
  }
  
  protected void runJobSetupTask(TaskUmbilicalProtocol umbilical,
                             TaskReporter reporter
                             ) throws IOException, InterruptedException {
    // do the setup
    getProgress().setStatus("setup");
    committer.setupJob(jobContext);
    done(umbilical, reporter);
  }
  
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile.setConf(this.conf);
    this.lDirAlloc = new LocalDirAllocator("mapred.local.dir");
    // add the static resolutions (this is required for the junit to
    // work on testcases that simulate multiple nodes on a single physical
    // node.
    String hostToResolved[] = conf.getStrings("hadoop.net.static.resolutions");
    if (hostToResolved != null) {
      for (String str : hostToResolved) {
        String name = str.substring(0, str.indexOf('='));
        String resolvedName = str.substring(str.indexOf('=') + 1);
        NetUtils.addStaticResolution(name, resolvedName);
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * OutputCollector for the combiner.
   */
  protected static class CombineOutputCollector<K extends Object, V extends Object> 
  implements OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    public CombineOutputCollector(Counters.Counter outCounter) {
      this.outCounter = outCounter;
    }
    public synchronized void setWriter(Writer<K, V> writer) {
      this.writer = writer;
    }
    public synchronized void collect(K key, V value)
        throws IOException {
      outCounter.increment(1);
      writer.append(key, value);
    }
  }

  /** Iterates values while keys match in sorted input. */
  static class ValuesIterator<KEY,VALUE> implements Iterator<VALUE> {
    protected RawKeyValueIterator in; //input iterator
    private KEY key;               // current key
    private KEY nextKey;
    private VALUE value;             // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private RawComparator<KEY> comparator;
    protected Progressable reporter;
    private Deserializer<KEY> keyDeserializer;
    private Deserializer<VALUE> valDeserializer;
    private DataInputBuffer keyIn = new DataInputBuffer();
    private DataInputBuffer valueIn = new DataInputBuffer();
    
    public ValuesIterator (RawKeyValueIterator in, 
                           RawComparator<KEY> comparator, 
                           Class<KEY> keyClass,
                           Class<VALUE> valClass, Configuration conf, 
                           Progressable reporter)
      throws IOException {
      this.in = in;
      this.comparator = comparator;
      this.reporter = reporter;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
      this.keyDeserializer.open(keyIn);
      this.valDeserializer = serializationFactory.getDeserializer(valClass);
      this.valDeserializer.open(this.valueIn);
      readNextKey();
      key = nextKey;
      nextKey = null; // force new instance creation
      hasNext = more;
    }

    RawKeyValueIterator getRawIterator() { return in; }
    
    /// Iterator methods

    public boolean hasNext() { return hasNext; }

    private int ctr = 0;
    public VALUE next() {
      if (!hasNext) {
        throw new NoSuchElementException("iterate past last value");
      }
      try {
        readNextValue();
        readNextKey();
      } catch (IOException ie) {
        throw new RuntimeException("problem advancing post rec#"+ctr, ie);
      }
      reporter.progress();
      return value;
    }

    public void remove() { throw new RuntimeException("not implemented"); }

    /// Auxiliary methods

    /** Start processing next unique key. */
    void nextKey() throws IOException {
      // read until we find a new key
      while (hasNext) { 
        readNextKey();
      }
      ++ctr;
      
      // move the next key to the current one
      KEY tmpKey = key;
      key = nextKey;
      nextKey = tmpKey;
      hasNext = more;
    }

    /** True iff more keys remain. */
    boolean more() { 
      return more; 
    }

    /** The current key. */
    KEY getKey() { 
      return key; 
    }

    /** 
     * read the next key 
     */
    private void readNextKey() throws IOException {
      more = in.next();
      if (more) {
        DataInputBuffer nextKeyBytes = in.getKey();
        keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength());
        nextKey = keyDeserializer.deserialize(nextKey);
        hasNext = key != null && (comparator.compare(key, nextKey) == 0);
      } else {
        hasNext = false;
      }
    }

    /**
     * Read the next value
     * @throws IOException
     */
    private void readNextValue() throws IOException {
      DataInputBuffer nextValueBytes = in.getValue();
      valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(), nextValueBytes.getLength());
      value = valDeserializer.deserialize(value);
    }
  }

  protected static class CombineValuesIterator<KEY,VALUE>
      extends ValuesIterator<KEY,VALUE> {

    private final Counters.Counter combineInputCounter;

    public CombineValuesIterator(RawKeyValueIterator in,
        RawComparator<KEY> comparator, Class<KEY> keyClass,
        Class<VALUE> valClass, Configuration conf, Reporter reporter,
        Counters.Counter combineInputCounter) throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
      this.combineInputCounter = combineInputCounter;
    }

    public VALUE next() {
      combineInputCounter.increment(1);
      return super.next();
    }
  }

  private static final Constructor<org.apache.hadoop.mapreduce.Reducer.Context> 
  contextConstructor;
  static {
    try {
      contextConstructor = 
        org.apache.hadoop.mapreduce.Reducer.Context.class.getConstructor
        (new Class[]{org.apache.hadoop.mapreduce.Reducer.class,
            Configuration.class,
            org.apache.hadoop.mapreduce.TaskAttemptID.class,
            RawKeyValueIterator.class,
            org.apache.hadoop.mapreduce.Counter.class,
            org.apache.hadoop.mapreduce.Counter.class,
            org.apache.hadoop.mapreduce.RecordWriter.class,
            org.apache.hadoop.mapreduce.OutputCommitter.class,
            org.apache.hadoop.mapreduce.StatusReporter.class,
            RawComparator.class,
            Class.class,
            Class.class});
    } catch (NoSuchMethodException nme) {
      throw new IllegalArgumentException("Can't find constructor");
    }
  }

  @SuppressWarnings("unchecked")
  protected static <INKEY,INVALUE,OUTKEY,OUTVALUE> 
  org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context
  createReduceContext(org.apache.hadoop.mapreduce.Reducer
                        <INKEY,INVALUE,OUTKEY,OUTVALUE> reducer,
                      Configuration job,
                      org.apache.hadoop.mapreduce.TaskAttemptID taskId, 
                      RawKeyValueIterator rIter,
                      org.apache.hadoop.mapreduce.Counter inputKeyCounter,
                      org.apache.hadoop.mapreduce.Counter inputValueCounter,
                      org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output, 
                      org.apache.hadoop.mapreduce.OutputCommitter committer,
                      org.apache.hadoop.mapreduce.StatusReporter reporter,
                      RawComparator<INKEY> comparator,
                      Class<INKEY> keyClass, Class<INVALUE> valueClass
  ) throws IOException, ClassNotFoundException {
    try {

      return contextConstructor.newInstance(reducer, job, taskId,
                                            rIter, inputKeyCounter, 
                                            inputValueCounter, output, 
                                            committer, reporter, comparator, 
                                            keyClass, valueClass);
    } catch (InstantiationException e) {
      throw new IOException("Can't create Context", e);
    } catch (InvocationTargetException e) {
      throw new IOException("Can't invoke Context constructor", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't invoke Context constructor", e);
    }
  }

  protected static abstract class CombinerRunner<K,V> {
    protected final Counters.Counter inputCounter;
    protected final JobConf job;
    protected final TaskReporter reporter;

    CombinerRunner(Counters.Counter inputCounter,
                   JobConf job,
                   TaskReporter reporter) {
      this.inputCounter = inputCounter;
      this.job = job;
      this.reporter = reporter;
    }
    
    /**
     * Run the combiner over a set of inputs.
     * @param iterator the key/value pairs to use as input
     * @param collector the output collector
     */
    abstract void combine(RawKeyValueIterator iterator, 
                          OutputCollector<K,V> collector
                         ) throws IOException, InterruptedException, 
                                  ClassNotFoundException;

    @SuppressWarnings("unchecked")
    static <K,V> 
    CombinerRunner<K,V> create(JobConf job,
                               TaskAttemptID taskId,
                               Counters.Counter inputCounter,
                               TaskReporter reporter,
                               org.apache.hadoop.mapreduce.OutputCommitter committer
                              ) throws ClassNotFoundException {
      Class<? extends Reducer<K,V,K,V>> cls = 
        (Class<? extends Reducer<K,V,K,V>>) job.getCombinerClass();

      if (cls != null) {
        return new OldCombinerRunner(cls, job, inputCounter, reporter);
      }
      // make a task context so we can get the classes
      org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
        new org.apache.hadoop.mapreduce.TaskAttemptContext(job, taskId);
      Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>> newcls = 
        (Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>>)
           taskContext.getCombinerClass();
      if (newcls != null) {
        return new NewCombinerRunner<K,V>(newcls, job, taskId, taskContext, 
                                          inputCounter, reporter, committer);
      }
      
      return null;
    }
  }
  
  protected static class OldCombinerRunner<K,V> extends CombinerRunner<K,V> {
    private final Class<? extends Reducer<K,V,K,V>> combinerClass;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final RawComparator<K> comparator;

    @SuppressWarnings("unchecked")
    protected OldCombinerRunner(Class<? extends Reducer<K,V,K,V>> cls,
                                JobConf conf,
                                Counters.Counter inputCounter,
                                TaskReporter reporter) {
      super(inputCounter, conf, reporter);
      combinerClass = cls;
      keyClass = (Class<K>) job.getMapOutputKeyClass();
      valueClass = (Class<V>) job.getMapOutputValueClass();
      comparator = (RawComparator<K>) job.getOutputKeyComparator();
    }

    @SuppressWarnings("unchecked")
    protected void combine(RawKeyValueIterator kvIter,
                           OutputCollector<K,V> combineCollector
                           ) throws IOException {
      Reducer<K,V,K,V> combiner = 
        ReflectionUtils.newInstance(combinerClass, job);
      try {
        CombineValuesIterator<K,V> values = 
          new CombineValuesIterator<K,V>(kvIter, comparator, keyClass, 
                                         valueClass, job, Reporter.NULL,
                                         inputCounter);
        while (values.more()) {
          combiner.reduce(values.getKey(), values, combineCollector,
              Reporter.NULL);
          values.nextKey();
        }
      } finally {
        combiner.close();
      }
    }
  }
  
  protected static class NewCombinerRunner<K, V> extends CombinerRunner<K,V> {
    private final Class<? extends org.apache.hadoop.mapreduce.Reducer<K,V,K,V>> 
        reducerClass;
    private final org.apache.hadoop.mapreduce.TaskAttemptID taskId;
    private final RawComparator<K> comparator;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final org.apache.hadoop.mapreduce.OutputCommitter committer;

    @SuppressWarnings("unchecked")
    NewCombinerRunner(Class reducerClass,
                      JobConf job,
                      org.apache.hadoop.mapreduce.TaskAttemptID taskId,
                      org.apache.hadoop.mapreduce.TaskAttemptContext context,
                      Counters.Counter inputCounter,
                      TaskReporter reporter,
                      org.apache.hadoop.mapreduce.OutputCommitter committer) {
      super(inputCounter, job, reporter);
      this.reducerClass = reducerClass;
      this.taskId = taskId;
      keyClass = (Class<K>) context.getMapOutputKeyClass();
      valueClass = (Class<V>) context.getMapOutputValueClass();
      comparator = (RawComparator<K>) context.getSortComparator();
      this.committer = committer;
    }

    private static class OutputConverter<K,V>
            extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
      OutputCollector<K,V> output;
      OutputConverter(OutputCollector<K,V> output) {
        this.output = output;
      }

      @Override
      public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context){
      }

      @Override
      public void write(K key, V value
                        ) throws IOException, InterruptedException {
        output.collect(key,value);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    void combine(RawKeyValueIterator iterator, 
                 OutputCollector<K,V> collector
                 ) throws IOException, InterruptedException,
                          ClassNotFoundException {
      // make a reducer
      org.apache.hadoop.mapreduce.Reducer<K,V,K,V> reducer =
        (org.apache.hadoop.mapreduce.Reducer<K,V,K,V>)
          ReflectionUtils.newInstance(reducerClass, job);
      org.apache.hadoop.mapreduce.Reducer.Context 
           reducerContext = createReduceContext(reducer, job, taskId,
                                                iterator, null, inputCounter, 
                                                new OutputConverter(collector),
                                                committer,
                                                reporter, comparator, keyClass,
                                                valueClass);
      reducer.run(reducerContext);
    } 
  }
}
