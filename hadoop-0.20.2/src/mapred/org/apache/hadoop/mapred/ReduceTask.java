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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Math;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.IFile.*;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;

/** A Reduce task. */
class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;
  private ReduceCopier reduceCopier;

  private CompressionCodec codec;


  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(Counter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(Counter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(Counter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(Counter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(Counter.COMBINE_OUTPUT_RECORDS);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
    new Comparator<FileStatus>() {
      public int compare(FileStatus a, FileStatus b) {
        if (a.getLen() < b.getLen())
          return -1;
        else if (a.getLen() == b.getLen())
          if (a.getPath().toString().equals(b.getPath().toString()))
            return 0;
          else
            return -1; 
        else
          return 1;
      }
  };
  
  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
    new TreeSet<FileStatus>(mapOutputFileComparator);

  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  
  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
                                 TaskTracker.RunningJob rjob
                                 ) throws IOException {
    return new ReduceTaskRunner(tip, tracker, this.conf, rjob);
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer.
  private Path[] getMapFiles(FileSystem fs, boolean isLocal) 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    if (isLocal) {
      // for local jobs
      for(int i = 0; i < numMaps; ++i) {
        fileList.add(mapOutputFile.getInputFile(i));
      }
    } else {
      // for non local jobs
      for (FileStatus filestatus : mapOutputFilesOnDisk) {
        fileList.add(filestatus.getPath());
      }
    }
    return fileList.toArray(new Path[0]);
  }

  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().get()); // update progress
      reporter.progress();
    }
  }

  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(Counter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(Counter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    this.umbilical = umbilical;
    job.setBoolean("mapred.skip.on", isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // start thread that will handle communication with parent
    TaskReporter reporter = new TaskReporter(getProgress(), umbilical,
        jvmContext);
    reporter.startCommunicationThread();
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }
    
    // Initialize the codec
    codec = initCodec();

    boolean isLocal = "local".equals(job.get("mapred.job.tracker", "local"));
    if (!isLocal) {
      reduceCopier = new ReduceCopier(umbilical, job, reporter);
      if (!reduceCopier.fetchOutputs()) {
        if(reduceCopier.mergeThrowable instanceof FSError) {
          throw (FSError)reduceCopier.mergeThrowable;
        }
        throw new IOException("Task: " + getTaskID() + 
            " - The reduce copier failed", reduceCopier.mergeThrowable);
      }
    }
    copyPhase.complete();                         // copy is already complete
    setPhase(TaskStatus.Phase.SORT);
    statusUpdate(umbilical);

    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    RawKeyValueIterator rIter = isLocal
      ? Merger.merge(job, rfs, job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(), codec, getMapFiles(rfs, true),
          !conf.getKeepFailedTaskFiles(), job.getInt("io.sort.factor", 100),
          new Path(getTaskID().toString()), job.getOutputKeyComparator(),
          reporter, spilledRecordsCounter, null)
      : reduceCopier.createKVIterator(job, rfs, reporter);
        
    // free up the data structures
    mapOutputFilesOnDisk.clear();
    
    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 
    statusUpdate(umbilical);
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    if (useNewApi) {
      runNewReducer(job, umbilical, reporter, rIter, comparator, 
                    keyClass, valueClass);
    } else {
      runOldReducer(job, umbilical, reporter, rIter, comparator, 
                    keyClass, valueClass);
    }
    done(umbilical, reporter);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldReducer(JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException {
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    String finalName = getOutputName(getPartition());

    FileSystem fs = FileSystem.get(job);

    final RecordWriter<OUTKEY,OUTVALUE> out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new OutputCollector<OUTKEY,OUTVALUE>() {
        public void collect(OUTKEY key, OUTVALUE value)
          throws IOException {
          out.write(key, value);
          reduceOutputCounter.increment(1);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };
    
    // apply reduce function
    try {
      //increment processed counter only if skipping feature is enabled
      boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job)>0 &&
        SkipBadRecords.getAutoIncrReducerProcCount(job);
      
      ReduceValuesIterator<INKEY,INVALUE> values = isSkipping() ? 
          new SkippingReduceValuesIterator<INKEY,INVALUE>(rIter, 
              comparator, keyClass, valueClass, 
              job, reporter, umbilical) :
          new ReduceValuesIterator<INKEY,INVALUE>(rIter, 
          job.getOutputValueGroupingComparator(), keyClass, valueClass, 
          job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
        }
        values.nextKey();
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
  }

  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
  
    NewTrackingRecordWriter(org.apache.hadoop.mapreduce.RecordWriter<K,V> real,
                            org.apache.hadoop.mapreduce.Counter recordCounter) {
      this.real = real;
      this.outputRecordCounter = recordCounter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      real.close(context);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      real.write(key,value);
      outputRecordCounter.increment(1);
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass
                     ) throws IOException,InterruptedException, 
                              ClassNotFoundException {
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    rIter = new RawKeyValueIterator() {
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        reducePhase.set(rawIter.getProgress().get());
        reporter.progress();
        return ret;
      }
    };
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.TaskAttemptContext(job, getTaskID());
    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output =
      (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE>)
        outputFormat.getRecordWriter(taskContext);
     org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW = 
       new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(output, reduceOutputCounter);
    job.setBoolean("mapred.skip.on", isSkipping());
    org.apache.hadoop.mapreduce.Reducer.Context 
         reducerContext = createReduceContext(reducer, job, getTaskID(),
                                               rIter, reduceInputKeyCounter,
                                               reduceInputValueCounter, 
                                               trackedRW, committer,
                                               reporter, comparator, keyClass,
                                               valueClass);
    reducer.run(reducerContext);
    output.close(reducerContext);
  }

  private static enum CopyOutputErrorType {
    NO_ERROR,
    READ_ERROR,
    OTHER_ERROR
  };

  class ReduceCopier<K, V> implements MRConstants {

    /** Reference to the umbilical object */
    private TaskUmbilicalProtocol umbilical;
    private final TaskReporter reporter;
    
    /** Reference to the task object */
    
    /** Number of ms before timing out a copy */
    private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
    
    /** Max events to fetch in one go from the tasktracker */
    private static final int MAX_EVENTS_TO_FETCH = 10000;

    /**
     * our reduce task instance
     */
    private ReduceTask reduceTask;
    
    /**
     * the list of map outputs currently being copied
     */
    private List<MapOutputLocation> scheduledCopies;
    
    /**
     *  the results of dispatched copy attempts
     */
    private List<CopyResult> copyResults;
    
    int numEventsFetched = 0;
    private Object copyResultsOrNewEventsLock = new Object();

    
    /**
     *  the number of outputs to copy in parallel
     */
    private int numCopiers;
    
    /**
     *  a number that is set to the max #fetches we'd schedule and then
     *  pause the schduling
     */
    private int maxInFlight;
    
    
    /**
     * busy hosts from which copies are being backed off
     * Map of host -> next contact time
     */
    private Map<String, Long> penaltyBox;
    
    /**
     * the set of unique hosts from which we are copying
     */
    private Set<String> uniqueHosts;
    
    /**
     * A reference to the RamManager for writing the map outputs to.
     */
    
    private ShuffleRamManager ramManager;
    
    /**
     * A reference to the local file system for writing the map outputs to.
     */
    private FileSystem localFileSys;

    private FileSystem rfs;
    /**
     * Number of files to merge at a time
     */
    private int ioSortFactor;
    
    /**
     * A reference to the throwable object (if merge throws an exception)
     */
    private volatile Throwable mergeThrowable;
    
    /** 
     * A flag to indicate when to exit localFS merge
     */
    private volatile boolean exitLocalFSMerge = false;

    /** 
     * A flag to indicate when to exit getMapEvents thread 
     */
    private volatile boolean exitGetMapEvents = false;
    
    /**
     * When we accumulate maxInMemOutputs number of files in ram, we merge/spill
     */
    private final int maxInMemOutputs;

    /**
     * Usage threshold for in-memory output accumulation.
     */
    private final float maxInMemCopyPer;

    /**
     * Maximum memory usage of map outputs to merge from memory into
     * the reduce, in bytes.
     */
    private final long maxInMemReduce;

    /**
     * The threads for fetching the files.
     */
    private List<MapOutputCopier> copiers = null;
    
    /**
     * The object for metrics reporting.
     */
    private ShuffleClientMetrics shuffleClientMetrics = null;
    
    /**
     * the minimum interval between tasktracker polls
     */
    private static final long MIN_POLL_INTERVAL = 1000;
    
    /**
     * a list of map output locations for fetch retrials 
     */
    private List<MapOutputLocation> retryFetches =
      new ArrayList<MapOutputLocation>();
    
    /** 
     * The set of required map outputs
     */
    private Set <TaskID> copiedMapOutputs = 
      Collections.synchronizedSet(new TreeSet<TaskID>());
    
    /** 
     * The set of obsolete map taskids.
     */
    private Set <TaskAttemptID> obsoleteMapIds = 
      Collections.synchronizedSet(new TreeSet<TaskAttemptID>());
    
    private Random random = null;

    /**
     * the max of all the map completion times
     */
    private int maxMapRuntime;
    
    /**
     * Maximum number of fetch-retries per-map before reporting it.
     */
    private int maxFetchFailuresBeforeReporting;
    
    /**
     * Maximum number of fetch failures before reducer aborts.
     */
    private final int abortFailureLimit;

    /**
     * Initial penalty time in ms for a fetch failure.
     */
    private static final long INITIAL_PENALTY = 10000;

    /**
     * Penalty growth rate for each fetch failure.
     */
    private static final float PENALTY_GROWTH_RATE = 1.3f;

    /**
     * Default limit for maximum number of fetch failures before reporting.
     */
    private final static int REPORT_FAILURE_LIMIT = 10;

    /**
     * Combiner runner, if a combiner is needed
     */
    private CombinerRunner combinerRunner;

    /**
     * Resettable collector used for combine.
     */
    private CombineOutputCollector combineCollector = null;

    /**
     * Maximum percent of failed fetch attempt before killing the reduce task.
     */
    private static final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;

    /**
     * Minimum percent of progress required to keep the reduce alive.
     */
    private static final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;

    /**
     * Maximum percent of shuffle execution time required to keep the reducer alive.
     */
    private static final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;
    
    /**
     * Minimum number of map fetch retries.
     */
    private static final int MIN_FETCH_RETRIES_PER_MAP = 2;

    /**
     * The minimum percentage of maps yet to be copied, 
     * which indicates end of shuffle
     */
    private static final float MIN_PENDING_MAPS_PERCENT = 0.25f;
    /**
     * Maximum no. of unique maps from which we failed to fetch map-outputs
     * even after {@link #maxFetchRetriesPerMap} retries; after this the
     * reduce task is failed.
     */
    private int maxFailedUniqueFetches = 5;

    /**
     * The maps from which we fail to fetch map-outputs 
     * even after {@link #maxFetchRetriesPerMap} retries.
     */
    Set<TaskID> fetchFailedMaps = new TreeSet<TaskID>(); 
    
    /**
     * A map of taskId -> no. of failed fetches
     */
    Map<TaskAttemptID, Integer> mapTaskToFailedFetchesMap = 
      new HashMap<TaskAttemptID, Integer>();    

    /**
     * Initial backoff interval (milliseconds)
     */
    private static final int BACKOFF_INIT = 4000; 
    
    /**
     * The interval for logging in the shuffle
     */
    private static final int MIN_LOG_TIME = 60000;

    /** 
     * List of in-memory map-outputs.
     */
    private final List<MapOutput> mapOutputsFilesInMemory =
      Collections.synchronizedList(new LinkedList<MapOutput>());
    
    /**
     * The map for (Hosts, List of MapIds from this Host) maintaining
     * map output locations
     */
    private final Map<String, List<MapOutputLocation>> mapLocations = 
      new ConcurrentHashMap<String, List<MapOutputLocation>>();
    
    /**
     * This class contains the methods that should be used for metrics-reporting
     * the specific metrics for shuffle. This class actually reports the
     * metrics for the shuffle client (the ReduceTask), and hence the name
     * ShuffleClientMetrics.
     */
    class ShuffleClientMetrics implements Updater {
      private MetricsRecord shuffleMetrics = null;
      private int numFailedFetches = 0;
      private int numSuccessFetches = 0;
      private long numBytes = 0;
      private int numThreadsBusy = 0;
      ShuffleClientMetrics(JobConf conf) {
        MetricsContext metricsContext = MetricsUtil.getContext("mapred");
        this.shuffleMetrics = 
          MetricsUtil.createRecord(metricsContext, "shuffleInput");
        this.shuffleMetrics.setTag("user", conf.getUser());
        this.shuffleMetrics.setTag("jobName", conf.getJobName());
        this.shuffleMetrics.setTag("jobId", ReduceTask.this.getJobID().toString());
        this.shuffleMetrics.setTag("taskId", getTaskID().toString());
        this.shuffleMetrics.setTag("sessionId", conf.getSessionId());
        metricsContext.registerUpdater(this);
      }
      public synchronized void inputBytes(long numBytes) {
        this.numBytes += numBytes;
      }
      public synchronized void failedFetch() {
        ++numFailedFetches;
      }
      public synchronized void successFetch() {
        ++numSuccessFetches;
      }
      public synchronized void threadBusy() {
        ++numThreadsBusy;
      }
      public synchronized void threadFree() {
        --numThreadsBusy;
      }
      public void doUpdates(MetricsContext unused) {
        synchronized (this) {
          shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
          shuffleMetrics.incrMetric("shuffle_failed_fetches", 
                                    numFailedFetches);
          shuffleMetrics.incrMetric("shuffle_success_fetches", 
                                    numSuccessFetches);
          if (numCopiers != 0) {
            shuffleMetrics.setMetric("shuffle_fetchers_busy_percent",
                100*((float)numThreadsBusy/numCopiers));
          } else {
            shuffleMetrics.setMetric("shuffle_fetchers_busy_percent", 0);
          }
          numBytes = 0;
          numSuccessFetches = 0;
          numFailedFetches = 0;
        }
        shuffleMetrics.update();
      }
    }

    /** Represents the result of an attempt to copy a map output */
    private class CopyResult {
      
      // the map output location against which a copy attempt was made
      private final MapOutputLocation loc;
      
      // the size of the file copied, -1 if the transfer failed
      private final long size;
      
      //a flag signifying whether a copy result is obsolete
      private static final int OBSOLETE = -2;
      
      private CopyOutputErrorType error = CopyOutputErrorType.NO_ERROR;
      CopyResult(MapOutputLocation loc, long size) {
        this.loc = loc;
        this.size = size;
      }

      CopyResult(MapOutputLocation loc, long size, CopyOutputErrorType error) {
        this.loc = loc;
        this.size = size;
        this.error = error;
      }

      public boolean getSuccess() { return size >= 0; }
      public boolean isObsolete() { 
        return size == OBSOLETE;
      }
      public long getSize() { return size; }
      public String getHost() { return loc.getHost(); }
      public MapOutputLocation getLocation() { return loc; }
      public CopyOutputErrorType getError() { return error; }
    }
    
    private int nextMapOutputCopierId = 0;
    private boolean reportReadErrorImmediately;
    
    /**
     * Abstraction to track a map-output.
     */
    private class MapOutputLocation {
      TaskAttemptID taskAttemptId;
      TaskID taskId;
      String ttHost;
      URL taskOutput;
      
      public MapOutputLocation(TaskAttemptID taskAttemptId, 
                               String ttHost, URL taskOutput) {
        this.taskAttemptId = taskAttemptId;
        this.taskId = this.taskAttemptId.getTaskID();
        this.ttHost = ttHost;
        this.taskOutput = taskOutput;
      }
      
      public TaskAttemptID getTaskAttemptId() {
        return taskAttemptId;
      }
      
      public TaskID getTaskId() {
        return taskId;
      }
      
      public String getHost() {
        return ttHost;
      }
      
      public URL getOutputLocation() {
        return taskOutput;
      }
    }
    
    /** Describes the output of a map; could either be on disk or in-memory. */
    private class MapOutput {
      final TaskID mapId;
      final TaskAttemptID mapAttemptId;
      
      final Path file;
      final Configuration conf;
      
      byte[] data;
      final boolean inMemory;
      long compressedSize;
      
      public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId, 
                       Configuration conf, Path file, long size) {
        this.mapId = mapId;
        this.mapAttemptId = mapAttemptId;
        
        this.conf = conf;
        this.file = file;
        this.compressedSize = size;
        
        this.data = null;
        
        this.inMemory = false;
      }
      
      public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId, byte[] data, int compressedLength) {
        this.mapId = mapId;
        this.mapAttemptId = mapAttemptId;
        
        this.file = null;
        this.conf = null;
        
        this.data = data;
        this.compressedSize = compressedLength;
        
        this.inMemory = true;
      }
      
      public void discard() throws IOException {
        if (inMemory) {
          data = null;
        } else {
          FileSystem fs = file.getFileSystem(conf);
          fs.delete(file, true);
        }
      }
    }
    
    class ShuffleRamManager implements RamManager {
      /* Maximum percentage of the in-memory limit that a single shuffle can 
       * consume*/ 
      private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;
      
      /* Maximum percentage of shuffle-threads which can be stalled 
       * simultaneously after which a merge is triggered. */ 
      private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;
      
      private final long maxSize;
      private final long maxSingleShuffleLimit;
      
      private long size = 0;
      
      private Object dataAvailable = new Object();
      private long fullSize = 0;
      private int numPendingRequests = 0;
      private int numRequiredMapOutputs = 0;
      private int numClosed = 0;
      private boolean closed = false;
      
      public ShuffleRamManager(Configuration conf) throws IOException {
        final float maxInMemCopyUse =
          conf.getFloat("mapred.job.shuffle.input.buffer.percent", 0.70f);
        if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
          throw new IOException("mapred.job.shuffle.input.buffer.percent" +
                                maxInMemCopyUse);
        }
        // Allow unit tests to fix Runtime memory
        maxSize = (int)(conf.getInt("mapred.job.reduce.total.mem.bytes",
            (int)Math.min(Runtime.getRuntime().maxMemory(), Integer.MAX_VALUE))
          * maxInMemCopyUse);
        maxSingleShuffleLimit = (long)(maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
        LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize + 
                 ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
      }
      
      public synchronized boolean reserve(int requestedSize, InputStream in) 
      throws InterruptedException {
        // Wait till the request can be fulfilled...
        while ((size + requestedSize) > maxSize) {
          
          // Close the input...
          if (in != null) {
            try {
              in.close();
            } catch (IOException ie) {
              LOG.info("Failed to close connection with: " + ie);
            } finally {
              in = null;
            }
          } 

          // Track pending requests
          synchronized (dataAvailable) {
            ++numPendingRequests;
            dataAvailable.notify();
          }

          // Wait for memory to free up
          wait();
          
          // Track pending requests
          synchronized (dataAvailable) {
            --numPendingRequests;
          }
        }
        
        size += requestedSize;
        
        return (in != null);
      }
      
      public synchronized void unreserve(int requestedSize) {
        size -= requestedSize;
        
        synchronized (dataAvailable) {
          fullSize -= requestedSize;
          --numClosed;
        }
        
        // Notify the threads blocked on RamManager.reserve
        notifyAll();
      }
      
      public boolean waitForDataToMerge() throws InterruptedException {
        boolean done = false;
        synchronized (dataAvailable) {
                 // Start in-memory merge if manager has been closed or...
          while (!closed
                 &&
                 // In-memory threshold exceeded and at least two segments
                 // have been fetched
                 (getPercentUsed() < maxInMemCopyPer || numClosed < 2)
                 &&
                 // More than "mapred.inmem.merge.threshold" map outputs
                 // have been fetched into memory
                 (maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
                 && 
                 // More than MAX... threads are blocked on the RamManager
                 // or the blocked threads are the last map outputs to be
                 // fetched. If numRequiredMapOutputs is zero, either
                 // setNumCopiedMapOutputs has not been called (no map ouputs
                 // have been fetched, so there is nothing to merge) or the
                 // last map outputs being transferred without
                 // contention, so a merge would be premature.
                 (numPendingRequests < 
                      numCopiers*MAX_STALLED_SHUFFLE_THREADS_FRACTION && 
                  (0 == numRequiredMapOutputs ||
                   numPendingRequests < numRequiredMapOutputs))) {
            dataAvailable.wait();
          }
          done = closed;
        }
        return done;
      }
      
      public void closeInMemoryFile(int requestedSize) {
        synchronized (dataAvailable) {
          fullSize += requestedSize;
          ++numClosed;
          dataAvailable.notify();
        }
      }
      
      public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
        synchronized (dataAvailable) {
          this.numRequiredMapOutputs = numRequiredMapOutputs;
          dataAvailable.notify();
        }
      }
      
      public void close() {
        synchronized (dataAvailable) {
          closed = true;
          LOG.info("Closed ram manager");
          dataAvailable.notify();
        }
      }
      
      private float getPercentUsed() {
        return (float)fullSize/maxSize;
      }

      boolean canFitInMemory(long requestedSize) {
        return (requestedSize < Integer.MAX_VALUE && 
                requestedSize < maxSingleShuffleLimit);
      }
    }

    /** Copies map outputs as they become available */
    private class MapOutputCopier extends Thread {
      // basic/unit connection timeout (in milliseconds)
      private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
      // default read timeout (in milliseconds)
      private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;
      private final int shuffleConnectionTimeout;
      private final int shuffleReadTimeout;

      private MapOutputLocation currentLocation = null;
      private int id = nextMapOutputCopierId++;
      private Reporter reporter;
      private boolean readError = false;
      
      // Decompression of map-outputs
      private CompressionCodec codec = null;
      private Decompressor decompressor = null;
      
      private final SecretKey jobTokenSecret;
      
      public MapOutputCopier(JobConf job, Reporter reporter, SecretKey jobTokenSecret) {
        setName("MapOutputCopier " + reduceTask.getTaskID() + "." + id);
        LOG.debug(getName() + " created");
        this.reporter = reporter;

        this.jobTokenSecret = jobTokenSecret;
 
        shuffleConnectionTimeout =
          job.getInt("mapreduce.reduce.shuffle.connect.timeout", STALLED_COPY_TIMEOUT);
        shuffleReadTimeout =
          job.getInt("mapreduce.reduce.shuffle.read.timeout", DEFAULT_READ_TIMEOUT);
        
        if (job.getCompressMapOutput()) {
          Class<? extends CompressionCodec> codecClass =
            job.getMapOutputCompressorClass(DefaultCodec.class);
          codec = ReflectionUtils.newInstance(codecClass, job);
          decompressor = CodecPool.getDecompressor(codec);
        }
      }
      
      /**
       * Fail the current file that we are fetching
       * @return were we currently fetching?
       */
      public synchronized boolean fail() {
        if (currentLocation != null) {
          finish(-1, CopyOutputErrorType.OTHER_ERROR);
          return true;
        } else {
          return false;
        }
      }
      
      /**
       * Get the current map output location.
       */
      public synchronized MapOutputLocation getLocation() {
        return currentLocation;
      }
      
      private synchronized void start(MapOutputLocation loc) {
        currentLocation = loc;
      }
      
      private synchronized void finish(long size, CopyOutputErrorType error) {
        if (currentLocation != null) {
          LOG.debug(getName() + " finishing " + currentLocation + " =" + size);
          synchronized (copyResultsOrNewEventsLock) {
            copyResults.add(new CopyResult(currentLocation, size, error));
            copyResultsOrNewEventsLock.notifyAll();
          }
          currentLocation = null;
        }
      }
      
      /** Loop forever and fetch map outputs as they become available.
       * The thread exits when it is interrupted by {@link ReduceTaskRunner}
       */
      @Override
      public void run() {
        while (true) {        
          try {
            MapOutputLocation loc = null;
            long size = -1;
            
            synchronized (scheduledCopies) {
              while (scheduledCopies.isEmpty()) {
                scheduledCopies.wait();
              }
              loc = scheduledCopies.remove(0);
            }
            CopyOutputErrorType error = CopyOutputErrorType.OTHER_ERROR;
            readError = false;
            try {
              shuffleClientMetrics.threadBusy();
              start(loc);
              size = copyOutput(loc);
              shuffleClientMetrics.successFetch();
              error = CopyOutputErrorType.NO_ERROR;
            } catch (IOException e) {
              LOG.warn(reduceTask.getTaskID() + " copy failed: " +
                       loc.getTaskAttemptId() + " from " + loc.getHost());
              LOG.warn(StringUtils.stringifyException(e));
              shuffleClientMetrics.failedFetch();
              if (readError) {
                error = CopyOutputErrorType.READ_ERROR;
              }
              // Reset 
              size = -1;
            } finally {
              shuffleClientMetrics.threadFree();
              finish(size, error);
            }
          } catch (InterruptedException e) { 
            break; // ALL DONE
          } catch (FSError e) {
            LOG.error("Task: " + reduceTask.getTaskID() + " - FSError: " + 
                      StringUtils.stringifyException(e));
            try {
              umbilical.fsError(reduceTask.getTaskID(), e.getMessage(), jvmContext);
            } catch (IOException io) {
              LOG.error("Could not notify TT of FSError: " + 
                      StringUtils.stringifyException(io));
            }
          } catch (Throwable th) {
            String msg = getTaskID() + " : Map output copy failure : " 
                         + StringUtils.stringifyException(th);
            reportFatalError(getTaskID(), th, msg);
          }
        }
        
        if (decompressor != null) {
          CodecPool.returnDecompressor(decompressor);
        }
          
      }
      
      /** Copies a a map output from a remote host, via HTTP. 
       * @param currentLocation the map output location to be copied
       * @return the path (fully qualified) of the copied file
       * @throws IOException if there is an error copying the file
       * @throws InterruptedException if the copier should give up
       */
      private long copyOutput(MapOutputLocation loc
                              ) throws IOException, InterruptedException {
        // check if we still need to copy the output from this location
        if (copiedMapOutputs.contains(loc.getTaskId()) || 
            obsoleteMapIds.contains(loc.getTaskAttemptId())) {
          return CopyResult.OBSOLETE;
        } 
 
        // a temp filename. If this file gets created in ramfs, we're fine,
        // else, we will check the localFS to find a suitable final location
        // for this path
        TaskAttemptID reduceId = reduceTask.getTaskID();
        Path filename =
            new Path(String.format(
                MapOutputFile.REDUCE_INPUT_FILE_FORMAT_STRING,
                TaskTracker.OUTPUT, loc.getTaskId().getId()));

        // Copy the map output to a temp file whose name is unique to this attempt 
        Path tmpMapOutput = new Path(filename+"-"+id);
        
        // Copy the map output
        MapOutput mapOutput = getMapOutput(loc, tmpMapOutput,
                                           reduceId.getTaskID().getId());
        if (mapOutput == null) {
          throw new IOException("Failed to fetch map-output for " + 
                                loc.getTaskAttemptId() + " from " + 
                                loc.getHost());
        }
        
        // The size of the map-output
        long bytes = mapOutput.compressedSize;
        
        // lock the ReduceTask while we do the rename
        synchronized (ReduceTask.this) {
          if (copiedMapOutputs.contains(loc.getTaskId())) {
            mapOutput.discard();
            return CopyResult.OBSOLETE;
          }

          // Special case: discard empty map-outputs
          if (bytes == 0) {
            try {
              mapOutput.discard();
            } catch (IOException ioe) {
              LOG.info("Couldn't discard output of " + loc.getTaskId());
            }
            
            // Note that we successfully copied the map-output
            noteCopiedMapOutput(loc.getTaskId());
            
            return bytes;
          }
          
          // Process map-output
          if (mapOutput.inMemory) {
            // Save it in the synchronized list of map-outputs
            mapOutputsFilesInMemory.add(mapOutput);
          } else {
            // Rename the temporary file to the final file; 
            // ensure it is on the same partition
            tmpMapOutput = mapOutput.file;
            filename = new Path(tmpMapOutput.getParent(), filename.getName());
            if (!localFileSys.rename(tmpMapOutput, filename)) {
              localFileSys.delete(tmpMapOutput, true);
              bytes = -1;
              throw new IOException("Failed to rename map output " + 
                  tmpMapOutput + " to " + filename);
            }

            synchronized (mapOutputFilesOnDisk) {        
              addToMapOutputFilesOnDisk(localFileSys.getFileStatus(filename));
            }
          }

          // Note that we successfully copied the map-output
          noteCopiedMapOutput(loc.getTaskId());
        }
        
        return bytes;
      }
      
      /**
       * Save the map taskid whose output we just copied.
       * This function assumes that it has been synchronized on ReduceTask.this.
       * 
       * @param taskId map taskid
       */
      private void noteCopiedMapOutput(TaskID taskId) {
        copiedMapOutputs.add(taskId);
        ramManager.setNumCopiedMapOutputs(numMaps - copiedMapOutputs.size());
      }

      /**
       * Get the map output into a local file (either in the inmemory fs or on the 
       * local fs) from the remote server.
       * We use the file system so that we generate checksum files on the data.
       * @param mapOutputLoc map-output to be fetched
       * @param filename the filename to write the data into
       * @param connectionTimeout number of milliseconds for connection timeout
       * @param readTimeout number of milliseconds for read timeout
       * @return the path of the file that got created
       * @throws IOException when something goes wrong
       */
      private MapOutput getMapOutput(MapOutputLocation mapOutputLoc, 
                                     Path filename, int reduce)
      throws IOException, InterruptedException {
        // Connect
        URL url = mapOutputLoc.getOutputLocation();
        URLConnection connection = url.openConnection();
        
        InputStream input = setupSecureConnection(mapOutputLoc, connection);
 
        // Validate header from map output
        TaskAttemptID mapId = null;
        try {
          mapId =
            TaskAttemptID.forName(connection.getHeaderField(FROM_MAP_TASK));
        } catch (IllegalArgumentException ia) {
          LOG.warn("Invalid map id ", ia);
          return null;
        }
        TaskAttemptID expectedMapId = mapOutputLoc.getTaskAttemptId();
        if (!mapId.equals(expectedMapId)) {
          LOG.warn("data from wrong map:" + mapId +
              " arrived to reduce task " + reduce +
              ", where as expected map output should be from " + expectedMapId);
          return null;
        }
        
        long decompressedLength = 
          Long.parseLong(connection.getHeaderField(RAW_MAP_OUTPUT_LENGTH));  
        long compressedLength = 
          Long.parseLong(connection.getHeaderField(MAP_OUTPUT_LENGTH));

        if (compressedLength < 0 || decompressedLength < 0) {
          LOG.warn(getName() + " invalid lengths in map output header: id: " +
              mapId + " compressed len: " + compressedLength +
              ", decompressed len: " + decompressedLength);
          return null;
        }
        int forReduce =
          (int)Integer.parseInt(connection.getHeaderField(FOR_REDUCE_TASK));
        
        if (forReduce != reduce) {
          LOG.warn("data for the wrong reduce: " + forReduce +
              " with compressed len: " + compressedLength +
              ", decompressed len: " + decompressedLength +
              " arrived to reduce task " + reduce);
          return null;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("header: " + mapId + ", compressed len: " + compressedLength +
              ", decompressed len: " + decompressedLength);
        }

        //We will put a file in memory if it meets certain criteria:
        //1. The size of the (decompressed) file should be less than 25% of 
        //    the total inmem fs
        //2. There is space available in the inmem fs
        
        // Check if this map-output can be saved in-memory
        boolean shuffleInMemory = ramManager.canFitInMemory(decompressedLength); 

        // Shuffle
        MapOutput mapOutput = null;
        if (shuffleInMemory) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Shuffling " + decompressedLength + " bytes (" + 
                compressedLength + " raw bytes) " + 
                "into RAM from " + mapOutputLoc.getTaskAttemptId());
          }

          mapOutput = shuffleInMemory(mapOutputLoc, connection, input,
                                      (int)decompressedLength,
                                      (int)compressedLength);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Shuffling " + decompressedLength + " bytes (" + 
                compressedLength + " raw bytes) " + 
                "into Local-FS from " + mapOutputLoc.getTaskAttemptId());
          }
          
          mapOutput = shuffleToDisk(mapOutputLoc, input, filename, 
              compressedLength);
        }
            
        return mapOutput;
      }
      
      private InputStream setupSecureConnection(MapOutputLocation mapOutputLoc, 
          URLConnection connection) throws IOException {

        // generate hash of the url
        String msgToEncode = 
          SecureShuffleUtils.buildMsgFrom(connection.getURL());
        String encHash = SecureShuffleUtils.hashFromString(msgToEncode, 
            jobTokenSecret);

        // put url hash into http header
        connection.setRequestProperty(
            SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
        
        InputStream input = getInputStream(connection, shuffleConnectionTimeout,
                                           shuffleReadTimeout); 

        // get the replyHash which is HMac of the encHash we sent to the server
        String replyHash = connection.getHeaderField(
            SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
        if(replyHash==null) {
          throw new IOException("security validation of TT Map output failed");
        }
        if (LOG.isDebugEnabled())
          LOG.debug("url="+msgToEncode+";encHash="+encHash+";replyHash="
              +replyHash);
        // verify that replyHash is HMac of encHash
        SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecret);
        if (LOG.isDebugEnabled())
          LOG.debug("for url="+msgToEncode+" sent hash and receievd reply");
        return input;
      }

      /** 
       * The connection establishment is attempted multiple times and is given up 
       * only on the last failure. Instead of connecting with a timeout of 
       * X, we try connecting with a timeout of x < X but multiple times. 
       */
      private InputStream getInputStream(URLConnection connection, 
                                         int connectionTimeout, 
                                         int readTimeout) 
      throws IOException {
        int unit = 0;
        if (connectionTimeout < 0) {
          throw new IOException("Invalid timeout "
                                + "[timeout = " + connectionTimeout + " ms]");
        } else if (connectionTimeout > 0) {
          unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout)
                 ? connectionTimeout
                 : UNIT_CONNECT_TIMEOUT;
        }
        // set the read timeout to the total timeout
        connection.setReadTimeout(readTimeout);
        // set the connect timeout to the unit-connect-timeout
        connection.setConnectTimeout(unit);
        while (true) {
          try {
            connection.connect();
            break;
          } catch (IOException ioe) {
            // update the total remaining connect-timeout
            connectionTimeout -= unit;

            // throw an exception if we have waited for timeout amount of time
            // note that the updated value if timeout is used here
            if (connectionTimeout == 0) {
              throw ioe;
            }

            // reset the connect timeout for the last try
            if (connectionTimeout < unit) {
              unit = connectionTimeout;
              // reset the connect time out for the final connect
              connection.setConnectTimeout(unit);
            }
          }
        }
        try {
          return connection.getInputStream();
        } catch (IOException ioe) {
          readError = true;
          throw ioe;
        }
      }

      private MapOutput shuffleInMemory(MapOutputLocation mapOutputLoc,
                                        URLConnection connection, 
                                        InputStream input,
                                        int mapOutputLength,
                                        int compressedLength)
      throws IOException, InterruptedException {
        // Reserve ram for the map-output
        boolean createdNow = ramManager.reserve(mapOutputLength, input);
      
        // Reconnect if we need to
        if (!createdNow) {
          // Reconnect
          try {
            connection = mapOutputLoc.getOutputLocation().openConnection();
            input = setupSecureConnection(mapOutputLoc, connection);
          } catch (IOException ioe) {
            LOG.info("Failed reopen connection to fetch map-output from " + 
                     mapOutputLoc.getHost());
            
            // Inform the ram-manager
            ramManager.closeInMemoryFile(mapOutputLength);
            ramManager.unreserve(mapOutputLength);
            
            throw ioe;
          }
        }

        IFileInputStream checksumIn = 
          new IFileInputStream(input,compressedLength);

        input = checksumIn;       
      
        // Are map-outputs compressed?
        if (codec != null) {
          decompressor.reset();
          input = codec.createInputStream(input, decompressor);
        }
      
        // Copy map-output into an in-memory buffer
        byte[] shuffleData = new byte[mapOutputLength];
        MapOutput mapOutput = 
          new MapOutput(mapOutputLoc.getTaskId(), 
                        mapOutputLoc.getTaskAttemptId(), shuffleData, compressedLength);
        
        int bytesRead = 0;
        try {
          int n = input.read(shuffleData, 0, shuffleData.length);
          while (n > 0) {
            bytesRead += n;
            shuffleClientMetrics.inputBytes(n);

            // indicate we're making progress
            reporter.progress();
            n = input.read(shuffleData, bytesRead, 
                           (shuffleData.length-bytesRead));
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("Read " + bytesRead + " bytes from map-output for " +
                mapOutputLoc.getTaskAttemptId());
          }

          input.close();
        } catch (IOException ioe) {
          LOG.info("Failed to shuffle from " + mapOutputLoc.getTaskAttemptId(), 
                   ioe);

          // Inform the ram-manager
          ramManager.closeInMemoryFile(mapOutputLength);
          ramManager.unreserve(mapOutputLength);
          
          // Discard the map-output
          try {
            mapOutput.discard();
          } catch (IOException ignored) {
            LOG.info("Failed to discard map-output from " + 
                     mapOutputLoc.getTaskAttemptId(), ignored);
          }
          mapOutput = null;
          
          // Close the streams
          IOUtils.cleanup(LOG, input);

          // Re-throw
          readError = true;
          throw ioe;
        }

        // Close the in-memory file
        ramManager.closeInMemoryFile(mapOutputLength);

        // Sanity check
        if (bytesRead != mapOutputLength) {
          // Inform the ram-manager
          ramManager.unreserve(mapOutputLength);
          
          // Discard the map-output
          try {
            mapOutput.discard();
          } catch (IOException ignored) {
            // IGNORED because we are cleaning up
            LOG.info("Failed to discard map-output from " + 
                     mapOutputLoc.getTaskAttemptId(), ignored);
          }
          mapOutput = null;

          throw new IOException("Incomplete map output received for " +
                                mapOutputLoc.getTaskAttemptId() + " from " +
                                mapOutputLoc.getOutputLocation() + " (" + 
                                bytesRead + " instead of " + 
                                mapOutputLength + ")"
          );
        }

        // TODO: Remove this after a 'fix' for HADOOP-3647
        if (LOG.isDebugEnabled()) {
          if (mapOutputLength > 0) {
            DataInputBuffer dib = new DataInputBuffer();
            dib.reset(shuffleData, 0, shuffleData.length);
            LOG.debug("Rec #1 from " + mapOutputLoc.getTaskAttemptId() + 
                " -> (" + WritableUtils.readVInt(dib) + ", " + 
                WritableUtils.readVInt(dib) + ") from " + 
                mapOutputLoc.getHost());
          }
        }
        
        return mapOutput;
      }
      
      private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
                                      InputStream input,
                                      Path filename,
                                      long mapOutputLength) 
      throws IOException {
        // Find out a suitable location for the output on local-filesystem
        Path localFilename = 
          lDirAlloc.getLocalPathForWrite(filename.toUri().getPath(), 
                                         mapOutputLength, conf);

        MapOutput mapOutput = 
          new MapOutput(mapOutputLoc.getTaskId(), mapOutputLoc.getTaskAttemptId(), 
                        conf, localFileSys.makeQualified(localFilename), 
                        mapOutputLength);


        // Copy data to local-disk
        OutputStream output = null;
        long bytesRead = 0;
        try {
          output = rfs.create(localFilename);
          
          byte[] buf = new byte[64 * 1024];
          int n = -1;
          try {
            n = input.read(buf, 0, buf.length);
          } catch (IOException ioe) {
            readError = true;
            throw ioe;
          }
          while (n > 0) {
            bytesRead += n;
            shuffleClientMetrics.inputBytes(n);
            output.write(buf, 0, n);

            // indicate we're making progress
            reporter.progress();
            try {
              n = input.read(buf, 0, buf.length);
            } catch (IOException ioe) {
              readError = true;
              throw ioe;
            }
          }

          LOG.info("Read " + bytesRead + " bytes from map-output for " +
              mapOutputLoc.getTaskAttemptId());

          output.close();
          input.close();
        } catch (IOException ioe) {
          LOG.info("Failed to shuffle from " + mapOutputLoc.getTaskAttemptId(), 
                   ioe);

          // Discard the map-output
          try {
            mapOutput.discard();
          } catch (IOException ignored) {
            LOG.info("Failed to discard map-output from " + 
                mapOutputLoc.getTaskAttemptId(), ignored);
          }
          mapOutput = null;

          // Close the streams
          IOUtils.cleanup(LOG, input, output);

          // Re-throw
          throw ioe;
        }

        // Sanity check
        if (bytesRead != mapOutputLength) {
          try {
            mapOutput.discard();
          } catch (Exception ioe) {
            // IGNORED because we are cleaning up
            LOG.info("Failed to discard map-output from " + 
                mapOutputLoc.getTaskAttemptId(), ioe);
          } catch (Throwable t) {
            String msg = getTaskID() + " : Failed in shuffle to disk :" 
                         + StringUtils.stringifyException(t);
            reportFatalError(getTaskID(), t, msg);
          }
          mapOutput = null;

          throw new IOException("Incomplete map output received for " +
                                mapOutputLoc.getTaskAttemptId() + " from " +
                                mapOutputLoc.getOutputLocation() + " (" + 
                                bytesRead + " instead of " + 
                                mapOutputLength + ")"
          );
        }

        return mapOutput;

      }
      
    } // MapOutputCopier
    
    private void configureClasspath(JobConf conf)
      throws IOException {
      
      // get the task and the current classloader which will become the parent
      Task task = ReduceTask.this;
      ClassLoader parent = conf.getClassLoader();   
      
      // get the work directory which holds the elements we are dynamically
      // adding to the classpath
      File workDir = new File(task.getJobFile()).getParentFile();
      ArrayList<URL> urllist = new ArrayList<URL>();
      
      // add the jars and directories to the classpath
      String jar = conf.getJar();
      if (jar != null) {      
        File jobCacheDir = new File(new Path(jar).getParent().toString());

        File[] libs = new File(jobCacheDir, "lib").listFiles();
        if (libs != null) {
          for (int i = 0; i < libs.length; i++) {
            urllist.add(libs[i].toURL());
          }
        }
        urllist.add(new File(jobCacheDir, "classes").toURL());
        urllist.add(jobCacheDir.toURL());
        
      }
      urllist.add(workDir.toURL());
      
      // create a new classloader with the old classloader as its parent
      // then set that classloader as the one used by the current jobconf
      URL[] urls = urllist.toArray(new URL[urllist.size()]);
      URLClassLoader loader = new URLClassLoader(urls, parent);
      conf.setClassLoader(loader);
    }
    
    public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
                        TaskReporter reporter
                        )throws ClassNotFoundException, IOException {
      
      configureClasspath(conf);
      this.reporter = reporter;
      this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
      this.umbilical = umbilical;      
      this.reduceTask = ReduceTask.this;

      this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
      this.copyResults = new ArrayList<CopyResult>(100);    
      this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
      this.maxInFlight = 4 * numCopiers;
      Counters.Counter combineInputCounter = 
        reporter.getCounter(Task.Counter.COMBINE_INPUT_RECORDS);
      this.combinerRunner = CombinerRunner.create(conf, getTaskID(),
                                                  combineInputCounter,
                                                  reporter, null);
      if (combinerRunner != null) {
        combineCollector = 
          new CombineOutputCollector(reduceCombineOutputCounter);
      }
      
      this.ioSortFactor = conf.getInt("io.sort.factor", 10);

      this.abortFailureLimit = Math.max(30, numMaps / 10);

      this.maxFetchFailuresBeforeReporting = conf.getInt(
          "mapreduce.reduce.shuffle.maxfetchfailures", REPORT_FAILURE_LIMIT);

      this.maxFailedUniqueFetches = Math.min(numMaps, 
                                             this.maxFailedUniqueFetches);
      this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold", 1000);
      this.maxInMemCopyPer =
        conf.getFloat("mapred.job.shuffle.merge.percent", 0.66f);
      final float maxRedPer =
        conf.getFloat("mapred.job.reduce.input.buffer.percent", 0f);
      if (maxRedPer > 1.0 || maxRedPer < 0.0) {
        throw new IOException("mapred.job.reduce.input.buffer.percent" +
                              maxRedPer);
      }
      this.maxInMemReduce = (int)Math.min(
          Runtime.getRuntime().maxMemory() * maxRedPer, Integer.MAX_VALUE);

      // Setup the RamManager
      ramManager = new ShuffleRamManager(conf);

      localFileSys = FileSystem.getLocal(conf);

      rfs = ((LocalFileSystem)localFileSys).getRaw();

      // hosts -> next contact time
      this.penaltyBox = new LinkedHashMap<String, Long>();
      
      // hostnames
      this.uniqueHosts = new HashSet<String>();
      
      // Seed the random number generator with a reasonably globally unique seed
      long randomSeed = System.nanoTime() + 
                        (long)Math.pow(this.reduceTask.getPartition(),
                                       (this.reduceTask.getPartition()%10)
                                      );
      this.random = new Random(randomSeed);
      this.maxMapRuntime = 0;
      this.reportReadErrorImmediately = 
        conf.getBoolean("mapreduce.reduce.shuffle.notify.readerror", true);
    }
    
    private boolean busyEnough(int numInFlight) {
      return numInFlight > maxInFlight;
    }
    
    
    public boolean fetchOutputs() throws IOException {
      int totalFailures = 0;
      int            numInFlight = 0, numCopied = 0;
      DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
      final Progress copyPhase = 
        reduceTask.getProgress().phase();
      LocalFSMerger localFSMergerThread = null;
      InMemFSMergeThread inMemFSMergeThread = null;
      GetMapEventsThread getMapEventsThread = null;
      
      for (int i = 0; i < numMaps; i++) {
        copyPhase.addPhase();       // add sub-phase per file
      }
      
      copiers = new ArrayList<MapOutputCopier>(numCopiers);
      
      // start all the copying threads
      for (int i=0; i < numCopiers; i++) {
        MapOutputCopier copier = new MapOutputCopier(conf, reporter, 
            reduceTask.getJobTokenSecret());
        copiers.add(copier);
        copier.start();
      }
      
      //start the on-disk-merge thread
      localFSMergerThread = new LocalFSMerger((LocalFileSystem)localFileSys);
      //start the in memory merger thread
      inMemFSMergeThread = new InMemFSMergeThread();
      localFSMergerThread.start();
      inMemFSMergeThread.start();
      
      // start the map events thread
      getMapEventsThread = new GetMapEventsThread();
      getMapEventsThread.start();
      
      // start the clock for bandwidth measurement
      long startTime = System.currentTimeMillis();
      long currentTime = startTime;
      long lastProgressTime = startTime;
      long lastOutputTime = 0;
      
        // loop until we get all required outputs
        while (copiedMapOutputs.size() < numMaps && mergeThrowable == null) {
          int numEventsAtStartOfScheduling;
          synchronized (copyResultsOrNewEventsLock) {
            numEventsAtStartOfScheduling = numEventsFetched;
          }
          
          currentTime = System.currentTimeMillis();
          boolean logNow = false;
          if (currentTime - lastOutputTime > MIN_LOG_TIME) {
            lastOutputTime = currentTime;
            logNow = true;
          }
          if (logNow) {
            LOG.info(reduceTask.getTaskID() + " Need another " 
                   + (numMaps - copiedMapOutputs.size()) + " map output(s) "
                   + "where " + numInFlight + " is already in progress");
          }

          // Put the hash entries for the failed fetches.
          Iterator<MapOutputLocation> locItr = retryFetches.iterator();

          while (locItr.hasNext()) {
            MapOutputLocation loc = locItr.next(); 
            List<MapOutputLocation> locList = 
              mapLocations.get(loc.getHost());
            
            // Check if the list exists. Map output location mapping is cleared 
            // once the jobtracker restarts and is rebuilt from scratch.
            // Note that map-output-location mapping will be recreated and hence
            // we continue with the hope that we might find some locations
            // from the rebuild map.
            if (locList != null) {
              // Add to the beginning of the list so that this map is 
              //tried again before the others and we can hasten the 
              //re-execution of this map should there be a problem
              locList.add(0, loc);
            }
          }

          if (retryFetches.size() > 0) {
            LOG.info(reduceTask.getTaskID() + ": " +  
                  "Got " + retryFetches.size() +
                  " map-outputs from previous failures");
          }
          // clear the "failed" fetches hashmap
          retryFetches.clear();

          // now walk through the cache and schedule what we can
          int numScheduled = 0;
          int numDups = 0;
          
          synchronized (scheduledCopies) {
  
            // Randomize the map output locations to prevent 
            // all reduce-tasks swamping the same tasktracker
            List<String> hostList = new ArrayList<String>();
            hostList.addAll(mapLocations.keySet()); 
            
            Collections.shuffle(hostList, this.random);
              
            Iterator<String> hostsItr = hostList.iterator();

            while (hostsItr.hasNext()) {
            
              String host = hostsItr.next();

              List<MapOutputLocation> knownOutputsByLoc = 
                mapLocations.get(host);

              // Check if the list exists. Map output location mapping is 
              // cleared once the jobtracker restarts and is rebuilt from 
              // scratch.
              // Note that map-output-location mapping will be recreated and 
              // hence we continue with the hope that we might find some 
              // locations from the rebuild map and add then for fetching.
              if (knownOutputsByLoc == null || knownOutputsByLoc.size() == 0) {
                continue;
              }
              
              //Identify duplicate hosts here
              if (uniqueHosts.contains(host)) {
                 numDups += knownOutputsByLoc.size(); 
                 continue;
              }

              Long penaltyEnd = penaltyBox.get(host);
              boolean penalized = false;
            
              if (penaltyEnd != null) {
                if (currentTime < penaltyEnd.longValue()) {
                  penalized = true;
                } else {
                  penaltyBox.remove(host);
                }
              }
              
              if (penalized)
                continue;

              synchronized (knownOutputsByLoc) {
              
                locItr = knownOutputsByLoc.iterator();
            
                while (locItr.hasNext()) {
              
                  MapOutputLocation loc = locItr.next();
              
                  // Do not schedule fetches from OBSOLETE maps
                  if (obsoleteMapIds.contains(loc.getTaskAttemptId())) {
                    locItr.remove();
                    continue;
                  }

                  uniqueHosts.add(host);
                  scheduledCopies.add(loc);
                  locItr.remove();  // remove from knownOutputs
                  numInFlight++; numScheduled++;

                  break; //we have a map from this host
                }
              }
            }
            scheduledCopies.notifyAll();
          }

          if (numScheduled > 0 || logNow) {
            LOG.info(reduceTask.getTaskID() + " Scheduled " + numScheduled +
                   " outputs (" + penaltyBox.size() +
                   " slow hosts and" + numDups + " dup hosts)");
          }

          if (penaltyBox.size() > 0 && logNow) {
            LOG.info("Penalized(slow) Hosts: ");
            for (String host : penaltyBox.keySet()) {
              LOG.info(host + " Will be considered after: " + 
                  ((penaltyBox.get(host) - currentTime)/1000) + " seconds.");
            }
          }

          // if we have no copies in flight and we can't schedule anything
          // new, just wait for a bit
          try {
            if (numInFlight == 0 && numScheduled == 0) {
              // we should indicate progress as we don't want TT to think
              // we're stuck and kill us
              reporter.progress();
              Thread.sleep(5000);
            }
          } catch (InterruptedException e) { } // IGNORE
          
          while (numInFlight > 0 && mergeThrowable == null) {
            LOG.debug(reduceTask.getTaskID() + " numInFlight = " + 
                      numInFlight);
            //the call to getCopyResult will either 
            //1) return immediately with a null or a valid CopyResult object,
            //                 or
            //2) if the numInFlight is above maxInFlight, return with a 
            //   CopyResult object after getting a notification from a 
            //   fetcher thread, 
            //So, when getCopyResult returns null, we can be sure that
            //we aren't busy enough and we should go and get more mapcompletion
            //events from the tasktracker
            CopyResult cr = getCopyResult(numInFlight, numEventsAtStartOfScheduling);

            if (cr == null) {
              break;
            }
            
            if (cr.getSuccess()) {  // a successful copy
              numCopied++;
              lastProgressTime = System.currentTimeMillis();
              reduceShuffleBytes.increment(cr.getSize());
                
              long secsSinceStart = 
                (System.currentTimeMillis()-startTime)/1000+1;
              float mbs = ((float)reduceShuffleBytes.getCounter())/(1024*1024);
              float transferRate = mbs/secsSinceStart;
                
              copyPhase.startNextPhase();
              copyPhase.setStatus("copy (" + numCopied + " of " + numMaps 
                                  + " at " +
                                  mbpsFormat.format(transferRate) +  " MB/s)");
                
              // Note successful fetch for this mapId to invalidate
              // (possibly) old fetch-failures
              fetchFailedMaps.remove(cr.getLocation().getTaskId());
            } else if (cr.isObsolete()) {
              //ignore
              LOG.info(reduceTask.getTaskID() + 
                       " Ignoring obsolete copy result for Map Task: " + 
                       cr.getLocation().getTaskAttemptId() + " from host: " + 
                       cr.getHost());
            } else {
              retryFetches.add(cr.getLocation());
              
              // note the failed-fetch
              TaskAttemptID mapTaskId = cr.getLocation().getTaskAttemptId();
              TaskID mapId = cr.getLocation().getTaskId();
              
              totalFailures++;
              Integer noFailedFetches = 
                mapTaskToFailedFetchesMap.get(mapTaskId);
              noFailedFetches = 
                (noFailedFetches == null) ? 1 : (noFailedFetches + 1);
              mapTaskToFailedFetchesMap.put(mapTaskId, noFailedFetches);
              LOG.info("Task " + getTaskID() + ": Failed fetch #" + 
                       noFailedFetches + " from " + mapTaskId);

              if (noFailedFetches >= abortFailureLimit) {
                LOG.fatal(noFailedFetches + " failures downloading "
                          + getTaskID() + ".");
                umbilical.shuffleError(getTaskID(),
                                 "Exceeded the abort failure limit;"
                                 + " bailing-out.", jvmContext);
              }
              
              checkAndInformJobTracker(noFailedFetches, mapTaskId,
                  cr.getError().equals(CopyOutputErrorType.READ_ERROR));

              // note unique failed-fetch maps
              if (noFailedFetches == maxFetchFailuresBeforeReporting) {
                fetchFailedMaps.add(mapId);
                  
                // did we have too many unique failed-fetch maps?
                // and did we fail on too many fetch attempts?
                // and did we progress enough
                //     or did we wait for too long without any progress?
               
                // check if the reducer is healthy
                boolean reducerHealthy = 
                    (((float)totalFailures / (totalFailures + numCopied)) 
                     < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
                
                // check if the reducer has progressed enough
                boolean reducerProgressedEnough = 
                    (((float)numCopied / numMaps) 
                     >= MIN_REQUIRED_PROGRESS_PERCENT);
                
                // check if the reducer is stalled for a long time
                // duration for which the reducer is stalled
                int stallDuration = 
                    (int)(System.currentTimeMillis() - lastProgressTime);
                // duration for which the reducer ran with progress
                int shuffleProgressDuration = 
                    (int)(lastProgressTime - startTime);
                // min time the reducer should run without getting killed
                int minShuffleRunDuration = 
                    (shuffleProgressDuration > maxMapRuntime) 
                    ? shuffleProgressDuration 
                    : maxMapRuntime;
                boolean reducerStalled = 
                    (((float)stallDuration / minShuffleRunDuration) 
                     >= MAX_ALLOWED_STALL_TIME_PERCENT);
                
                // kill if not healthy and has insufficient progress
                if ((fetchFailedMaps.size() >= maxFailedUniqueFetches ||
                     fetchFailedMaps.size() == (numMaps - copiedMapOutputs.size()))
                    && !reducerHealthy 
                    && (!reducerProgressedEnough || reducerStalled)) { 
                  LOG.fatal("Shuffle failed with too many fetch failures " + 
                            "and insufficient progress!" +
                            "Killing task " + getTaskID() + ".");
                  umbilical.shuffleError(getTaskID(), 
                                         "Exceeded MAX_FAILED_UNIQUE_FETCHES;"
                                         + " bailing-out.", jvmContext);
                }

              }
                
              currentTime = System.currentTimeMillis();
              long currentBackOff = (long)(INITIAL_PENALTY *
                  Math.pow(PENALTY_GROWTH_RATE, noFailedFetches));

              penaltyBox.put(cr.getHost(), currentTime + currentBackOff);
              LOG.warn(reduceTask.getTaskID() + " adding host " +
                       cr.getHost() + " to penalty box, next contact in " +
                       (currentBackOff/1000) + " seconds");
            }
            uniqueHosts.remove(cr.getHost());
            numInFlight--;
          }
        }
        
        // all done, inform the copiers to exit
        exitGetMapEvents= true;
        try {
          getMapEventsThread.join();
          LOG.info("getMapsEventsThread joined.");
        } catch (InterruptedException ie) {
          LOG.info("getMapsEventsThread threw an exception: " +
              StringUtils.stringifyException(ie));
        }

        synchronized (copiers) {
          synchronized (scheduledCopies) {
            for (MapOutputCopier copier : copiers) {
              copier.interrupt();
            }
            copiers.clear();
          }
        }
        
        // copiers are done, exit and notify the waiting merge threads
        synchronized (mapOutputFilesOnDisk) {
          exitLocalFSMerge = true;
          mapOutputFilesOnDisk.notify();
        }
        
        ramManager.close();
        
        //Do a merge of in-memory files (if there are any)
        if (mergeThrowable == null) {
          try {
            // Wait for the on-disk merge to complete
            localFSMergerThread.join();
            LOG.info("Interleaved on-disk merge complete: " + 
                     mapOutputFilesOnDisk.size() + " files left.");
            
            //wait for an ongoing merge (if it is in flight) to complete
            inMemFSMergeThread.join();
            LOG.info("In-memory merge complete: " + 
                     mapOutputsFilesInMemory.size() + " files left.");
            } catch (InterruptedException ie) {
            LOG.warn(reduceTask.getTaskID() +
                     " Final merge of the inmemory files threw an exception: " + 
                     StringUtils.stringifyException(ie));
            // check if the last merge generated an error
            if (mergeThrowable != null) {
              mergeThrowable = ie;
            }
            return false;
          }
        }
        return mergeThrowable == null && copiedMapOutputs.size() == numMaps;
    }
    
    // Notify the JobTracker
    // after every read error, if 'reportReadErrorImmediately' is true or
    // after every 'maxFetchFailuresBeforeReporting' failures
    protected void checkAndInformJobTracker(
        int failures, TaskAttemptID mapId, boolean readError) {
      if ((reportReadErrorImmediately && readError)
          || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
        synchronized (ReduceTask.this) {
          taskStatus.addFetchFailedMap(mapId);
          reporter.progress();
          LOG.info("Failed to fetch map-output from " + mapId +
                   " even after MAX_FETCH_RETRIES_PER_MAP retries... "
                   + " or it is a read error, "
                   + " reporting to the JobTracker");
        }
      }
    }



    private long createInMemorySegments(
        List<Segment<K, V>> inMemorySegments, long leaveBytes)
        throws IOException {
      long totalSize = 0L;
      synchronized (mapOutputsFilesInMemory) {
        // fullSize could come from the RamManager, but files can be
        // closed but not yet present in mapOutputsFilesInMemory
        long fullSize = 0L;
        for (MapOutput mo : mapOutputsFilesInMemory) {
          fullSize += mo.data.length;
        }
        while(fullSize > leaveBytes) {
          MapOutput mo = mapOutputsFilesInMemory.remove(0);
          totalSize += mo.data.length;
          fullSize -= mo.data.length;
          Reader<K, V> reader = 
            new InMemoryReader<K, V>(ramManager, mo.mapAttemptId,
                                     mo.data, 0, mo.data.length);
          Segment<K, V> segment = 
            new Segment<K, V>(reader, true);
          inMemorySegments.add(segment);
        }
      }
      return totalSize;
    }

    /**
     * Create a RawKeyValueIterator from copied map outputs. All copying
     * threads have exited, so all of the map outputs are available either in
     * memory or on disk. We also know that no merges are in progress, so
     * synchronization is more lax, here.
     *
     * The iterator returned must satisfy the following constraints:
     *   1. Fewer than io.sort.factor files may be sources
     *   2. No more than maxInMemReduce bytes of map outputs may be resident
     *      in memory when the reduce begins
     *
     * If we must perform an intermediate merge to satisfy (1), then we can
     * keep the excluded outputs from (2) in memory and include them in the
     * first merge pass. If not, then said outputs must be written to disk
     * first.
     */
    @SuppressWarnings("unchecked")
    private RawKeyValueIterator createKVIterator(
        JobConf job, FileSystem fs, Reporter reporter) throws IOException {

      // merge config params
      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
      Class<V> valueClass = (Class<V>)job.getMapOutputValueClass();
      boolean keepInputs = job.getKeepFailedTaskFiles();
      final Path tmpDir = new Path(getTaskID().toString());
      final RawComparator<K> comparator =
        (RawComparator<K>)job.getOutputKeyComparator();

      // segments required to vacate memory
      List<Segment<K,V>> memDiskSegments = new ArrayList<Segment<K,V>>();
      long inMemToDiskBytes = 0;
      if (mapOutputsFilesInMemory.size() > 0) {
        TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
        inMemToDiskBytes = createInMemorySegments(memDiskSegments,
            maxInMemReduce);
        final int numMemDiskSegments = memDiskSegments.size();
        if (numMemDiskSegments > 0 &&
              ioSortFactor > mapOutputFilesOnDisk.size()) {
          // must spill to disk, but can't retain in-mem for intermediate merge
          final Path outputPath =
              mapOutputFile.getInputFileForWrite(mapId, inMemToDiskBytes);
          final RawKeyValueIterator rIter = Merger.merge(job, fs,
              keyClass, valueClass, memDiskSegments, numMemDiskSegments,
              tmpDir, comparator, reporter, spilledRecordsCounter, null);
          final Writer writer = new Writer(job, fs, outputPath,
              keyClass, valueClass, codec, null);
          try {
            Merger.writeFile(rIter, writer, reporter, job);
            addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
          } catch (Exception e) {
            if (null != outputPath) {
              fs.delete(outputPath, true);
            }
            throw new IOException("Final merge failed", e);
          } finally {
            if (null != writer) {
              writer.close();
            }
          }
          LOG.info("Merged " + numMemDiskSegments + " segments, " +
                   inMemToDiskBytes + " bytes to disk to satisfy " +
                   "reduce memory limit");
          inMemToDiskBytes = 0;
          memDiskSegments.clear();
        } else if (inMemToDiskBytes != 0) {
          LOG.info("Keeping " + numMemDiskSegments + " segments, " +
                   inMemToDiskBytes + " bytes in memory for " +
                   "intermediate, on-disk merge");
        }
      }

      // segments on disk
      List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
      long onDiskBytes = inMemToDiskBytes;
      Path[] onDisk = getMapFiles(fs, false);
      for (Path file : onDisk) {
        onDiskBytes += fs.getFileStatus(file).getLen();
        diskSegments.add(new Segment<K, V>(job, fs, file, codec, keepInputs));
      }
      LOG.info("Merging " + onDisk.length + " files, " +
               onDiskBytes + " bytes from disk");
      Collections.sort(diskSegments, new Comparator<Segment<K,V>>() {
        public int compare(Segment<K, V> o1, Segment<K, V> o2) {
          if (o1.getLength() == o2.getLength()) {
            return 0;
          }
          return o1.getLength() < o2.getLength() ? -1 : 1;
        }
      });

      // build final list of segments from merged backed by disk + in-mem
      List<Segment<K,V>> finalSegments = new ArrayList<Segment<K,V>>();
      long inMemBytes = createInMemorySegments(finalSegments, 0);
      LOG.info("Merging " + finalSegments.size() + " segments, " +
               inMemBytes + " bytes from memory into reduce");
      if (0 != onDiskBytes) {
        final int numInMemSegments = memDiskSegments.size();
        diskSegments.addAll(0, memDiskSegments);
        memDiskSegments.clear();
        RawKeyValueIterator diskMerge = Merger.merge(
            job, fs, keyClass, valueClass, codec, diskSegments,
            ioSortFactor, numInMemSegments, tmpDir, comparator,
            reporter, false, spilledRecordsCounter, null);
        diskSegments.clear();
        if (0 == finalSegments.size()) {
          return diskMerge;
        }
        finalSegments.add(new Segment<K,V>(
              new RawKVIteratorReader(diskMerge, onDiskBytes), true));
      }
      return Merger.merge(job, fs, keyClass, valueClass,
                   finalSegments, finalSegments.size(), tmpDir,
                   comparator, reporter, spilledRecordsCounter, null);
    }

    class RawKVIteratorReader extends IFile.Reader<K,V> {

      private final RawKeyValueIterator kvIter;

      public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
          throws IOException {
        super(null, null, size, null, spilledRecordsCounter);
        this.kvIter = kvIter;
      }

      public boolean next(DataInputBuffer key, DataInputBuffer value)
          throws IOException {
        if (kvIter.next()) {
          final DataInputBuffer kb = kvIter.getKey();
          final DataInputBuffer vb = kvIter.getValue();
          final int kp = kb.getPosition();
          final int klen = kb.getLength() - kp;
          key.reset(kb.getData(), kp, klen);
          final int vp = vb.getPosition();
          final int vlen = vb.getLength() - vp;
          value.reset(vb.getData(), vp, vlen);
          bytesRead += klen + vlen;
          return true;
        }
        return false;
      }

      public long getPosition() throws IOException {
        return bytesRead;
      }

      public void close() throws IOException {
        kvIter.close();
      }
    }

    private CopyResult getCopyResult(int numInFlight, int numEventsAtStartOfScheduling) {
      boolean waitedForNewEvents = false;
      
      synchronized (copyResultsOrNewEventsLock) {
        while (copyResults.isEmpty()) {
          try {
            //The idea is that if we have scheduled enough, we can wait until
            // we hear from one of the copiers, or until there are new
            // map events ready to be scheduled
            if (busyEnough(numInFlight)) {
              // All of the fetcher threads are busy. So, no sense trying
              // to schedule more until one finishes.
              copyResultsOrNewEventsLock.wait();
            } else if (numEventsFetched == numEventsAtStartOfScheduling &&
                       !waitedForNewEvents) {
              // no sense trying to schedule more, since there are no
              // new events to even try to schedule.
              // We could handle this with a normal wait() without a timeout,
              // but since this code is being introduced in a stable branch,
              // we want to be very conservative. A 2-second wait is enough
              // to prevent the busy-loop experienced before.
              waitedForNewEvents = true;
              copyResultsOrNewEventsLock.wait(2000);
            } else {
              return null;
            }
          } catch (InterruptedException e) { }
        }
        return copyResults.remove(0);
      }    
    }
    
    private void addToMapOutputFilesOnDisk(FileStatus status) {
      synchronized (mapOutputFilesOnDisk) {
        mapOutputFilesOnDisk.add(status);
        mapOutputFilesOnDisk.notify();
      }
    }
    
    
    
    /** Starts merging the local copy (on disk) of the map's output so that
     * most of the reducer's input is sorted i.e overlapping shuffle
     * and merge phases.
     */
    private class LocalFSMerger extends Thread {
      private LocalFileSystem localFileSys;

      public LocalFSMerger(LocalFileSystem fs) {
        this.localFileSys = fs;
        setName("Thread for merging on-disk files");
        setDaemon(true);
      }

      @SuppressWarnings("unchecked")
      public void run() {
        try {
          LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
          while(!exitLocalFSMerge){
            synchronized (mapOutputFilesOnDisk) {
              while (!exitLocalFSMerge &&
                  mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
                LOG.info(reduceTask.getTaskID() + " Thread waiting: " + getName());
                mapOutputFilesOnDisk.wait();
              }
            }
            if(exitLocalFSMerge) {//to avoid running one extra time in the end
              break;
            }
            List<Path> mapFiles = new ArrayList<Path>();
            long approxOutputSize = 0;
            int bytesPerSum = 
              reduceTask.getConf().getInt("io.bytes.per.checksum", 512);
            LOG.info(reduceTask.getTaskID() + "We have  " + 
                mapOutputFilesOnDisk.size() + " map outputs on disk. " +
                "Triggering merge of " + ioSortFactor + " files");
            // 1. Prepare the list of files to be merged. This list is prepared
            // using a list of map output files on disk. Currently we merge
            // io.sort.factor files into 1.
            synchronized (mapOutputFilesOnDisk) {
              for (int i = 0; i < ioSortFactor; ++i) {
                FileStatus filestatus = mapOutputFilesOnDisk.first();
                mapOutputFilesOnDisk.remove(filestatus);
                mapFiles.add(filestatus.getPath());
                approxOutputSize += filestatus.getLen();
              }
            }
            
            // sanity check
            if (mapFiles.size() == 0) {
                return;
            }
            
            // add the checksum length
            approxOutputSize += ChecksumFileSystem
                                .getChecksumLength(approxOutputSize,
                                                   bytesPerSum);
  
            // 2. Start the on-disk merge process
            Path outputPath = 
              lDirAlloc.getLocalPathForWrite(mapFiles.get(0).toString(), 
                                             approxOutputSize, conf)
              .suffix(".merged");
            Writer writer = 
              new Writer(conf,rfs, outputPath, 
                         conf.getMapOutputKeyClass(), 
                         conf.getMapOutputValueClass(),
                         codec, null);
            RawKeyValueIterator iter  = null;
            Path tmpDir = new Path(reduceTask.getTaskID().toString());
            try {
              iter = Merger.merge(conf, rfs,
                                  conf.getMapOutputKeyClass(),
                                  conf.getMapOutputValueClass(),
                                  codec, mapFiles.toArray(new Path[mapFiles.size()]), 
                                  true, ioSortFactor, tmpDir, 
                                  conf.getOutputKeyComparator(), reporter,
                                  spilledRecordsCounter, null);
              
              Merger.writeFile(iter, writer, reporter, conf);
              writer.close();
            } catch (Exception e) {
              localFileSys.delete(outputPath, true);
              throw new IOException (StringUtils.stringifyException(e));
            }
            
            synchronized (mapOutputFilesOnDisk) {
              addToMapOutputFilesOnDisk(localFileSys.getFileStatus(outputPath));
            }
            
            LOG.info(reduceTask.getTaskID() +
                     " Finished merging " + mapFiles.size() + 
                     " map output files on disk of total-size " + 
                     approxOutputSize + "." + 
                     " Local output file is " + outputPath + " of size " +
                     localFileSys.getFileStatus(outputPath).getLen());
            }
        } catch (Exception e) {
          LOG.warn(reduceTask.getTaskID()
                   + " Merging of the local FS files threw an exception: "
                   + StringUtils.stringifyException(e));
          if (mergeThrowable == null) {
            mergeThrowable = e;
          }
        } catch (Throwable t) {
          String msg = getTaskID() + " : Failed to merge on the local FS" 
                       + StringUtils.stringifyException(t);
          reportFatalError(getTaskID(), t, msg);
        }
      }
    }

    private class InMemFSMergeThread extends Thread {
      
      public InMemFSMergeThread() {
        setName("Thread for merging in memory files");
        setDaemon(true);
      }
      
      public void run() {
        LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
        try {
          boolean exit = false;
          do {
            exit = ramManager.waitForDataToMerge();
            if (!exit) {
              doInMemMerge();
            }
          } while (!exit);
        } catch (Exception e) {
          LOG.warn(reduceTask.getTaskID() +
                   " Merge of the inmemory files threw an exception: "
                   + StringUtils.stringifyException(e));
          ReduceCopier.this.mergeThrowable = e;
        } catch (Throwable t) {
          String msg = getTaskID() + " : Failed to merge in memory" 
                       + StringUtils.stringifyException(t);
          reportFatalError(getTaskID(), t, msg);
        }
      }
      
      @SuppressWarnings("unchecked")
      private void doInMemMerge() throws IOException{
        if (mapOutputsFilesInMemory.size() == 0) {
          return;
        }
        
        //name this output file same as the name of the first file that is 
        //there in the current list of inmem files (this is guaranteed to
        //be absent on the disk currently. So we don't overwrite a prev. 
        //created spill). Also we need to create the output file now since
        //it is not guaranteed that this file will be present after merge
        //is called (we delete empty files as soon as we see them
        //in the merge method)

        //figure out the mapId 
        TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;

        List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K,V>>();
        long mergeOutputSize = createInMemorySegments(inMemorySegments, 0);
        int noInMemorySegments = inMemorySegments.size();

        Path outputPath =
            mapOutputFile.getInputFileForWrite(mapId, mergeOutputSize);

        Writer writer = 
          new Writer(conf, rfs, outputPath,
                     conf.getMapOutputKeyClass(),
                     conf.getMapOutputValueClass(),
                     codec, null);

        RawKeyValueIterator rIter = null;
        try {
          LOG.info("Initiating in-memory merge with " + noInMemorySegments + 
                   " segments...");
          
          rIter = Merger.merge(conf, rfs,
                               (Class<K>)conf.getMapOutputKeyClass(),
                               (Class<V>)conf.getMapOutputValueClass(),
                               inMemorySegments, inMemorySegments.size(),
                               new Path(reduceTask.getTaskID().toString()),
                               conf.getOutputKeyComparator(), reporter,
                               spilledRecordsCounter, null);
          
          if (combinerRunner == null) {
            Merger.writeFile(rIter, writer, reporter, conf);
          } else {
            combineCollector.setWriter(writer);
            combinerRunner.combine(rIter, combineCollector);
          }
          writer.close();

          LOG.info(reduceTask.getTaskID() + 
              " Merge of the " + noInMemorySegments +
              " files in-memory complete." +
              " Local file is " + outputPath + " of size " + 
              localFileSys.getFileStatus(outputPath).getLen());
        } catch (Exception e) { 
          //make sure that we delete the ondisk file that we created 
          //earlier when we invoked cloneFileAttributes
          localFileSys.delete(outputPath, true);
          throw (IOException)new IOException
                  ("Intermediate merge failed").initCause(e);
        }

        // Note the output of the merge
        FileStatus status = localFileSys.getFileStatus(outputPath);
        synchronized (mapOutputFilesOnDisk) {
          addToMapOutputFilesOnDisk(status);
        }
      }
    }

    private class GetMapEventsThread extends Thread {
      
      private IntWritable fromEventId = new IntWritable(0);
      private static final long SLEEP_TIME = 1000;
      
      public GetMapEventsThread() {
        setName("Thread for polling Map Completion Events");
        setDaemon(true);
      }
      
      @Override
      public void run() {
      
        LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
        
        do {
          try {
            int numNewMaps = getMapCompletionEvents();
            if (numNewMaps > 0) {
              synchronized (copyResultsOrNewEventsLock) {
                numEventsFetched += numNewMaps;
                copyResultsOrNewEventsLock.notifyAll();
              }
            }
            if (LOG.isDebugEnabled()) {
              if (numNewMaps > 0) {
                LOG.debug(reduceTask.getTaskID() + ": " +  
                    "Got " + numNewMaps + " new map-outputs"); 
              }
            }
            Thread.sleep(SLEEP_TIME);
          } 
          catch (InterruptedException e) {
            LOG.warn(reduceTask.getTaskID() +
                " GetMapEventsThread returning after an " +
                " interrupted exception");
            return;
          }
          catch (Throwable t) {
            String msg = reduceTask.getTaskID()
                         + " GetMapEventsThread Ignoring exception : " 
                         + StringUtils.stringifyException(t);
            reportFatalError(getTaskID(), t, msg);
          }
        } while (!exitGetMapEvents);

        LOG.info("GetMapEventsThread exiting");
      
      }
      
      /** 
       * Queries the {@link TaskTracker} for a set of map-completion events 
       * from a given event ID.
       * @throws IOException
       */  
      private int getMapCompletionEvents() throws IOException {
        
        int numNewMaps = 0;
        
        MapTaskCompletionEventsUpdate update = 
          umbilical.getMapCompletionEvents(reduceTask.getJobID(), 
                                           fromEventId.get(), 
                                           MAX_EVENTS_TO_FETCH,
                                           reduceTask.getTaskID(), jvmContext);
        TaskCompletionEvent events[] = update.getMapTaskCompletionEvents();
          
        // Check if the reset is required.
        // Since there is no ordering of the task completion events at the 
        // reducer, the only option to sync with the new jobtracker is to reset 
        // the events index
        if (update.shouldReset()) {
          fromEventId.set(0);
          obsoleteMapIds.clear(); // clear the obsolete map
          mapLocations.clear(); // clear the map locations mapping
        }
        
        // Update the last seen event ID
        fromEventId.set(fromEventId.get() + events.length);
        
        // Process the TaskCompletionEvents:
        // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
        // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop 
        //    fetching from those maps.
        // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
        //    outputs at all.
        for (TaskCompletionEvent event : events) {
          switch (event.getTaskStatus()) {
            case SUCCEEDED:
            {
              URI u = URI.create(event.getTaskTrackerHttp());
              String host = u.getHost();
              TaskAttemptID taskId = event.getTaskAttemptId();
              URL mapOutputLocation = new URL(event.getTaskTrackerHttp() + 
                                      "/mapOutput?job=" + taskId.getJobID() +
                                      "&map=" + taskId + 
                                      "&reduce=" + getPartition());
              List<MapOutputLocation> loc = mapLocations.get(host);
              if (loc == null) {
                loc = Collections.synchronizedList
                  (new LinkedList<MapOutputLocation>());
                mapLocations.put(host, loc);
               }
              loc.add(new MapOutputLocation(taskId, host, mapOutputLocation));
              numNewMaps ++;
            }
            break;
            case FAILED:
            case KILLED:
            case OBSOLETE:
            {
              obsoleteMapIds.add(event.getTaskAttemptId());
              LOG.info("Ignoring obsolete output of " + event.getTaskStatus() + 
                       " map-task: '" + event.getTaskAttemptId() + "'");
            }
            break;
            case TIPFAILED:
            {
              copiedMapOutputs.add(event.getTaskAttemptId().getTaskID());
              LOG.info("Ignoring output of failed map TIP: '" +  
                   event.getTaskAttemptId() + "'");
            }
            break;
          }
        }
        return numNewMaps;
      }
    }
  }

  /**
   * Return the exponent of the power of two closest to the given
   * positive value, or zero if value leq 0.
   * This follows the observation that the msb of a given value is
   * also the closest power of two, unless the bit following it is
   * set.
   */
  private static int getClosestPowerOf2(int value) {
    if (value <= 0)
      throw new IllegalArgumentException("Undefined for " + value);
    final int hob = Integer.highestOneBit(value);
    return Integer.numberOfTrailingZeros(hob) +
      (((hob >>> 1) & value) == 0 ? 0 : 1);
  }
}
