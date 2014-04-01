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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Controlled Map/Reduce Job. The tasks are controlled by the presence of
 * particularly named files in the directory signalFileDir on the file-system
 * that the job is configured to work with. Tasks get scheduled by the
 * scheduler, occupy the slots on the TaskTrackers and keep running till the
 * user gives a signal via files whose names are of the form MAPS_[0-9]* and
 * REDUCES_[0-9]*. For e.g., whenever the map tasks see that a file name MAPS_5
 * is created in the singalFileDir, all the maps whose TaskAttemptIDs are below
 * 4 get finished. At any time, there should be only one MAPS_[0-9]* file and
 * only one REDUCES_[0-9]* file in the singnalFileDir. In the beginning MAPS_0
 * and REDUCE_0 files are present, and further signals are given by renaming
 * these files.
 * 
 */
class ControlledMapReduceJob extends Configured implements Tool,
    Mapper<NullWritable, NullWritable, IntWritable, NullWritable>,
    Reducer<IntWritable, NullWritable, NullWritable, NullWritable>,
    Partitioner<IntWritable, NullWritable>,
    InputFormat<NullWritable, NullWritable> {

  static final Log LOG = LogFactory.getLog(ControlledMapReduceJob.class);

  private FileSystem fs = null;
  private int taskNumber;

  private static ArrayList<Path> signalFileDirCache = new ArrayList<Path>();

  private Path signalFileDir;
  {
    Random random = new Random();
    signalFileDir = new Path("signalFileDir-" + random.nextLong());
    while (signalFileDirCache.contains(signalFileDir)) {
      signalFileDir = new Path("signalFileDir-" + random.nextLong());
    }
    signalFileDirCache.add(signalFileDir);
  }

  private long mapsFinished = 0;
  private long reducesFinished = 0;

  private RunningJob rJob = null;

  private int numMappers;
  private int numReducers;

  private final String MAP_SIGFILE_PREFIX = "MAPS_";
  private final String REDUCE_SIGFILE_PREFIX = "REDUCES_";

  private void initialize()
      throws IOException {
    fs = FileSystem.get(getConf());
    fs.mkdirs(signalFileDir);
    writeFile(new Path(signalFileDir, MAP_SIGFILE_PREFIX + mapsFinished));
    writeFile(new Path(signalFileDir, REDUCE_SIGFILE_PREFIX + reducesFinished));
  }

  /**
   * Finish N number of maps/reduces.
   * 
   * @param isMap
   * @param noOfTasksToFinish
   * @throws IOException
   */
  public void finishNTasks(boolean isMap, int noOfTasksToFinish)
      throws IOException {
    if (noOfTasksToFinish < 0) {
      throw new IOException(
          "Negative values for noOfTasksToFinish not acceptable");
    }

    if (noOfTasksToFinish == 0) {
      return;
    }

    LOG.info("Going to finish off " + noOfTasksToFinish);
    String PREFIX = isMap ? MAP_SIGFILE_PREFIX : REDUCE_SIGFILE_PREFIX;
    long tasksFinished = isMap ? mapsFinished : reducesFinished;
    Path oldSignalFile =
        new Path(signalFileDir, PREFIX + String.valueOf(tasksFinished));
    Path newSignalFile =
        new Path(signalFileDir, PREFIX
            + String.valueOf(tasksFinished + noOfTasksToFinish));
    fs.rename(oldSignalFile, newSignalFile);
    if (isMap) {
      mapsFinished += noOfTasksToFinish;
    } else {
      reducesFinished += noOfTasksToFinish;
    }
    LOG.info("Successfully sent signal to finish off " + noOfTasksToFinish);
  }

  /**
   * Finished all tasks of type determined by isMap
   * 
   * @param isMap
   * @throws IOException
   */
  public void finishAllTasks(boolean isMap)
      throws IOException {
    finishNTasks(isMap, (isMap ? numMappers : numReducers));
  }

  /**
   * Finish the job
   * 
   * @throws IOException
   */
  public void finishJob()
      throws IOException {
    finishAllTasks(true);
    finishAllTasks(false);
  }

  /**
   * Wait till noOfTasksToBeRunning number of tasks of type specified by isMap
   * started running. This currently uses a jip object and directly uses its api
   * to determine the number of tasks running.
   * 
   * <p>
   * 
   * TODO: It should eventually use a JobID and then get the information from
   * the JT to check the number of running tasks.
   * 
   * @param jip
   * @param isMap
   * @param noOfTasksToBeRunning
   */
  static void waitTillNTasksStartRunning(JobInProgress jip, boolean isMap,
      int noOfTasksToBeRunning)
      throws InterruptedException {
    int numTasks = 0;
    while (numTasks != noOfTasksToBeRunning) {
      Thread.sleep(1000);
      numTasks = isMap ? jip.runningMaps() : jip.runningReduces();
      LOG.info("Waiting till " + noOfTasksToBeRunning
          + (isMap ? " map" : " reduce") + " tasks of the job "
          + jip.getJobID() + " start running. " + numTasks
          + " tasks already started running.");
    }
  }

  /**
   * Make sure that the number of tasks of type specified by isMap running in
   * the given job is the same as noOfTasksToBeRunning
   * 
   * <p>
   * 
   * TODO: It should eventually use a JobID and then get the information from
   * the JT to check the number of running tasks.
   * 
   * @param jip
   * @param isMap
   * @param noOfTasksToBeRunning
   */
  static void assertNumTasksRunning(JobInProgress jip, boolean isMap,
      int noOfTasksToBeRunning)
      throws Exception {
    if ((isMap ? jip.runningMaps() : jip.runningReduces()) != noOfTasksToBeRunning) {
      throw new Exception("Number of tasks running is not "
          + noOfTasksToBeRunning);
    }
  }

  /**
   * Wait till noOfTasksToFinish number of tasks of type specified by isMap
   * are finished. This currently uses a jip object and directly uses its api to
   * determine the number of tasks finished.
   * 
   * <p>
   * 
   * TODO: It should eventually use a JobID and then get the information from
   * the JT to check the number of finished tasks.
   * 
   * @param jip
   * @param isMap
   * @param noOfTasksToFinish
   * @throws InterruptedException
   */
  static void waitTillNTotalTasksFinish(JobInProgress jip, boolean isMap,
      int noOfTasksToFinish)
      throws InterruptedException {
    int noOfTasksAlreadyFinished = 0;
    while (noOfTasksAlreadyFinished < noOfTasksToFinish) {
      Thread.sleep(1000);
      noOfTasksAlreadyFinished =
          (isMap ? jip.finishedMaps() : jip.finishedReduces());
      LOG.info("Waiting till " + noOfTasksToFinish
          + (isMap ? " map" : " reduce") + " tasks of the job "
          + jip.getJobID() + " finish. " + noOfTasksAlreadyFinished
          + " tasks already got finished.");
    }
  }

  /**
   * Have all the tasks of type specified by isMap finished in this job?
   * 
   * @param jip
   * @param isMap
   * @return true if finished, false otherwise
   */
  static boolean haveAllTasksFinished(JobInProgress jip, boolean isMap) {
    return ((isMap ? jip.runningMaps() : jip.runningReduces()) == 0);
  }

  private void writeFile(Path name)
      throws IOException {
    Configuration conf = new Configuration(false);
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, conf, name, BytesWritable.class,
            BytesWritable.class, CompressionType.NONE);
    writer.append(new BytesWritable(), new BytesWritable());
    writer.close();
  }

  @Override
  public void configure(JobConf conf) {
    try {
      signalFileDir = new Path(conf.get("signal.dir.path"));
      numReducers = conf.getNumReduceTasks();
      fs = FileSystem.get(conf);
      String taskAttemptId = conf.get("mapred.task.id");
      if (taskAttemptId != null) {
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptId);
        taskNumber = taskAttemptID.getTaskID().getId();
      }
    } catch (IOException ioe) {
      LOG.warn("Caught exception " + ioe);
    }
  }

  private FileStatus[] listSignalFiles(FileSystem fileSys, final boolean isMap)
      throws IOException {
    return fileSys.globStatus(new Path(signalFileDir.toString() + "/*"),
        new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (isMap && path.getName().startsWith(MAP_SIGFILE_PREFIX)) {
              LOG.debug("Found signal file : " + path.getName());
              return true;
            } else if (!isMap
                && path.getName().startsWith(REDUCE_SIGFILE_PREFIX)) {
              LOG.debug("Found signal file : " + path.getName());
              return true;
            }
            LOG.info("Didn't find any relevant signal files.");
            return false;
          }
        });
  }

  @Override
  public void map(NullWritable key, NullWritable value,
      OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    LOG.info(taskNumber + " has started.");
    FileStatus[] files = listSignalFiles(fs, true);
    String[] sigFileComps = files[0].getPath().getName().split("_");
    String signalType = sigFileComps[0];
    int noOfTasks = Integer.parseInt(sigFileComps[1]);

    while (!signalType.equals("MAPS") || taskNumber + 1 > noOfTasks) {
      LOG.info("Signal type found : " + signalType
          + " .Number of tasks to be finished by this signal : " + noOfTasks
          + " . My id : " + taskNumber);
      LOG.info(taskNumber + " is still alive.");
      try {
        reporter.progress();
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.info(taskNumber + " is still alive.");
        break;
      }
      files = listSignalFiles(fs, true);
      sigFileComps = files[0].getPath().getName().split("_");
      signalType = sigFileComps[0];
      noOfTasks = Integer.parseInt(sigFileComps[1]);
    }
    LOG.info("Signal type found : " + signalType
        + " .Number of tasks to be finished by this signal : " + noOfTasks
        + " . My id : " + taskNumber);
    // output numReduce number of random values, so that
    // each reducer will get one key each.
    for (int i = 0; i < numReducers; i++) {
      output.collect(new IntWritable(i), NullWritable.get());
    }

    LOG.info(taskNumber + " is finished.");
  }

  @Override
  public void reduce(IntWritable key, Iterator<NullWritable> values,
      OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    LOG.info(taskNumber + " has started.");
    FileStatus[] files = listSignalFiles(fs, false);
    String[] sigFileComps = files[0].getPath().getName().split("_");
    String signalType = sigFileComps[0];
    int noOfTasks = Integer.parseInt(sigFileComps[1]);

    while (!signalType.equals("REDUCES") || taskNumber + 1 > noOfTasks) {
      LOG.info("Signal type found : " + signalType
          + " .Number of tasks to be finished by this signal : " + noOfTasks
          + " . My id : " + taskNumber);
      LOG.info(taskNumber + " is still alive.");
      try {
        reporter.progress();
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.info(taskNumber + " is still alive.");
        break;
      }
      files = listSignalFiles(fs, false);
      sigFileComps = files[0].getPath().getName().split("_");
      signalType = sigFileComps[0];
      noOfTasks = Integer.parseInt(sigFileComps[1]);
    }
    LOG.info("Signal type found : " + signalType
        + " .Number of tasks to be finished by this signal : " + noOfTasks
        + " . My id : " + taskNumber);
    LOG.info(taskNumber + " is finished.");
  }

  @Override
  public void close()
      throws IOException {
    // nothing
  }

  public JobID getJobId() {
    if (rJob == null) {
      return null;
    }
    return rJob.getID();
  }

  public int run(int numMapper, int numReducer)
      throws IOException {
    JobConf conf =
        getControlledMapReduceJobConf(getConf(), numMapper, numReducer);
    JobClient client = new JobClient(conf);
    rJob = client.submitJob(conf);
    while (!rJob.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        break;
      }
    }
    if (rJob.isSuccessful()) {
      return 0;
    }
    return 1;
  }

  private JobConf getControlledMapReduceJobConf(Configuration clusterConf,
      int numMapper, int numReducer)
      throws IOException {
    setConf(clusterConf);
    initialize();
    JobConf conf = new JobConf(getConf(), ControlledMapReduceJob.class);
    conf.setJobName("ControlledJob");
    conf.set("signal.dir.path", signalFileDir.toString());
    conf.setNumMapTasks(numMapper);
    conf.setNumReduceTasks(numReducer);
    conf.setMapperClass(ControlledMapReduceJob.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setReducerClass(ControlledMapReduceJob.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
    conf.setInputFormat(ControlledMapReduceJob.class);
    FileInputFormat.addInputPath(conf, new Path("ignored"));
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapSpeculativeExecution(false);
    conf.setReduceSpeculativeExecution(false);

    // Set the following for reduce tasks to be able to be started running
    // immediately along with maps.
    conf.set("mapred.reduce.slowstart.completed.maps", String.valueOf(0));

    return conf;
  }

  @Override
  public int run(String[] args)
      throws Exception {
    numMappers = Integer.parseInt(args[0]);
    numReducers = Integer.parseInt(args[1]);
    return run(numMappers, numReducers);
  }

  @Override
  public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
    return k.get() % numPartitions;
  }

  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) {
    LOG.debug("Inside RecordReader.getRecordReader");
    return new RecordReader<NullWritable, NullWritable>() {
      private int pos = 0;

      public void close() {
        // nothing
      }

      public NullWritable createKey() {
        return NullWritable.get();
      }

      public NullWritable createValue() {
        return NullWritable.get();
      }

      public long getPos() {
        return pos;
      }

      public float getProgress() {
        return pos * 100;
      }

      public boolean next(NullWritable key, NullWritable value) {
        if (pos++ == 0) {
          LOG.debug("Returning the next record");
          return true;
        }
        LOG.debug("No more records. Returning none.");
        return false;
      }
    };
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) {
    LOG.debug("Inside InputSplit.getSplits");
    InputSplit[] ret = new InputSplit[numSplits];
    for (int i = 0; i < numSplits; ++i) {
      ret[i] = new EmptySplit();
    }
    return ret;
  }

  public static class EmptySplit implements InputSplit {
    public void write(DataOutput out)
        throws IOException {
    }

    public void readFields(DataInput in)
        throws IOException {
    }

    public long getLength() {
      return 0L;
    }

    public String[] getLocations() {
      return new String[0];
    }
  }

  static class ControlledMapReduceJobRunner extends Thread {
    private JobConf conf;
    private ControlledMapReduceJob job;
    private JobID jobID;

    private int numMappers;
    private int numReducers;

    public ControlledMapReduceJobRunner() {
      this(new JobConf(), 5, 5);
    }

    public ControlledMapReduceJobRunner(JobConf cnf, int numMap, int numRed) {
      this.conf = cnf;
      this.numMappers = numMap;
      this.numReducers = numRed;
    }

    public ControlledMapReduceJob getJob() {
      while (job == null) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.info(ControlledMapReduceJobRunner.class.getName()
              + " is interrupted.");
          break;
        }
      }
      return job;
    }

    public JobID getJobID()
        throws IOException {
      ControlledMapReduceJob job = getJob();
      JobID id = job.getJobId();
      while (id == null) {
        id = job.getJobId();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.info(ControlledMapReduceJobRunner.class.getName()
              + " is interrupted.");
          break;
        }
      }
      return id;
    }

    @Override
    public void run() {
      if (job != null) {
        LOG.warn("Job is already running.");
        return;
      }
      try {
        job = new ControlledMapReduceJob();
        int ret =
            ToolRunner.run(this.conf, job, new String[] {
                String.valueOf(numMappers), String.valueOf(numReducers) });
        LOG.info("Return value for the job : " + ret);
      } catch (Exception e) {
        LOG.warn("Caught exception : " + StringUtils.stringifyException(e));
      }
    }

    static ControlledMapReduceJobRunner getControlledMapReduceJobRunner(
        JobConf conf, int numMappers, int numReducers) {
      return new ControlledMapReduceJobRunner(conf, numMappers, numReducers);
    }
  }
}
