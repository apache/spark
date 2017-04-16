/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.gridmix.RandomAlgorithms.Selector;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SleepJob extends GridmixJob {
  public static final Log LOG = LogFactory.getLog(SleepJob.class);
  private static final ThreadLocal <Random> rand = 
    new ThreadLocal <Random> () {
        @Override protected Random initialValue() {
            return new Random();
    }
  };
  
  public static final String SLEEPJOB_MAPTASK_ONLY="gridmix.sleep.maptask-only";
  private final boolean mapTasksOnly;
  private final int fakeLocations;
  private final String[] hosts;
  private final Selector selector;
  
  /**
   * Interval at which to report progress, in seconds.
   */
  public static final String GRIDMIX_SLEEP_INTERVAL = "gridmix.sleep.interval";
  public static final String GRIDMIX_SLEEP_MAX_MAP_TIME = 
    "gridmix.sleep.max-map-time";
  public static final String GRIDMIX_SLEEP_MAX_REDUCE_TIME = 
    "gridmix.sleep.max-reduce-time";

  private final long mapMaxSleepTime, reduceMaxSleepTime;

  public SleepJob(Configuration conf, long submissionMillis, JobStory jobdesc,
      Path outRoot, UserGroupInformation ugi, int seq, int numLocations,
      String[] hosts) throws IOException {
    super(conf, submissionMillis, jobdesc, outRoot, ugi, seq);
    this.fakeLocations = numLocations;
    this.hosts = hosts;
    this.selector = (fakeLocations > 0)? new Selector(hosts.length, (float) fakeLocations
        / hosts.length, rand.get()) : null;
    this.mapTasksOnly = conf.getBoolean(SLEEPJOB_MAPTASK_ONLY, false);
    mapMaxSleepTime = conf.getLong(GRIDMIX_SLEEP_MAX_MAP_TIME, Long.MAX_VALUE);
    reduceMaxSleepTime = conf.getLong(GRIDMIX_SLEEP_MAX_REDUCE_TIME,
        Long.MAX_VALUE);
  }

  @Override
  public Job call()
    throws IOException, InterruptedException, ClassNotFoundException {
    ugi.doAs(
      new PrivilegedExceptionAction<Job>() {
        public Job run()
          throws IOException, ClassNotFoundException, InterruptedException {
          job.setMapperClass(SleepMapper.class);
          job.setReducerClass(SleepReducer.class);
          job.setNumReduceTasks((mapTasksOnly) ? 0 : jobdesc.getNumberReduces());
          job.setMapOutputKeyClass(GridmixKey.class);
          job.setMapOutputValueClass(NullWritable.class);
          job.setSortComparatorClass(GridmixKey.Comparator.class);
          job.setGroupingComparatorClass(SpecGroupingComparator.class);
          job.setInputFormatClass(SleepInputFormat.class);
          job.setOutputFormatClass(NullOutputFormat.class);
          job.setPartitionerClass(DraftPartitioner.class);
          job.setJarByClass(SleepJob.class);
          job.getConfiguration().setBoolean(
            "mapred.used.genericoptionsparser", true);
          job.submit();
          return job;

        }
      });

    return job;
  }

  public static class SleepMapper
    extends Mapper<LongWritable, LongWritable, GridmixKey, NullWritable> {

    @Override
    public void map(LongWritable key, LongWritable value, Context context)
      throws IOException, InterruptedException {
      context.setStatus("Sleeping... " + value.get() + " ms left");
      long now = System.currentTimeMillis();
      if (now < key.get()) {
        TimeUnit.MILLISECONDS.sleep(key.get() - now);
      }
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException {
      final int nReds = context.getNumReduceTasks();
      if (nReds > 0) {
        final SleepSplit split = (SleepSplit) context.getInputSplit();
        int id = split.getId();
        final int nMaps = split.getNumMaps();
        //This is a hack to pass the sleep duration via Gridmix key
        //TODO: We need to come up with better solution for this.
        
        final GridmixKey key = new GridmixKey(GridmixKey.REDUCE_SPEC, 0, 0L);
        for (int i = id, idx = 0; i < nReds; i += nMaps) {
          key.setPartition(i);
          key.setReduceOutputBytes(split.getReduceDurations(idx++));
          id += nReds;
          context.write(key, NullWritable.get());
        }
      }
    }

  }

  public static class SleepReducer
    extends Reducer<GridmixKey, NullWritable, NullWritable, NullWritable> {

    private long duration = 0L;

    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      if (!context.nextKey() ||
        context.getCurrentKey().getType() != GridmixKey.REDUCE_SPEC) {
        throw new IOException("Missing reduce spec");
      }
      for (NullWritable ignored : context.getValues()) {
        final GridmixKey spec = context.getCurrentKey();
        duration += spec.getReduceOutputBytes();
      }
      final long RINTERVAL = TimeUnit.MILLISECONDS.convert(
        context.getConfiguration().getLong(
          GRIDMIX_SLEEP_INTERVAL, 5), TimeUnit.SECONDS);
      //This is to stop accumulating deviation from expected sleep time
      //over a period of time.
      long start = System.currentTimeMillis();
      long slept = 0L;
      long sleep = 0L;
      while (slept < duration) {
        final long rem = duration - slept;
        sleep = Math.min(rem, RINTERVAL);
        context.setStatus("Sleeping... " + rem + " ms left");
        TimeUnit.MILLISECONDS.sleep(sleep);
        slept = System.currentTimeMillis() - start;
      }
    }

    @Override
    protected void cleanup(Context context)
      throws IOException, InterruptedException {
      final String msg = "Slept for " + duration;
      LOG.info(msg);
      context.setStatus(msg);
    }
  }

  public static class SleepInputFormat
    extends InputFormat<LongWritable, LongWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      return pullDescription(jobCtxt);
    }

    @Override
    public RecordReader<LongWritable, LongWritable> createRecordReader(
      InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException {
      final long duration = split.getLength();
      final long RINTERVAL = TimeUnit.MILLISECONDS.convert(
        context.getConfiguration().getLong(
          GRIDMIX_SLEEP_INTERVAL, 5), TimeUnit.SECONDS);
      if (RINTERVAL <= 0) {
        throw new IOException(
          "Invalid " + GRIDMIX_SLEEP_INTERVAL + ": " + RINTERVAL);
      }
      return new RecordReader<LongWritable, LongWritable>() {
        long start = -1;
        long slept = 0L;
        long sleep = 0L;
        final LongWritable key = new LongWritable();
        final LongWritable val = new LongWritable();

        @Override
        public boolean nextKeyValue() throws IOException {
          if (start == -1) {
            start = System.currentTimeMillis();
          }
          slept += sleep;
          sleep = Math.min(duration - slept, RINTERVAL);
          key.set(slept + sleep + start);
          val.set(duration - slept);
          return slept < duration;
        }

        @Override
        public float getProgress() throws IOException {
          return slept / ((float) duration);
        }

        @Override
        public LongWritable getCurrentKey() {
          return key;
        }

        @Override
        public LongWritable getCurrentValue() {
          return val;
        }

        @Override
        public void close() throws IOException {
          final String msg = "Slept for " + duration;
          LOG.info(msg);
        }

        public void initialize(InputSplit split, TaskAttemptContext ctxt) {
        }
      };
    }
  }

  public static class SleepSplit extends InputSplit implements Writable {
    private int id;
    private int nSpec;
    private int nMaps;
    private long sleepDuration;
    private long[] reduceDurations = new long[0];
    private String[] locations = new String[0];

    public SleepSplit() {
    }

    public SleepSplit(
      int id, long sleepDuration, long[] reduceDurations, int nMaps,
      String[] locations) {
      this.id = id;
      this.sleepDuration = sleepDuration;
      nSpec = reduceDurations.length;
      this.reduceDurations = reduceDurations;
      this.nMaps = nMaps;
      this.locations = locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVInt(out, id);
      WritableUtils.writeVLong(out, sleepDuration);
      WritableUtils.writeVInt(out, nMaps);
      WritableUtils.writeVInt(out, nSpec);
      for (int i = 0; i < nSpec; ++i) {
        WritableUtils.writeVLong(out, reduceDurations[i]);
      }
      WritableUtils.writeVInt(out, locations.length);
      for (int i = 0; i < locations.length; ++i) {
        Text.writeString(out, locations[i]);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = WritableUtils.readVInt(in);
      sleepDuration = WritableUtils.readVLong(in);
      nMaps = WritableUtils.readVInt(in);
      nSpec = WritableUtils.readVInt(in);
      if (reduceDurations.length < nSpec) {
        reduceDurations = new long[nSpec];
      }
      for (int i = 0; i < nSpec; ++i) {
        reduceDurations[i] = WritableUtils.readVLong(in);
      }
      final int nLoc = WritableUtils.readVInt(in);
      if (nLoc != locations.length) {
        locations = new String[nLoc];
      }
      for (int i = 0; i < nLoc; ++i) {
        locations[i] = Text.readString(in);
      }
    }

    @Override
    public long getLength() {
      return sleepDuration;
    }

    public int getId() {
      return id;
    }

    public int getNumMaps() {
      return nMaps;
    }

    public long getReduceDurations(int i) {
      return reduceDurations[i];
    }

    @Override
    public String[] getLocations() {
      return locations;
    }
  }

  private TaskAttemptInfo getSuccessfulAttemptInfo(TaskType type, int task) {
    TaskAttemptInfo ret;
    for (int i = 0; true; ++i) {
      // Rumen should make up an attempt if it's missing. Or this won't work
      // at all. It's hard to discern what is happening in there.
      ret = jobdesc.getTaskAttemptInfo(type, task, i);
      if (ret.getRunState() == TaskStatus.State.SUCCEEDED) {
        break;
      }
    }
    if(ret.getRunState() != TaskStatus.State.SUCCEEDED) {
      LOG.warn("No sucessful attempts tasktype " + type +" task "+ task);
    }

    return ret;
  }

  @Override
  void buildSplits(FilePool inputDir) throws IOException {
    final List<InputSplit> splits = new ArrayList<InputSplit>();
    final int reds = (mapTasksOnly) ? 0 : jobdesc.getNumberReduces();
    final int maps = jobdesc.getNumberMaps();
    for (int i = 0; i < maps; ++i) {
      final int nSpec = reds / maps + ((reds % maps) > i ? 1 : 0);
      final long[] redDurations = new long[nSpec];
      for (int j = 0; j < nSpec; ++j) {
        final ReduceTaskAttemptInfo info =
          (ReduceTaskAttemptInfo) getSuccessfulAttemptInfo(
            TaskType.REDUCE, i + j * maps);
        // Include only merge/reduce time
        redDurations[j] = Math.min(reduceMaxSleepTime, info.getMergeRuntime()
            + info.getReduceRuntime());
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            String.format(
              "SPEC(%d) %d -> %d %d/%d", id(), i, i + j * maps, redDurations[j],
              info.getRuntime()));
        }
      }
      final TaskAttemptInfo info = getSuccessfulAttemptInfo(TaskType.MAP, i);
      ArrayList<String> locations = new ArrayList<String>(fakeLocations);
      if (fakeLocations > 0) {
        selector.reset();
      }
      for (int k=0; k<fakeLocations; ++k) {
        int index = selector.next();
        if (index < 0) break;
        locations.add(hosts[index]);
      }

      splits.add(new SleepSplit(i,
          Math.min(info.getRuntime(), mapMaxSleepTime), redDurations, maps,
          locations.toArray(new String[locations.size()])));
    }
    pushDescription(id(), splits);
  }
}
