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

import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.MapTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DebugJobProducer implements JobStoryProducer {
  public static final Log LOG = LogFactory.getLog(DebugJobProducer.class);
  final ArrayList<JobStory> submitted;
  private final Configuration conf;
  private final AtomicInteger numJobs;

  public DebugJobProducer(int numJobs, Configuration conf) {
    super();
    MockJob.reset();
    this.conf = conf;
    this.numJobs = new AtomicInteger(numJobs);
    this.submitted = new ArrayList<JobStory>();
  }

  @Override
  public JobStory getNextJob() throws IOException {
    if (numJobs.getAndDecrement() > 0) {
      final MockJob ret = new MockJob(conf);
      submitted.add(ret);
      return ret;
    }
    return null;
  }

  @Override
  public void close() {
  }


  static double[] getDistr(Random r, double mindist, int size) {
    assert 0.0 <= mindist && mindist <= 1.0;
    final double min = mindist / size;
    final double rem = 1.0 - min * size;
    final double[] tmp = new double[size];
    for (int i = 0; i < tmp.length - 1; ++i) {
      tmp[i] = r.nextDouble() * rem;
    }
    tmp[tmp.length - 1] = rem;
    Arrays.sort(tmp);

    final double[] ret = new double[size];
    ret[0] = tmp[0] + min;
    for (int i = 1; i < size; ++i) {
      ret[i] = tmp[i] - tmp[i - 1] + min;
    }
    return ret;
  }


  /**
   * Generate random task data for a synthetic job.
   */
  static class MockJob implements JobStory {

    static final int MIN_REC = 1 << 14;
    static final int MIN_BYTES = 1 << 20;
    static final int VAR_REC = 1 << 14;
    static final int VAR_BYTES = 4 << 20;
    static final int MAX_MAP = 5;
    static final int MAX_RED = 3;
    final Configuration conf;

    static void initDist(
      Random r, double min, int[] recs, long[] bytes, long tot_recs,
      long tot_bytes) {
      final double[] recs_dist = getDistr(r, min, recs.length);
      final double[] bytes_dist = getDistr(r, min, recs.length);
      long totalbytes = 0L;
      int totalrecs = 0;
      for (int i = 0; i < recs.length; ++i) {
        recs[i] = (int) Math.round(tot_recs * recs_dist[i]);
        bytes[i] = Math.round(tot_bytes * bytes_dist[i]);
        totalrecs += recs[i];
        totalbytes += bytes[i];
      }
      // Add/remove excess
      recs[0] += totalrecs - tot_recs;
      bytes[0] += totalbytes - tot_bytes;
      if (LOG.isInfoEnabled()) {
        LOG.info(
          "DIST: " + Arrays.toString(recs) + " " + tot_recs + "/" + totalrecs +
            " " + Arrays.toString(bytes) + " " + tot_bytes + "/" + totalbytes);
      }
    }

    private static final AtomicInteger seq = new AtomicInteger(0);
    // set timestamp in the past
    private static final AtomicLong timestamp = new AtomicLong(
      System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(
        60, TimeUnit.DAYS));

    private final int id;
    private final String name;
    private final int[] m_recsIn, m_recsOut, r_recsIn, r_recsOut;
    private final long[] m_bytesIn, m_bytesOut, r_bytesIn, r_bytesOut;
    private final long submitTime;

    public MockJob(Configuration conf) {
      final Random r = new Random();
      final long seed = r.nextLong();
      r.setSeed(seed);
      id = seq.getAndIncrement();
      name = String.format("MOCKJOB%05d", id);
      this.conf = conf;
      LOG.info(name + " (" + seed + ")");
      submitTime = timestamp.addAndGet(
        TimeUnit.MILLISECONDS.convert(
          r.nextInt(10), TimeUnit.SECONDS));

      m_recsIn = new int[r.nextInt(MAX_MAP) + 1];
      m_bytesIn = new long[m_recsIn.length];
      m_recsOut = new int[m_recsIn.length];
      m_bytesOut = new long[m_recsIn.length];

      r_recsIn = new int[r.nextInt(MAX_RED) + 1];
      r_bytesIn = new long[r_recsIn.length];
      r_recsOut = new int[r_recsIn.length];
      r_bytesOut = new long[r_recsIn.length];

      // map input
      final long map_recs = r.nextInt(VAR_REC) + MIN_REC;
      final long map_bytes = r.nextInt(VAR_BYTES) + MIN_BYTES;
      initDist(r, 0.5, m_recsIn, m_bytesIn, map_recs, map_bytes);

      // shuffle
      final long shuffle_recs = r.nextInt(VAR_REC) + MIN_REC;
      final long shuffle_bytes = r.nextInt(VAR_BYTES) + MIN_BYTES;
      initDist(r, 0.5, m_recsOut, m_bytesOut, shuffle_recs, shuffle_bytes);
      initDist(r, 0.8, r_recsIn, r_bytesIn, shuffle_recs, shuffle_bytes);

      // reduce output
      final long red_recs = r.nextInt(VAR_REC) + MIN_REC;
      final long red_bytes = r.nextInt(VAR_BYTES) + MIN_BYTES;
      initDist(r, 0.5, r_recsOut, r_bytesOut, red_recs, red_bytes);

      if (LOG.isDebugEnabled()) {
        int iMapBTotal = 0, oMapBTotal = 0, iRedBTotal = 0, oRedBTotal = 0;
        int iMapRTotal = 0, oMapRTotal = 0, iRedRTotal = 0, oRedRTotal = 0;
        for (int i = 0; i < m_recsIn.length; ++i) {
          iMapRTotal += m_recsIn[i];
          iMapBTotal += m_bytesIn[i];
          oMapRTotal += m_recsOut[i];
          oMapBTotal += m_bytesOut[i];
        }
        for (int i = 0; i < r_recsIn.length; ++i) {
          iRedRTotal += r_recsIn[i];
          iRedBTotal += r_bytesIn[i];
          oRedRTotal += r_recsOut[i];
          oRedBTotal += r_bytesOut[i];
        }
        LOG.debug(
          String.format(
            "%s: M (%03d) %6d/%10d -> %6d/%10d" +
              " R (%03d) %6d/%10d -> %6d/%10d @%d", name, m_bytesIn.length,
            iMapRTotal, iMapBTotal, oMapRTotal, oMapBTotal, r_bytesIn.length,
            iRedRTotal, iRedBTotal, oRedRTotal, oRedBTotal, submitTime));
      }
    }
    @Override
   public String getName() {
     return name;
    }

   @Override
   public String getUser() {
     String s = String.format("foobar%d", id);
     GridmixTestUtils.createHomeAndStagingDirectory(s,(JobConf)conf);
     return s;
   }

   @Override
   public JobID getJobID() {
     return new JobID("job_mock_" + name, id);
    }

    @Override
   public Values getOutcome() {
     return Values.SUCCESS;
    }

   @Override
   public long getSubmissionTime() {
     return submitTime;
   }

   @Override
   public int getNumberMaps() {
     return m_bytesIn.length;
   }

   @Override
   public int getNumberReduces() {
     return r_bytesIn.length;
   }
    
    @Override
    public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
      switch (taskType) {
        case MAP:
          return new TaskInfo(m_bytesIn[taskNumber], m_recsIn[taskNumber],
              m_bytesOut[taskNumber], m_recsOut[taskNumber], -1);
        case REDUCE:
          return new TaskInfo(r_bytesIn[taskNumber], r_recsIn[taskNumber],
              r_bytesOut[taskNumber], r_recsOut[taskNumber], -1);
        default:
          throw new IllegalArgumentException("Not interested");
      }
    }

    @Override
    public InputSplit[] getInputSplits() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskAttemptInfo getTaskAttemptInfo(
      TaskType taskType, int taskNumber, int taskAttemptNumber) {
      switch (taskType) {
        case MAP:
          return new MapTaskAttemptInfo(
            State.SUCCEEDED, new TaskInfo(
              m_bytesIn[taskNumber], m_recsIn[taskNumber],
              m_bytesOut[taskNumber], m_recsOut[taskNumber], -1),100);

        case REDUCE:
          return new ReduceTaskAttemptInfo(
            State.SUCCEEDED, new TaskInfo(
              r_bytesIn[taskNumber], r_recsIn[taskNumber],
              r_bytesOut[taskNumber], r_recsOut[taskNumber], -1),100,100,100);
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(
      int taskNumber, int taskAttemptNumber, int locality) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.hadoop.mapred.JobConf getJobConf() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getQueueName() {
      String qName = "q"+((id % 2)+1);
      return qName;
    }
    
    public static void reset() {
      seq.set(0);
      timestamp.set(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(
        60, TimeUnit.DAYS));
    }
  }
}
