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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Synthetic job generated from a trace description.
 */
abstract class GridmixJob implements Callable<Job>, Delayed {

  public static final String JOBNAME = "GRIDMIX";
  public static final String ORIGNAME = "gridmix.job.name.original";
  public static final Log LOG = LogFactory.getLog(GridmixJob.class);

  private static final ThreadLocal<Formatter> nameFormat =
    new ThreadLocal<Formatter>() {
      @Override
      protected Formatter initialValue() {
        final StringBuilder sb = new StringBuilder(JOBNAME.length() + 5);
        sb.append(JOBNAME);
        return new Formatter(sb);
      }
    };

  protected final int seq;
  protected final Path outdir;
  protected final Job job;
  protected final JobStory jobdesc;
  protected final UserGroupInformation ugi;
  protected final long submissionTimeNanos;
  private static final ConcurrentHashMap<Integer,List<InputSplit>> descCache =
     new ConcurrentHashMap<Integer,List<InputSplit>>();
  protected static final String GRIDMIX_JOB_SEQ = "gridmix.job.seq";
  protected static final String GRIDMIX_USE_QUEUE_IN_TRACE = 
      "gridmix.job-submission.use-queue-in-trace";
  protected static final String GRIDMIX_DEFAULT_QUEUE = 
      "gridmix.job-submission.default-queue";

  private static void setJobQueue(Job job, String queue) {
    if (queue != null)
      job.getConfiguration().set("mapred.job.queue.name", queue);
  }
  
  public GridmixJob(
    final Configuration conf, long submissionMillis, final JobStory jobdesc,
    Path outRoot, UserGroupInformation ugi, final int seq) throws IOException {
    this.ugi = ugi;
    this.jobdesc = jobdesc;
    this.seq = seq;

    ((StringBuilder)nameFormat.get().out()).setLength(JOBNAME.length());
    try {
      job = this.ugi.doAs(new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException {
          Job ret = new Job(conf, nameFormat.get().format("%05d", seq)
              .toString());
          ret.getConfiguration().setInt(GRIDMIX_JOB_SEQ, seq);
          ret.getConfiguration().set(ORIGNAME,
              null == jobdesc.getJobID() ? "<unknown>" : jobdesc.getJobID()
                  .toString());
          if (conf.getBoolean(GRIDMIX_USE_QUEUE_IN_TRACE, false)) {
            setJobQueue(ret, jobdesc.getQueueName());
          } else {
            setJobQueue(ret, conf.get(GRIDMIX_DEFAULT_QUEUE));
          }

          return ret;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    outdir = new Path(outRoot, "" + seq);
  }

  protected GridmixJob(
    final Configuration conf, long submissionMillis, final String name)
  throws IOException {
    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    jobdesc = null;
    outdir = null;
    seq = -1;
    ugi = UserGroupInformation.getCurrentUser();

    try {
      job = this.ugi.doAs(new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException {
          Job ret = new Job(conf, name);
          ret.getConfiguration().setInt("gridmix.job.seq", seq);
          setJobQueue(ret, conf.get(GRIDMIX_DEFAULT_QUEUE));

          return ret;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }

  public String toString() {
    return job.getJobName();
  }

  public long getDelay(TimeUnit unit) {
    return unit.convert(submissionTimeNanos - System.nanoTime(),
        TimeUnit.NANOSECONDS);
  }

  int id() {
    return seq;
  }

  Job getJob() {
    return job;
  }

  JobStory getJobDesc() {
    return jobdesc;
  }

  static void pushDescription(int seq, List<InputSplit> splits) {
    if (null != descCache.putIfAbsent(seq, splits)) {
      throw new IllegalArgumentException("Description exists for id " + seq);
    }
  }

  static List<InputSplit> pullDescription(JobContext jobCtxt) {
    return pullDescription(GridmixJob.getJobSeqId(jobCtxt));
  }
  
  static List<InputSplit> pullDescription(int seq) {
    return descCache.remove(seq);
  }

  static void clearAll() {
    descCache.clear();
  }

  void buildSplits(FilePool inputDir) throws IOException {

  }

  @Override
  public int compareTo(Delayed other) {
    if (this == other) {
      return 0;
    }
    if (other instanceof GridmixJob) {
      final long otherNanos = ((GridmixJob)other).submissionTimeNanos;
      if (otherNanos < submissionTimeNanos) {
        return 1;
      }
      if (otherNanos > submissionTimeNanos) {
        return -1;
      }
      return id() - ((GridmixJob)other).id();
    }
    final long diff =
      getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS);
    return 0 == diff ? 0 : (diff > 0 ? 1 : -1);
  }


  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    // not possible unless job is cloned; all jobs should be unique
    return other instanceof GridmixJob && id() == ((GridmixJob)other).id();
  }

  @Override
  public int hashCode() {
    return id();
  }

  static int getJobSeqId(JobContext job) {
    return job.getConfiguration().getInt(GRIDMIX_JOB_SEQ,-1);
  }

  public static class DraftPartitioner<V> extends Partitioner<GridmixKey,V> {
    public int getPartition(GridmixKey key, V value, int numReduceTasks) {
      return key.getPartition();
    }
  }

  public static class SpecGroupingComparator
      implements RawComparator<GridmixKey> {
    private final DataInputBuffer di = new DataInputBuffer();
    private final byte[] reset = di.getData();
    @Override
    public int compare(GridmixKey g1, GridmixKey g2) {
      final byte t1 = g1.getType();
      final byte t2 = g2.getType();
      if (t1 == GridmixKey.REDUCE_SPEC ||
          t2 == GridmixKey.REDUCE_SPEC) {
        return t1 - t2;
      }
      assert t1 == GridmixKey.DATA;
      assert t2 == GridmixKey.DATA;
      return g1.compareTo(g2);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        final int ret;
        di.reset(b1, s1, l1);
        final int x1 = WritableUtils.readVInt(di);
        di.reset(b2, s2, l2);
        final int x2 = WritableUtils.readVInt(di);
        final int t1 = b1[s1 + x1];
        final int t2 = b2[s2 + x2];
        if (t1 == GridmixKey.REDUCE_SPEC ||
            t2 == GridmixKey.REDUCE_SPEC) {
          ret = t1 - t2;
        } else {
          assert t1 == GridmixKey.DATA;
          assert t2 == GridmixKey.DATA;
          ret =
            WritableComparator.compareBytes(b1, s1, x1, b2, s2, x2);
        }
        di.reset(reset, 0, 0);
        return ret;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class RawBytesOutputFormat<K>
      extends FileOutputFormat<K,GridmixRecord> {

    @Override
    public RecordWriter<K,GridmixRecord> getRecordWriter(
        TaskAttemptContext job) throws IOException {

      Path file = getDefaultWorkFile(job, "");
      FileSystem fs = file.getFileSystem(job.getConfiguration());
      final FSDataOutputStream fileOut = fs.create(file, false);
      return new RecordWriter<K,GridmixRecord>() {
        @Override
        public void write(K ignored, GridmixRecord value)
            throws IOException {
          value.writeRandom(fileOut, value.getSize());
        }
        @Override
        public void close(TaskAttemptContext ctxt) throws IOException {
          fileOut.close();
        }
      };
    }
  }
}
