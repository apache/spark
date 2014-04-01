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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

// TODO can replace with form of GridmixJob
class GenerateData extends GridmixJob {


  /**
   * Total bytes to write.
   */
  public static final String GRIDMIX_GEN_BYTES = "gridmix.gen.bytes";

  /**
   * Maximum size per file written.
   */
  public static final String GRIDMIX_GEN_CHUNK = "gridmix.gen.bytes.per.file";

  /**
   * Size of writes to output file.
   */
  public static final String GRIDMIX_VAL_BYTES = "gendata.val.bytes";

  /**
   * Status reporting interval, in megabytes.
   */
  public static final String GRIDMIX_GEN_INTERVAL = "gendata.interval.mb";

  /**
   * Blocksize of generated data.
   */
  public static final String GRIDMIX_GEN_BLOCKSIZE = "gridmix.gen.blocksize";

  /**
   * Replication of generated data.
   */
  public static final String GRIDMIX_GEN_REPLICATION = "gridmix.gen.replicas";

  public GenerateData(Configuration conf, Path outdir, long genbytes)
      throws IOException {
    super(conf, 0L, "GRIDMIX_GENDATA");
    job.getConfiguration().setLong(GRIDMIX_GEN_BYTES, genbytes);
    FileOutputFormat.setOutputPath(job, outdir);
  }

  @Override
  public Job call() throws IOException, InterruptedException,
                           ClassNotFoundException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    ugi.doAs( new PrivilegedExceptionAction <Job>() {
       public Job run() throws IOException, ClassNotFoundException,
                               InterruptedException {
        job.setMapperClass(GenDataMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(GenDataFormat.class);
        job.setOutputFormatClass(RawBytesOutputFormat.class);
        job.setJarByClass(GenerateData.class);
        try {
          FileInputFormat.addInputPath(job, new Path("ignored"));
        } catch (IOException e) {
          LOG.error("Error  while adding input path ", e);
        }
        job.submit();
        return job;
      }
    });
    return job;
  }

  public static class GenDataMapper
      extends Mapper<NullWritable,LongWritable,NullWritable,BytesWritable> {

    private BytesWritable val;
    private final Random r = new Random();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      val = new BytesWritable(new byte[
          context.getConfiguration().getInt(GRIDMIX_VAL_BYTES, 1024 * 1024)]);
    }

    @Override
    public void map(NullWritable key, LongWritable value, Context context)
        throws IOException, InterruptedException {
      for (long bytes = value.get(); bytes > 0; bytes -= val.getLength()) {
        r.nextBytes(val.getBytes());
        val.setSize((int)Math.min(val.getLength(), bytes));
        context.write(key, val);
      }
    }

  }

  static class GenDataFormat extends InputFormat<NullWritable,LongWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      final JobClient client =
        new JobClient(new JobConf(jobCtxt.getConfiguration()));
      ClusterStatus stat = client.getClusterStatus(true);
      final long toGen =
        jobCtxt.getConfiguration().getLong(GRIDMIX_GEN_BYTES, -1);
      if (toGen < 0) {
        throw new IOException("Invalid/missing generation bytes: " + toGen);
      }
      final int nTrackers = stat.getTaskTrackers();
      final long bytesPerTracker = toGen / nTrackers;
      final ArrayList<InputSplit> splits = new ArrayList<InputSplit>(nTrackers);
      final Pattern trackerPattern = Pattern.compile("tracker_([^:]*):.*");
      final Matcher m = trackerPattern.matcher("");
      for (String tracker : stat.getActiveTrackerNames()) {
        m.reset(tracker);
        if (!m.find()) {
          System.err.println("Skipping node: " + tracker);
          continue;
        }
        final String name = m.group(1);
        splits.add(new GenSplit(bytesPerTracker, new String[] { name }));
      }
      return splits;
    }

    @Override
    public RecordReader<NullWritable,LongWritable> createRecordReader(
        InputSplit split, final TaskAttemptContext taskContext)
        throws IOException {
      return new RecordReader<NullWritable,LongWritable>() {
        long written = 0L;
        long write = 0L;
        long RINTERVAL;
        long toWrite;
        final NullWritable key = NullWritable.get();
        final LongWritable val = new LongWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext ctxt)
            throws IOException, InterruptedException {
          toWrite = split.getLength();
          RINTERVAL = ctxt.getConfiguration().getInt(
              GRIDMIX_GEN_INTERVAL, 10) << 20;
        }
        @Override
        public boolean nextKeyValue() throws IOException {
          written += write;
          write = Math.min(toWrite - written, RINTERVAL);
          val.set(write);
          return written < toWrite;
        }
        @Override
        public float getProgress() throws IOException {
          return written / ((float)toWrite);
        }
        @Override
        public NullWritable getCurrentKey() { return key; }
        @Override
        public LongWritable getCurrentValue() { return val; }
        @Override
        public void close() throws IOException {
          taskContext.setStatus("Wrote " + toWrite);
        }
      };
    }
  }

  static class GenSplit extends InputSplit implements Writable {
    private long bytes;
    private int nLoc;
    private String[] locations;

    public GenSplit() { }
    public GenSplit(long bytes, String[] locations) {
      this(bytes, locations.length, locations);
    }
    public GenSplit(long bytes, int nLoc, String[] locations) {
      this.bytes = bytes;
      this.nLoc = nLoc;
      this.locations = Arrays.copyOf(locations, nLoc);
    }
    @Override
    public long getLength() {
      return bytes;
    }
    @Override
    public String[] getLocations() {
      return locations;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      bytes = in.readLong();
      nLoc = in.readInt();
      if (null == locations || locations.length < nLoc) {
        locations = new String[nLoc];
      }
      for (int i = 0; i < nLoc; ++i) {
        locations[i] = Text.readString(in);
      }
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(bytes);
      out.writeInt(nLoc);
      for (int i = 0; i < nLoc; ++i) {
        Text.writeString(out, locations[i]);
      }
    }
  }

  static class RawBytesOutputFormat
      extends FileOutputFormat<NullWritable,BytesWritable> {

    @Override
    public RecordWriter<NullWritable,BytesWritable> getRecordWriter(
        TaskAttemptContext job) throws IOException {

      return new ChunkWriter(getDefaultWorkFile(job, ""),
          job.getConfiguration());
    }

    static class ChunkWriter extends RecordWriter<NullWritable,BytesWritable> {
      private final Path outDir;
      private final FileSystem fs;
      private final int blocksize;
      private final short replicas;
      private final long maxFileBytes;
      private final FsPermission genPerms = new FsPermission((short) 0777);

      private long accFileBytes = 0L;
      private long fileIdx = -1L;
      private OutputStream fileOut = null;

      public ChunkWriter(Path outDir, Configuration conf) throws IOException {
        this.outDir = outDir;
        fs = outDir.getFileSystem(conf);
        blocksize = conf.getInt(GRIDMIX_GEN_BLOCKSIZE, 1 << 28);
        replicas = (short) conf.getInt(GRIDMIX_GEN_REPLICATION, 3);
        maxFileBytes = conf.getLong(GRIDMIX_GEN_CHUNK, 1L << 30);
        nextDestination();
      }
      private void nextDestination() throws IOException {
        if (fileOut != null) {
          fileOut.close();
        }
        fileOut = fs.create(new Path(outDir, "segment-" + (++fileIdx)),
            genPerms, false, 64 * 1024, replicas, blocksize, null);
        accFileBytes = 0L;
      }
      @Override
      public void write(NullWritable key, BytesWritable value)
          throws IOException {
        int written = 0;
        final int total = value.getLength();
        while (written < total) {
          if (accFileBytes >= maxFileBytes) {
            nextDestination();
          }
          final int write = (int)
            Math.min(total - written, maxFileBytes - accFileBytes);
          fileOut.write(value.getBytes(), written, write);
          written += write;
          accFileBytes += write;
        }
      }
      @Override
      public void close(TaskAttemptContext ctxt) throws IOException {
        fileOut.close();
      }
    }
  }

}
