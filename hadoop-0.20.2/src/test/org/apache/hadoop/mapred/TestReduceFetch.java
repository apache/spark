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

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestMapCollection.FakeIF;
import org.apache.hadoop.mapred.TestMapCollection.FakeSplit;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestReduceFetch extends TestCase {

  private static MiniMRCluster mrCluster = null;
  private static MiniDFSCluster dfsCluster = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestReduceFetch.class)) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        dfsCluster = new MiniDFSCluster(conf, 2, true, null);
        mrCluster = new MiniMRCluster(2,
            dfsCluster.getFileSystem().getUri().toString(), 1);
      }
      protected void tearDown() throws Exception {
        if (dfsCluster != null) { dfsCluster.shutdown(); }
        if (mrCluster != null) { mrCluster.shutdown(); }
      }
    };
    return setup;
  }

  public static class MapMB
      implements Mapper<NullWritable,NullWritable,Text,Text> {

    public void map(NullWritable nk, NullWritable nv,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      Text key = new Text();
      Text val = new Text();
      key.set("KEYKEYKEYKEYKEYKEYKEYKEY");
      byte[] b = new byte[1000];
      Arrays.fill(b, (byte)'V');
      val.set(b);
      b = null;
      for (int i = 0; i < 4 * 1024; ++i) {
        output.collect(key, val);
      }
    }
    public void configure(JobConf conf) { }
    public void close() throws IOException { }
  }

  public static Counters runJob(JobConf conf) throws Exception {
    conf.setMapperClass(MapMB.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(FakeIF.class);
    FileInputFormat.setInputPaths(conf, new Path("/in"));
    final Path outp = new Path("/out");
    FileOutputFormat.setOutputPath(conf, outp);
    RunningJob job = null;
    try {
      job = JobClient.runJob(conf);
      assertTrue(job.isSuccessful());
    } finally {
      FileSystem fs = dfsCluster.getFileSystem();
      if (fs.exists(outp)) {
        fs.delete(outp, true);
      }
    }
    return job.getCounters();
  }

  public void testReduceFromDisk() throws Exception {
    final int MAP_TASKS = 8;
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "0.0");
    job.setNumMapTasks(MAP_TASKS);
    job.setInt("mapred.job.reduce.total.mem.bytes", 128 << 20);
    job.set("mapred.job.shuffle.input.buffer.percent", "0.05");
    job.setInt("io.sort.factor", 2);
    job.setInt("mapred.inmem.merge.threshold", 4);
    Counters c = runJob(job);
    final long spill = c.findCounter(Task.Counter.SPILLED_RECORDS).getCounter();
    final long out = c.findCounter(Task.Counter.MAP_OUTPUT_RECORDS).getCounter();
    assertTrue("Expected all records spilled during reduce (" + spill + ")",
        spill >= 2 * out); // all records spill at map, reduce
    assertTrue("Expected intermediate merges (" + spill + ")",
        spill >= 2 * out + (out / MAP_TASKS)); // some records hit twice
  }

  public void testReduceFromPartialMem() throws Exception {
    final int MAP_TASKS = 7;
    JobConf job = mrCluster.createJobConf();
    job.setNumMapTasks(MAP_TASKS);
    job.setInt("mapred.inmem.merge.threshold", 0);
    job.set("mapred.job.reduce.input.buffer.percent", "1.0");
    job.setInt("mapred.reduce.parallel.copies", 1);
    job.setInt("io.sort.mb", 10);
    job.setInt("mapred.job.reduce.total.mem.bytes", 128 << 20);
    job.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, "-Xmx128m");
    job.set("mapred.job.shuffle.input.buffer.percent", "0.14");
    job.setNumTasksToExecutePerJvm(1);
    job.set("mapred.job.shuffle.merge.percent", "1.0");
    Counters c = runJob(job);
    final long out = c.findCounter(Task.Counter.MAP_OUTPUT_RECORDS).getCounter();
    final long spill = c.findCounter(Task.Counter.SPILLED_RECORDS).getCounter();
    assertTrue("Expected some records not spilled during reduce" + spill + ")",
        spill < 2 * out); // spilled map records, some records at the reduce
  }

  public void testReduceFromMem() throws Exception {
    final int MAP_TASKS = 3;
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "1.0");
    job.set("mapred.job.shuffle.input.buffer.percent", "1.0");
    job.setInt("mapred.job.reduce.total.mem.bytes", 128 << 20);
    job.setNumMapTasks(MAP_TASKS);
    Counters c = runJob(job);
    final long spill = c.findCounter(Task.Counter.SPILLED_RECORDS).getCounter();
    final long out = c.findCounter(Task.Counter.MAP_OUTPUT_RECORDS).getCounter();
    assertEquals("Spilled records: " + spill, out, spill); // no reduce spill
  }

}
