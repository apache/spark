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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;

/**
 * A JUnit test to test the Map-Reduce framework's sort 
 * with a Mini Map-Reduce Cluster with a Mini HDFS Clusters.
 */
public class TestMiniMRDFSSort extends TestCase {
  // Input/Output paths for sort
  private static final Path SORT_INPUT_PATH = new Path("/sort/input");
  private static final Path SORT_OUTPUT_PATH = new Path("/sort/output");

  // Knobs to control randomwriter; and hence sort
  private static final int NUM_HADOOP_SLAVES = 3;
  // make it big enough to cause a spill in the map
  private static final int RW_BYTES_PER_MAP = 3 * 1024 * 1024;
  private static final int RW_MAPS_PER_HOST = 2;

  private static MiniMRCluster mrCluster = null;
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem dfs = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestMiniMRDFSSort.class)) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        dfsCluster = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(NUM_HADOOP_SLAVES, 
                                      dfs.getUri().toString(), 1);
      }
      protected void tearDown() throws Exception {
        if (dfsCluster != null) { dfsCluster.shutdown(); }
        if (mrCluster != null) { mrCluster.shutdown(); }
      }
    };
    return setup;
  }

  public static void runRandomWriter(JobConf job, Path sortInput) 
  throws Exception {
    // Scale down the default settings for RandomWriter for the test-case
    // Generates NUM_HADOOP_SLAVES * RW_MAPS_PER_HOST * RW_BYTES_PER_MAP
    job.setInt("test.randomwrite.bytes_per_map", RW_BYTES_PER_MAP);
    job.setInt("test.randomwriter.maps_per_host", RW_MAPS_PER_HOST);
    String[] rwArgs = {sortInput.toString()};
    
    // Run RandomWriter
    assertEquals(ToolRunner.run(job, new RandomWriter(), rwArgs), 0);
  }
  
  private static void runSort(JobConf job, Path sortInput, Path sortOutput) 
  throws Exception {

    job.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    job.setInt("io.sort.mb", 1);
    job.setNumMapTasks(12);

    // Setup command-line arguments to 'sort'
    String[] sortArgs = {sortInput.toString(), sortOutput.toString()};
    
    // Run Sort
    Sort sort = new Sort();
    assertEquals(ToolRunner.run(job, sort, sortArgs), 0);
    Counters counters = sort.getResult().getCounters();
    long mapInput = counters.findCounter(Task.Counter.MAP_INPUT_BYTES
    ).getValue();
    long hdfsRead = counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,
                                         "HDFS_BYTES_READ").getValue();
    // the hdfs read should be between 100% and 110% of the map input bytes
    assertTrue("map input = " + mapInput + ", hdfs read = " + hdfsRead,
               (hdfsRead < (mapInput * 1.1)) &&
               (hdfsRead > mapInput));  
  }
  
  private static void runSortValidator(JobConf job, 
                                       Path sortInput, Path sortOutput) 
  throws Exception {
    String[] svArgs = {"-sortInput", sortInput.toString(), 
                       "-sortOutput", sortOutput.toString()};

    // Run Sort-Validator
    assertEquals(ToolRunner.run(job, new SortValidator(), svArgs), 0);
  }
  
  private static class ReuseDetector extends MapReduceBase
      implements Mapper<BytesWritable,BytesWritable, Text, Text> {
    static int instances = 0;
    Reporter reporter = null;

    @Override
    public void map(BytesWritable key, BytesWritable value,
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
      this.reporter = reporter;
    }
    
    public void close() throws IOException {
      reporter.incrCounter("jvm", "use", ++instances);
    }
  }

  private static void runJvmReuseTest(JobConf job,
                                      boolean reuse) throws IOException {
    // setup a map-only job that reads the input and only sets the counters
    // based on how many times the jvm was reused.
    job.setInt("mapred.job.reuse.jvm.num.tasks", reuse ? -1 : 1);
    FileInputFormat.setInputPaths(job, SORT_INPUT_PATH);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setMapperClass(ReuseDetector.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumMapTasks(24);
    job.setNumReduceTasks(0);
    RunningJob result = JobClient.runJob(job);
    long uses = result.getCounters().findCounter("jvm", "use").getValue();
    int maps = job.getNumMapTasks();
    if (reuse) {
      assertTrue("maps = " + maps + ", uses = " + uses, maps < uses);
    } else {
      assertEquals("uses should be number of maps", job.getNumMapTasks(), uses);
    }
  }

  public void testMapReduceSort() throws Exception {
    // Run randomwriter to generate input for 'sort'
    runRandomWriter(mrCluster.createJobConf(), SORT_INPUT_PATH);

    // Run sort
    runSort(mrCluster.createJobConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);

    // Run sort-validator to check if sort worked correctly
    runSortValidator(mrCluster.createJobConf(), SORT_INPUT_PATH, 
                     SORT_OUTPUT_PATH);
  }
  
  public void testJvmReuse() throws Exception {
    runJvmReuseTest(mrCluster.createJobConf(), true);
  }

  public void testNoJvmReuse() throws Exception {
    runJvmReuseTest(mrCluster.createJobConf(), false);
  }
}
