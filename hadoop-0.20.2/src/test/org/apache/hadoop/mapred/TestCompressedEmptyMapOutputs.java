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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

/**
 * A JUnit test to test the Map-Reduce framework's sort in presence of 
 * null intermediate map-outputs, when compression is enabled for intermediate
 * map-outputs. 
 */
public class TestCompressedEmptyMapOutputs extends TestCase {
  // Input/Output paths for sort
  private static final Path SORT_INPUT_PATH = new Path("/sort/input");
  private static final Path SORT_OUTPUT_PATH = new Path("/sort/output");

  // Knobs to control randomwriter; and hence sort
  private static final int NUM_HADOOP_SLAVES = 3;
  private static final int RW_BYTES_PER_MAP = 50000;
  private static final int RW_MAPS_PER_HOST = 5;
  
  private static void runRandomWriter(JobConf job, Path sortInput) 
  throws Exception {
    // Scale down the default settings for RandomWriter for the test-case
    // Generates NUM_HADOOP_SLAVES * RW_MAPS_PER_HOST * RW_BYTES_PER_MAP -> 1MB
    job.setInt("test.randomwrite.bytes_per_map", RW_BYTES_PER_MAP);
    job.setInt("test.randomwriter.maps_per_host", RW_MAPS_PER_HOST);
    String[] rwArgs = {sortInput.toString()};
    
    // Run RandomWriter
    assertEquals(ToolRunner.run(job, new RandomWriter(), rwArgs), 0);
  }


  static class SinkMapper<K, V>
  extends MapReduceBase implements Mapper<K, V, K, V> {

    public void map(K key, V val,
        OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
      // Don't output anything!
      if (false) output.collect(key, val);
    }
  }

  private static void runSort(JobConf jobConf, Path sortInput, Path sortOutput) 
  throws Exception {
    // Set up the job
    jobConf.setJobName("null-sorter");
    
    jobConf.setMapperClass(SinkMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);

    jobConf.setNumReduceTasks(2);

    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    
    FileInputFormat.setInputPaths(jobConf, sortInput);
    FileOutputFormat.setOutputPath(jobConf, sortOutput);

    // Compress the intermediate map-outputs!
    jobConf.setCompressMapOutput(true);

    // Run the job
    JobClient.runJob(jobConf);
  }
  
  public void testMapReduceSortWithCompressedEmptyMapOutputs() 
  throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      Configuration conf = new Configuration();

      // Start the mini-MR and mini-DFS clusters
      dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri().toString(), 1);

      // Run randomwriter to generate input for 'sort'
      runRandomWriter(mr.createJobConf(), SORT_INPUT_PATH);
      
      // Run sort
      runSort(mr.createJobConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }

}
