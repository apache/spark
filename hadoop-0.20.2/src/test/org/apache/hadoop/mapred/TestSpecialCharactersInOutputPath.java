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

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Progressable;

/**
 * A JUnit test to test that jobs' output filenames are not HTML-encoded (cf HADOOP-1795).
 */
public class TestSpecialCharactersInOutputPath extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestSpecialCharactersInOutputPath.class.getName());
  
  private static final String OUTPUT_FILENAME = "result[0]";
  
  public static boolean launchJob(String fileSys,
                                       String jobTracker,
                                       JobConf conf,
                                       int numMaps,
                                       int numReduces) throws IOException {
    
    final Path inDir = new Path("/testing/input");
    final Path outDir = new Path("/testing/output");
    FileSystem fs = FileSystem.getNamed(fileSys, conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      LOG.warn("Can't create " + inDir);
      return false;
    }
    // generate an input file
    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("foo foo2 foo3");
    file.close();

    // use WordCount example
    FileSystem.setDefaultUri(conf, fileSys);
    conf.set("mapred.job.tracker", jobTracker);
    conf.setJobName("foo");

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(SpecialTextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(IdentityMapper.class);        
    conf.setReducerClass(IdentityReducer.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
      
    // run job and wait for completion
    RunningJob runningJob = JobClient.runJob(conf);
      
    try {
      assertTrue(runningJob.isComplete());
      assertTrue(runningJob.isSuccessful());
      assertTrue("Output folder not found!", fs.exists(new Path("/testing/output/" + OUTPUT_FILENAME)));
    } catch (NullPointerException npe) {
      // This NPE should no more happens
      fail("A NPE should not have happened.");
    }
          
    // return job result
    LOG.info("job is complete: " + runningJob.isSuccessful());
    return (runningJob.isSuccessful());
  }
  
  public void testJobWithDFS() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;
      final int jobTrackerPort = 60050;
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getName();
      mr = new MiniMRCluster(taskTrackers, namenode, 2);
      final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      JobConf jobConf = new JobConf();
      boolean result;
      result = launchJob(namenode, jobTrackerName, jobConf, 
                              3, 1);
      assertTrue(result);
          
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  /** generates output filenames with special characters */
  static class SpecialTextOutputFormat<K,V> extends TextOutputFormat<K,V> {
    @Override
    public RecordWriter<K,V> getRecordWriter(FileSystem ignored, JobConf job,
             String name, Progressable progress) throws IOException {
        return super.getRecordWriter(ignored, job, OUTPUT_FILENAME, progress);
    }
  }
}
