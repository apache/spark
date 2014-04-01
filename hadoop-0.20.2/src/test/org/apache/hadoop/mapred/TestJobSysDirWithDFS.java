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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

/**
 * A JUnit test to test Job System Directory with Mini-DFS.
 */
public class TestJobSysDirWithDFS extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestJobSysDirWithDFS.class.getName());
  
  static final int NUM_MAPS = 10;
  static final int NUM_SAMPLES = 100000;
  
  public static class TestResult {
    public String output;
    public RunningJob job;
    TestResult(RunningJob job, String output) {
      this.job = job;
      this.output = output;
    }
  }

  public static TestResult launchWordCount(JobConf conf,
                                           Path inDir,
                                           Path outDir,
                                           String input,
                                           int numMaps,
                                           int numReduces,
                                           String sysDir) throws IOException {
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);
    
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(WordCount.MapClass.class);        
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    conf.set("mapred.system.dir", "/tmp/subru/mapred/system");
    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.runJob(conf);
    // Checking that the Job Client system dir is not used
    assertFalse(FileSystem.get(conf).exists(new Path(conf.get("mapred.system.dir")))); 
    // Check if the Job Tracker system dir is propogated to client
    sysDir = jobClient.getSystemDir().toString();
    System.out.println("Job sys dir -->" + sysDir);
    assertFalse(sysDir.contains("/tmp/subru/mapred/system"));
    assertTrue(sysDir.contains("custom"));
    return new TestResult(job, MapReduceTestUtil.readOutput(outDir, conf));
  }

 static void runWordCount(MiniMRCluster mr, JobConf jobConf, String sysDir)
 throws IOException {
    LOG.info("runWordCount");
    // Run a word count example
    // Keeping tasks that match this pattern
    TestResult result;
    final Path inDir = new Path("./wc/input");
    final Path outDir = new Path("./wc/output");
    result = launchWordCount(jobConf, inDir, outDir,
                             "The quick brown fox\nhas many silly\n" + 
                             "red fox sox\n",
                             3, 1, sysDir);
    assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                 "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
    // Checking if the Job ran successfully in spite of different system dir config
    //  between Job Client & Job Tracker
    assertTrue(result.job.isSuccessful());
  }

  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      JobConf conf = new JobConf();
      conf.set("mapred.system.dir", "/tmp/custom/mapred/system");
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1, null, null, conf);

      runWordCount(mr, mr.createJobConf(), conf.get("mapred.system.dir"));
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
}
