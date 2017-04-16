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

import java.io.*;
import java.net.*;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



/**
 * A JUnit test to test that killing completed jobs does not move them
 * to the failed sate - See JIRA HADOOP-2132
 */
public class TestKillCompletedJob extends TestCase {
  
  
  static Boolean launchWordCount(String fileSys,
                                String jobTracker,
                                JobConf conf,
                                String input,
                                int numMaps,
                                int numReduces) throws IOException {
    final Path inDir = new Path("/testing/wc/input");
    final Path outDir = new Path("/testing/wc/output");
    FileSystem fs = FileSystem.get(URI.create(fileSys), conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    FileSystem.setDefaultUri(conf, fileSys);
    conf.set("mapred.job.tracker", jobTracker);
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

    RunningJob rj = JobClient.runJob(conf);
    JobID jobId = rj.getID();
    
    // Kill the job after it is successful
    if (rj.isSuccessful())
    {
      System.out.println("Job Id:" + jobId + 
        " completed successfully. Killing it now");
      rj.killJob();
    }
    
       
    return rj.isSuccessful();
      
  }

     
  public void testKillCompJob() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 1;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getUri().toString();
      mr = new MiniMRCluster(taskTrackers, namenode, 3);
      JobConf jobConf = new JobConf();
    
      Boolean result;
      final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      result = launchWordCount(namenode, jobTrackerName, jobConf, 
                               "Small text\n",
                               1, 0);
      assertTrue(result);
          
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
}
