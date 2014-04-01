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
import org.apache.hadoop.examples.SleepJob.SleepInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import junit.framework.TestCase;

public class TestTrackerBlacklistAcrossJobs extends TestCase {
  private static final String hosts[] = new String[] {
    "host1.rack.com", "host2.rack.com", "host3.rack.com"
  };
  final Path inDir = new Path("/testing");
  final Path outDir = new Path("/output");

  public static class SleepJobFailOnHost extends MapReduceBase
    implements Mapper<IntWritable, IntWritable, IntWritable, NullWritable> {
    String hostname = "";
    
    public void configure(JobConf job) {
      this.hostname = job.get("slave.host.name");
    }
    
    public void map(IntWritable key, IntWritable value,
                    OutputCollector<IntWritable, NullWritable> output,
                    Reporter reporter)
    throws IOException {
      if (this.hostname.equals(hosts[0])) {
        // fail here
        throw new IOException("failing on host: " + hosts[0]);
      }
    }
  }
  
  public void testBlacklistAcrossJobs() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    Configuration conf = new Configuration();
    // setup dfs and input
    dfs = new MiniDFSCluster(conf, 1, true, null, hosts);
    fileSys = dfs.getFileSystem();
    if (!fileSys.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    UtilsForTests.writeFile(dfs.getNameNode(), conf, 
                                 new Path(inDir + "/file"), (short) 1);
    // start mr cluster
    JobConf jtConf = new JobConf();
    jtConf.setInt("mapred.max.tracker.blacklists", 1);
    mr = new MiniMRCluster(3, fileSys.getUri().toString(),
                           1, null, hosts, jtConf);

    // setup job configuration
    JobConf mrConf = mr.createJobConf();
    JobConf job = new JobConf(mrConf);
    job.setInt("mapred.max.tracker.failures", 1);
    job.setNumMapTasks(30);
    job.setNumReduceTasks(0);
    job.setMapperClass(SleepJobFailOnHost.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setInputFormat(SleepInputFormat.class);
    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);
    
    // run the job
    JobClient jc = new JobClient(mrConf);
    RunningJob running = JobClient.runJob(job);
    assertEquals("Job failed", JobStatus.SUCCEEDED, running.getJobState());
    assertEquals("Didn't blacklist the host", 1, 
      jc.getClusterStatus().getBlacklistedTrackers());
    assertEquals("Fault count should be 1", 1, mr.getFaultCount(hosts[0]));

    // run the same job once again 
    // there should be no change in blacklist count
    running = JobClient.runJob(job);
    assertEquals("Job failed", JobStatus.SUCCEEDED, running.getJobState());
    assertEquals("Didn't blacklist the host", 1,
      jc.getClusterStatus().getBlacklistedTrackers());
    assertEquals("Fault count should be 1", 1, mr.getFaultCount(hosts[0]));
  }
}
