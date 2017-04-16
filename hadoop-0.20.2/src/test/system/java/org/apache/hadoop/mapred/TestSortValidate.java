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

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;

import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A System test to test the Map-Reduce framework's sort 
 * with a real Map-Reduce Cluster.
 */
public class TestSortValidate {
  // Input/Output paths for sort
  private static final Path SORT_INPUT_PATH = new Path("inputDirectory");
  private static final Path SORT_OUTPUT_PATH = new Path("outputDirectory");

  // make it big enough to cause a spill in the map
  private static final int RW_BYTES_PER_MAP = 3 * 1024 * 1024;
  private static final int RW_MAPS_PER_HOST = 2;

  private MRCluster cluster = null;
  private FileSystem dfs = null;
  private JobClient client = null;

  private static final Log LOG = LogFactory.getLog(TestSortValidate.class);

  public TestSortValidate()
  throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
  }

  @Before
  public void setUp() throws java.lang.Exception {
    cluster.setUp();
    client = cluster.getJTClient().getClient();

    dfs = client.getFs();
    dfs.delete(SORT_INPUT_PATH, true);
    dfs.delete(SORT_OUTPUT_PATH, true);
  }

  @After
  public void after() throws Exception {
    cluster.tearDown();
    dfs.delete(SORT_INPUT_PATH, true);
    dfs.delete(SORT_OUTPUT_PATH, true);
  }

  public void runRandomWriter(Configuration job, Path sortInput) 
  throws Exception {
    // Scale down the default settings for RandomWriter for the test-case
    // Generates NUM_HADOOP_SLAVES * RW_MAPS_PER_HOST * RW_BYTES_PER_MAP
    job.setInt("test.randomwrite.bytes_per_map", RW_BYTES_PER_MAP);
    job.setInt("test.randomwriter.maps_per_host", RW_MAPS_PER_HOST);
    String[] rwArgs = {sortInput.toString()};
 
    runAndVerify(job,new RandomWriter(), rwArgs);
  }

  private void runAndVerify(Configuration job, Tool tool, String[] args)
    throws Exception {

    // This calculates the previous number fo jobs submitted before a new
    // job gets submitted.
    int prevJobsNum = 0;

    // JTProtocol wovenClient
    JTProtocol wovenClient = cluster.getJTClient().getProxy();

    // JobStatus
    JobStatus[] jobStatus = null;

    // JobID
    JobID id = null;

    // RunningJob rJob;
    RunningJob rJob = null;

    // JobInfo jInfo;
    JobInfo jInfo = null;

    //Getting the previous job numbers that are submitted.
    jobStatus = client.getAllJobs();
    prevJobsNum = jobStatus.length;

    // Run RandomWriter
    Assert.assertEquals(ToolRunner.run(job, tool, args), 0);

    //Waiting for the job to appear in the jobstatus
    jobStatus = client.getAllJobs();

    while (jobStatus.length - prevJobsNum == 0) {
      LOG.info("Waiting for the job to appear in the jobStatus");
      Thread.sleep(1000);
      jobStatus = client.getAllJobs();
    }

    //Getting the jobId of the just submitted job
    //The just submitted job is always added in the first slot of jobstatus
    id = jobStatus[0].getJobID();

    rJob = client.getJob(id);

    jInfo = wovenClient.getJobInfo(id);

    //Making sure that the job is complete.
    while (jInfo != null && !jInfo.getStatus().isJobComplete()) {
      Thread.sleep(10000);
      jInfo = wovenClient.getJobInfo(id);
    }

    cluster.getJTClient().verifyCompletedJob(id);
  }
  
  private void runSort(Configuration job, Path sortInput, Path sortOutput) 
  throws Exception {

    job.setInt("io.sort.mb", 1);

    // Setup command-line arguments to 'sort'
    String[] sortArgs = {sortInput.toString(), sortOutput.toString()};
    
    runAndVerify(job,new Sort(), sortArgs);

  }
  
  private void runSortValidator(Configuration job, 
                                       Path sortInput, Path sortOutput) 
  throws Exception {
    String[] svArgs = {"-sortInput", sortInput.toString(), 
                       "-sortOutput", sortOutput.toString()};

    runAndVerify(job,new SortValidator(), svArgs);

  }
 
  @Test 
  public void testMapReduceSort() throws Exception {
    // Run randomwriter to generate input for 'sort'
    runRandomWriter(cluster.getConf(), SORT_INPUT_PATH);

    // Run sort
    runSort(cluster.getConf(), SORT_INPUT_PATH, SORT_OUTPUT_PATH);

    // Run sort-validator to check if sort worked correctly
    runSortValidator(cluster.getConf(), SORT_INPUT_PATH, 
                     SORT_OUTPUT_PATH);
  }
}
