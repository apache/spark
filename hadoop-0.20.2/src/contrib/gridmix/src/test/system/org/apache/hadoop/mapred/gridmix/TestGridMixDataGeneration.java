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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.gridmix.RoundRobinUserResolver;
import org.apache.hadoop.mapred.gridmix.EchoUserResolver;
import org.apache.hadoop.mapred.gridmix.SubmitterUserResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;
import java.io.IOException;

/**
 * Verify the Gridmix data generation with various submission policies and 
 * user resolver modes.
 */
public class TestGridMixDataGeneration {
  private static final Log LOG = 
      LogFactory.getLog(TestGridMixDataGeneration.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static JTClient jtClient;
  private static JTProtocol rtClient;
  private static Path gridmixDir;
  private static int cSize;

  @BeforeClass
  public static void before() throws Exception {
    String [] excludeExpList = {"java.net.ConnectException", 
        "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(excludeExpList);
    cluster.setUp();
    cSize = cluster.getTTClients().size();
    jtClient = cluster.getJTClient();
    rtClient = jtClient.getProxy();
    gridmixDir = new Path("hdfs:///user/" + UtilsForGridmix.getUserName()
       + "/herriot-gridmix");
    UtilsForGridmix.createDirs(gridmixDir, rtClient.getDaemonConf());
  }

  @AfterClass
  public static void after() throws Exception {
    UtilsForGridmix.cleanup(gridmixDir,conf);
    cluster.tearDown();
  }
  
  /**
   * Generate the data in a STRESS submission policy with SubmitterUserResolver 
   * mode and verify whether the generated data matches with given 
   * input size or not.
   * @throws IOException
   */
  @Test
  public void testGenerateDataWithSTRESSSubmission() throws Exception {
    conf = rtClient.getDaemonConf();
    final long inputSize = cSize * 128;
    String [] runtimeValues ={"LOADJOB",
       SubmitterUserResolver.class.getName(),
       "STRESS",
       inputSize+"m",
       "file:///dev/null"}; 

    int exitCode = UtilsForGridmix.runGridmixJob(gridmixDir, 
      conf,GridMixRunMode.DATA_GENERATION, runtimeValues);
    Assert.assertEquals("Data generation has failed.", 0 , exitCode);
    checkGeneratedDataAndJobStatus(inputSize);
  }
  
  /**
   * Generate the data in a REPLAY submission policy with RoundRobinUserResolver
   * mode and verify whether the generated data matches with the given 
   * input size or not.
   * @throws Exception
   */
  @Test
  public void testGenerateDataWithREPLAYSubmission() throws Exception {
    conf = rtClient.getDaemonConf();
    final long inputSize = cSize * 300;
    String [] runtimeValues ={"LOADJOB",
       RoundRobinUserResolver.class.getName(),
       "REPLAY",
       inputSize +"m",
       "file://" + cluster.getProxyUsersFilePath(),
       "file:///dev/null"};
    
    int exitCode = UtilsForGridmix.runGridmixJob(gridmixDir, 
       conf,GridMixRunMode.DATA_GENERATION, runtimeValues);
    Assert.assertEquals("Data generation has failed.", 0 , exitCode);
    checkGeneratedDataAndJobStatus(inputSize); 
  }
  
  /**
   * Generate the data in a SERIAL submission policy with EchoUserResolver
   * mode and also set the no.of bytes per file in the data.Verify whether each 
   * file size matches with given per file size or not and also 
   * verify the overall size of generated data.
   * @throws Exception
   */
  @Test
  public void testGenerateDataWithSERIALSubmission() throws Exception {
    conf = rtClient.getDaemonConf();
    int perNodeSize = 500; // 500 mb per node data
    final long inputSize = cSize * perNodeSize;
    String [] runtimeValues ={"LOADJOB",
       EchoUserResolver.class.getName(),
       "SERIAL",
       inputSize + "m",
       "file:///dev/null"};
    int bytesPerFile = 200; // 200 mb per file of data
    String [] otherArgs = {
      "-D", GridMixConfig.GRIDMIX_BYTES_PER_FILE + 
      "=" + (bytesPerFile * 1024 * 1024)
    };
    int exitCode = UtilsForGridmix.runGridmixJob(gridmixDir, 
       conf,GridMixRunMode.DATA_GENERATION, runtimeValues,otherArgs);
    Assert.assertEquals("Data generation has failed.", 0 , exitCode);
    LOG.info("Verify the eache file size in a generate data.");
    verifyEachNodeSize(new Path(gridmixDir,"input"));
    verifyNumOfFilesGeneratedInEachNode(new Path(gridmixDir,"input"), 
       perNodeSize, bytesPerFile);
    checkGeneratedDataAndJobStatus(inputSize);
  }
  
  private void checkGeneratedDataAndJobStatus(long inputSize) 
     throws IOException {
    LOG.info("Verify the generated data size.");
    long dataSize = getDataSize(new Path(gridmixDir,"input"));
    Assert.assertTrue("Generate data has not matched with given size",
       dataSize + 0.1 > inputSize || dataSize - 0.1 < inputSize);
 
    JobClient jobClient = jtClient.getClient();
    LOG.info("Verify the job status after completion of job.");
    Assert.assertEquals("Job has not succeeded.", JobStatus.SUCCEEDED, 
       jobClient.getAllJobs()[0].getRunState());
  }
  
  private void verifyEachNodeSize(Path inputDir) throws IOException {
    FileSystem fs = inputDir.getFileSystem(conf);
    FileStatus [] fstatus = fs.listStatus(inputDir);
    for (FileStatus fstat : fstatus) {
      if ( fstat.isDir()) {
        long fileSize = getDataSize(fstat.getPath());
        Assert.assertTrue("The Size has not " + 
           " matched with given per node file size(500mb)", 
           fileSize + 0.1 > 500 || fileSize - 0.1 < 500);
      }
    }    
  }

  private void verifyNumOfFilesGeneratedInEachNode(Path inputDir, 
     int nodeSize, int fileSize) throws IOException {
    int expFileCount = Math.round(nodeSize/fileSize) + 
       ((nodeSize%fileSize != 0)? 1:0);
    FileSystem fs = inputDir.getFileSystem(conf);
    FileStatus [] fstatus = fs.listStatus(inputDir);
    for (FileStatus fstat : fstatus) {
      if ( fstat.isDir()) {
        FileSystem nodeFs = fstat.getPath().getFileSystem(conf);
        long actFileCount = nodeFs.getContentSummary(fstat.getPath())
           .getFileCount();
        Assert.assertEquals("File count has not matched.", 
           expFileCount, actFileCount);
      }
    }
  }

  private static long getDataSize(Path inputDir) throws IOException {
    FileSystem fs = inputDir.getFileSystem(conf);
    ContentSummary csmry = fs.getContentSummary(inputDir);
    long dataSize = csmry.getLength();
    dataSize = dataSize/(1024 * 1024);
    return dataSize;
  }
}
