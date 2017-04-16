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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;

/**
 * Verifying the job submissions with original user and other user
 * with Linux task Controller.Original user should succeed and other
 * user should not.
 */
public class TestLinuxTaskControllerOtherUser {
  private static final Log LOG = LogFactory.
      getLog(TestLinuxTaskControllerOtherUser.class);
  private static MRCluster cluster = null;
  private static JobClient jobClient = null;
  private static JTProtocol remoteJTClient = null;
  private static JTClient jtClient = null;
  private static Configuration conf = new Configuration();
  private static UserGroupInformation proxyUGI = null;
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output"); 

  @BeforeClass
  public static void before() throws Exception {
    cluster = MRCluster.createCluster(conf);
    cluster.setUp();
    jtClient = cluster.getJTClient();
    jobClient = jtClient.getClient();
    remoteJTClient = cluster.getJTClient().getProxy();
    conf = remoteJTClient.getDaemonConf();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
  }
 
  /**
   * Submit a Sleep Job with a diferent user id and verify it failure
   * @param none
   * @return void
   */
  @Test
  public void testSubmitJobDifferentUserJobClient() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    LOG.info("LoginUser:" + ugi);
    if (conf.get("mapred.task.tracker.task-controller").
        equals("org.apache.hadoop.mapred.LinuxTaskController")) {
      //Changing the User name
      proxyUGI = UserGroupInformation.createRemoteUser(
          "hadoop1");
 
      SleepJob job = new SleepJob();
      job.setConf(conf);
      final JobConf jobConf = job.setupJobConf(2, 1, 2000, 2000, 100, 100);
      String error = null;
      RunningJob runJob = null;
      //Getting the jobClient with the changed remote user and 
      //then submit the command.
      try {
        final JobClient jClient =
            proxyUGI.doAs(new PrivilegedExceptionAction<JobClient>() {
          public JobClient run() throws IOException {
            return new JobClient(jobConf);
          }
        });

        runJob = proxyUGI.doAs(
          new PrivilegedExceptionAction<RunningJob>() {
          public RunningJob run() throws IOException {
            return jClient.submitJob(jobConf);
          }
        });
      } catch (Exception e) {error = e.toString();}
      //A error is expected to be thrown
      if (error.indexOf("No valid credentials provided") != -1) {
        LOG.info("e's value is :" + error);
      } else {
        Assert.fail("Some unknown error is thrown :" + error);
      }
      Assert.assertNull("Job is still running", runJob);
    }
  }

  //Cleanup directories in dfs.
  private static void cleanup(Path dir, Configuration conf)
      throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }
}
