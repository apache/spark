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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import testjar.UserNamePermission;

public class TestTaskOwner {
  private static final Log LOG = LogFactory.getLog(TestTaskOwner.class);
  private static Path outDir = new Path("output");
  private static Path inDir = new Path("input");
  public static MRCluster cluster;

  // The role of this job is to write the user name to the output file
  // which will be parsed

  @BeforeClass
  public static void setUp() throws java.lang.Exception {

    cluster = MRCluster.createCluster(new Configuration());
    cluster.setUp();
    FileSystem fs = inDir.getFileSystem(cluster.getJTClient().getConf());
    // Make sure that all is clean in case last tearDown wasn't successful
    fs.delete(outDir, true);
    fs.delete(inDir, true);

    fs.create(inDir, true);
  }

  @Test
  public void testProcessPermission() throws Exception {
  // The user will submit a job which a plain old map reduce job
  // this job will output the username of the task that is running
  // in the cluster and we will authenticate whether matches
  // with the job that is submitted by the same user.

    Configuration conf = cluster.getJTClient().getConf();
    Job job = new Job(conf, "user name check");

    job.setJarByClass(UserNamePermission.class);
    job.setMapperClass(UserNamePermission.UserNameMapper.class);
    job.setCombinerClass(UserNamePermission.UserNameReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(UserNamePermission.UserNameReducer.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);

    // now verify the user name that is written by the task tracker is same
    // as the
    // user name that was used to launch the task in the first place
    FileSystem fs = outDir.getFileSystem(conf);

    Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
     new Utils.OutputFileUtils.OutputFilesFilter()));

    for (int i = 0; i < fileList.length; ++i) {
	  LOG.info("File list[" + i + "]" + ": " + fileList[i]);
	  BufferedReader file = new BufferedReader(new InputStreamReader(fs
      .open(fileList[i])));
       String line = file.readLine();
       while (line != null) {
         StringTokenizer token = new StringTokenizer(line);
         if (token.hasMoreTokens()) {
           LOG.info("First token " + token.nextToken());
           String userName = token.nextToken();

           LOG.info("Next token " + userName);
           Assert
             .assertEquals(
              "The user name did not match permission violation ",
               userName, System.getProperty("user.name")
              .toString());
           break;
         }
        }
        file.close();
     }
  }

  @AfterClass
  public static void tearDown() throws java.lang.Exception {
    FileSystem fs = outDir.getFileSystem(cluster.getJTClient().getConf());
    fs.delete(outDir, true);
    fs.delete(inDir, true);
    cluster.tearDown();
   }
}


