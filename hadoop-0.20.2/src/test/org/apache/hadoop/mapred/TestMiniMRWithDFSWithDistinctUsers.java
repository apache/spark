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
import java.security.PrivilegedExceptionAction;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.*;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 */
public class TestMiniMRWithDFSWithDistinctUsers extends TestCase {
  static final UserGroupInformation DFS_UGI = createUGI("dfs", true);
  static final UserGroupInformation PI_UGI = createUGI("pi", false);
  static final UserGroupInformation WC_UGI = createUGI("wc", false);

  static UserGroupInformation createUGI(String name, boolean issuper) {
    String group = issuper? "supergroup": name;
    return UserGroupInformation.createUserForTesting(name,new String[]{group});
  }
  
  static void mkdir(FileSystem fs, String dir) throws IOException {
    Path p = new Path(dir);
    fs.mkdirs(p);
    fs.setPermission(p, new FsPermission((short)0777));
  }

  // runs a sample job as a user (ugi)
  RunningJob runJobAsUser(final JobConf job, UserGroupInformation ugi)
  throws Exception {
    JobSubmissionProtocol jobSubmitClient =
      TestSubmitJob.getJobSubmitClient(job, ugi);
    JobID id = jobSubmitClient.getNewJobId();
   
    InputSplit[] splits = computeJobSplit(JobID.downgrade(id), job);
    final Path jobSubmitDir = new Path(id.toString());
    FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        return jobSubmitDir.getFileSystem(job);
      }
    });
    Path qJobSubmitDir = jobSubmitDir.makeQualified(fs);
    uploadJobFiles(JobID.downgrade(id), splits, qJobSubmitDir, ugi, job);
   
    jobSubmitClient.submitJob(id, qJobSubmitDir.toString(), null);
   
    JobClient jc = new JobClient(job);
    return jc.getJob(JobID.downgrade(id));
  }
 
  // a helper api for split computation
  private InputSplit[] computeJobSplit(JobID id, JobConf conf)
  throws IOException {
    InputSplit[] splits =
      conf.getInputFormat().getSplits(conf, conf.getNumMapTasks());
    conf.setNumMapTasks(splits.length);
    return splits;
  }


  // a helper api for split submission
  private void uploadJobFiles(JobID id, InputSplit[] splits,
                             Path jobSubmitDir, UserGroupInformation ugi,
                             final JobConf conf)
  throws Exception {
    final Path confLocation = JobSubmissionFiles.getJobConfPath(jobSubmitDir);
    FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        return confLocation.getFileSystem(conf);
      }
    });
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf, fs, splits);
    FsPermission perm = new FsPermission((short)0700);
   
    // localize conf
    DataOutputStream confOut = FileSystem.create(fs, confLocation, perm);
    conf.writeXml(confOut);
    confOut.close();
  }
 
  public void testDistinctUsers() throws Exception {
    MiniMRCluster mr = null;
    Configuration conf = new Configuration();
    final MiniDFSCluster dfs = new MiniDFSCluster(conf, 4, true, null);
    try {
      FileSystem fs = DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return dfs.getFileSystem();
        }
      });
      mkdir(fs, "/user");
      mkdir(fs, "/mapred");

      UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser();
      mr = new MiniMRCluster(0, 0, 4, dfs.getFileSystem().getUri().toString(),
           1, null, null, MR_UGI);

      String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      JobConf job1 = mr.createJobConf();
      String input = "The quick brown fox\nhas many silly\n"
                     + "red fox sox\n";
      Path inDir = new Path("/testing/distinct/input");
      Path outDir = new Path("/testing/distinct/output");
      TestMiniMRClasspath.configureWordCount(fs, jobTrackerName, job1,
                                             input, 2, 1, inDir, outDir);

      runJobAsUser(job1, PI_UGI);

      JobConf job2 = mr.createJobConf();
      Path inDir2 = new Path("/testing/distinct/input2");
      Path outDir2 = new Path("/testing/distinct/output2");
      TestMiniMRClasspath.configureWordCount(fs, jobTrackerName, job2,
                                             input, 2, 1, inDir2, outDir2);
      runJobAsUser(job2, WC_UGI);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();}
    }
  }
}
