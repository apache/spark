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

package org.apache.hadoop.mapred.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;

/**
 * HadoopTestCase that tests the local job runner.
 */
public class TestLocalJobControl extends HadoopTestCase {

  public static final Log LOG = LogFactory.getLog(TestLocalJobControl.class
      .getName());

  /**
   * Initialises a new instance of this test case to use a Local MR cluster and
   * a local filesystem.
   * 
   * @throws IOException If an error occurs initialising this object.
   */
  public TestLocalJobControl() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
  }

  /**
   * This is a main function for testing JobControl class. It first cleans all
   * the dirs it will use. Then it generates some random text data in
   * TestJobControlData/indir. Then it creates 4 jobs: Job 1: copy data from
   * indir to outdir_1 Job 2: copy data from indir to outdir_2 Job 3: copy data
   * from outdir_1 and outdir_2 to outdir_3 Job 4: copy data from outdir to
   * outdir_4 The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1
   * and 2. The job 4 depends on job 3.
   * 
   * Then it creates a JobControl object and add the 4 jobs to the JobControl
   * object. Finally, it creates a thread to run the JobControl object and
   * monitors/reports the job states.
   */
  public void testLocalJobControlDataCopy() throws Exception {

    FileSystem fs = FileSystem.get(createJobConf());
    Path rootDataDir = new Path(System.getProperty("test.build.data", "."),
        "TestLocalJobControlData");
    Path indir = new Path(rootDataDir, "indir");
    Path outdir_1 = new Path(rootDataDir, "outdir_1");
    Path outdir_2 = new Path(rootDataDir, "outdir_2");
    Path outdir_3 = new Path(rootDataDir, "outdir_3");
    Path outdir_4 = new Path(rootDataDir, "outdir_4");

    JobControlTestUtils.cleanData(fs, indir);
    JobControlTestUtils.generateData(fs, indir);

    JobControlTestUtils.cleanData(fs, outdir_1);
    JobControlTestUtils.cleanData(fs, outdir_2);
    JobControlTestUtils.cleanData(fs, outdir_3);
    JobControlTestUtils.cleanData(fs, outdir_4);

    ArrayList<Job> dependingJobs = null;

    ArrayList<Path> inPaths_1 = new ArrayList<Path>();
    inPaths_1.add(indir);
    JobConf jobConf_1 = JobControlTestUtils.createCopyJob(inPaths_1, outdir_1);
    Job job_1 = new Job(jobConf_1, dependingJobs);
    ArrayList<Path> inPaths_2 = new ArrayList<Path>();
    inPaths_2.add(indir);
    JobConf jobConf_2 = JobControlTestUtils.createCopyJob(inPaths_2, outdir_2);
    Job job_2 = new Job(jobConf_2, dependingJobs);

    ArrayList<Path> inPaths_3 = new ArrayList<Path>();
    inPaths_3.add(outdir_1);
    inPaths_3.add(outdir_2);
    JobConf jobConf_3 = JobControlTestUtils.createCopyJob(inPaths_3, outdir_3);
    dependingJobs = new ArrayList<Job>();
    dependingJobs.add(job_1);
    dependingJobs.add(job_2);
    Job job_3 = new Job(jobConf_3, dependingJobs);

    ArrayList<Path> inPaths_4 = new ArrayList<Path>();
    inPaths_4.add(outdir_3);
    JobConf jobConf_4 = JobControlTestUtils.createCopyJob(inPaths_4, outdir_4);
    dependingJobs = new ArrayList<Job>();
    dependingJobs.add(job_3);
    Job job_4 = new Job(jobConf_4, dependingJobs);

    JobControl theControl = new JobControl("Test");
    theControl.addJob(job_1);
    theControl.addJob(job_2);
    theControl.addJob(job_3);
    theControl.addJob(job_4);

    Thread theController = new Thread(theControl);
    theController.start();
    while (!theControl.allFinished()) {
      LOG.debug("Jobs in waiting state: " + theControl.getWaitingJobs().size());
      LOG.debug("Jobs in ready state: " + theControl.getReadyJobs().size());
      LOG.debug("Jobs in running state: " + theControl.getRunningJobs().size());
      LOG.debug("Jobs in success state: "
          + theControl.getSuccessfulJobs().size());
      LOG.debug("Jobs in failed state: " + theControl.getFailedJobs().size());
      LOG.debug("\n");
      try {
        Thread.sleep(5000);
      } catch (Exception e) {

      }
    }

    assertEquals("Some jobs failed", 0, theControl.getFailedJobs().size());
    theControl.stop();
  }

}
