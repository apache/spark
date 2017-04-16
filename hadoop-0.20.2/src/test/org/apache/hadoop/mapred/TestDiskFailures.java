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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Verify if TaskTracker's in-memory good mapred local dirs list gets updated
 * properly when disks fail.
 */
public class TestDiskFailures extends ClusterMapReduceTestCase {

  private static final Log LOG = LogFactory.getLog(TestDiskFailures.class);

  private static String localPathRoot = System.getProperty(
      "test.build.data", "/tmp").replace(' ', '+');
  private String DISK_HEALTH_CHECK_INTERVAL = "1000";//1 sec

  @Override
  protected void setUp() throws Exception {
    // Do not start cluster here
  };

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    FileUtil.fullyDelete(new File(localPathRoot));
  };

  /**
   * Make some of the the mapred-local-dirs fail/inaccessible and verify if
   * TaskTracker gets reinited properly.
   * @throws Exception
   */
  public void testDiskFailures() throws Exception {

    FileSystem fs = FileSystem.get(new Configuration());
    Path dir = new Path(localPathRoot, "mapred_local_dirs_base");
    FileSystem.mkdirs(fs, dir, new FsPermission((short)0777));

    Properties props = new Properties();
    props.setProperty(JobConf.MAPRED_LOCAL_DIR_PROPERTY, dir.toUri().getPath());
    // set disk health check interval to a small value (say 4 sec).
    props.setProperty(TaskTracker.DISK_HEALTH_CHECK_INTERVAL_PROPERTY,
        DISK_HEALTH_CHECK_INTERVAL);

    // Let us have 4 mapred-local-dirs per tracker
    final int numMapredLocalDirs = 4;
    startCluster(true, props, numMapredLocalDirs);

    MiniMRCluster cluster = getMRCluster();
    String[] localDirs = cluster.getTaskTrackerLocalDirs(0);

    // Make 1 disk fail and verify if TaskTracker gets re-inited or not and
    // the good mapred local dirs list gets updated properly in TaskTracker.
    prepareDirToFail(localDirs[2]);
    String expectedMapredLocalDirs = localDirs[0] + "," + localDirs[1] + ","
                                     + localDirs[3];
    verifyReinitTaskTrackerAfterDiskFailure(expectedMapredLocalDirs, cluster);

    // Make 2 more disks fail and verify if TaskTracker gets re-inited or not
    // and the good mapred local dirs list gets updated properly in TaskTracker.
    prepareDirToFail(localDirs[0]);
    prepareDirToFail(localDirs[3]);
    expectedMapredLocalDirs = localDirs[1];
    verifyReinitTaskTrackerAfterDiskFailure(expectedMapredLocalDirs, cluster);

    // Fail the remaining single disk(i.e. the remaining good mapred-local-dir).
    prepareDirToFail(localDirs[1]);
    waitForDiskHealthCheck();
    assertTrue(
        "Tasktracker is not dead even though all mapred local dirs became bad.",
        cluster.getTaskTrackerRunner(0).exited);
  }

  /**
   * Wait for the TaskTracker to go for the disk-health-check and (possibly)
   * reinit.
   * DiskHealthCheckInterval is 1 sec. So this wait time should be greater than
   * [1 sec + TT_reinit_execution_time]. Let us have this as 4sec.
   */
  private void waitForDiskHealthCheck() {
    try {
      Thread.sleep(4000);
    } catch(InterruptedException e) {
      LOG.error("Interrupted while waiting for TaskTracker reinit.");
    }
  }

  /**
   * Verify if TaskTracker gets reinited properly after disk failure.
   * @param expectedMapredLocalDirs expected mapred local dirs
   * @param cluster MiniMRCluster in which 1st TaskTracker is supposed to get
   *                reinited because of disk failure
   * @throws IOException
   */
  private void verifyReinitTaskTrackerAfterDiskFailure(
      String expectedMapredLocalDirs, MiniMRCluster cluster)
      throws IOException {
    // Wait for the TaskTracker to get reinited. DiskHealthCheckInterval is
    // 1 sec. So this wait time should be > [1 sec + TT_reinit_execution_time].
    waitForDiskHealthCheck();
    String[] updatedLocalDirs = cluster.getTaskTrackerRunner(0)
        .getTaskTracker().getJobConf().getLocalDirs();
    String seenMapredLocalDirs = StringUtils.arrayToString(updatedLocalDirs);
    LOG.info("ExpectedMapredLocalDirs=" + expectedMapredLocalDirs);
    assertTrue("TaskTracker could not reinit properly after disk failure.",
        expectedMapredLocalDirs.equals(seenMapredLocalDirs));    
  }

  /**
   * Prepare directory for a failure. Replace the given directory on the
   * local FileSystem with a regular file with the same name.
   * This would cause failure of creation of directory in DiskChecker.checkDir()
   * with the same name.
   * @throws IOException 
   */
  private void prepareDirToFail(String dir)
      throws IOException {
    File file = new File(dir);
    FileUtil.fullyDelete(file);
    file.createNewFile();
  }
}
