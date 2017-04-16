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
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.*;

/**
 * Test if JobTracker is resilient to garbage in mapred.system.dir.
 */
public class TestMapredSystemDir extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMapredSystemDir.class);
  
  // mapred ugi
  private static final UserGroupInformation MR_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("mr", false);
  private static final FsPermission SYSTEM_DIR_PARENT_PERMISSION =
    FsPermission.createImmutable((short) 0755); // rwxr-xr-x
  private static final FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------

  public void testGarbledMapredSystemDir() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      // start dfs
      conf.set("dfs.permissions.supergroup", "supergroup");
      conf.set("mapred.system.dir", "/mapred");
      dfs = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = dfs.getFileSystem();

      // create Configs.SYSTEM_DIR's parent with restrictive permissions.
      // So long as the JT has access to the system dir itself it should
      // be able to start.
      Path mapredSysDir = new Path(conf.get("mapred.system.dir"));
      Path parentDir =  mapredSysDir.getParent();
      fs.mkdirs(parentDir);
      fs.setPermission(parentDir,
                       new FsPermission(SYSTEM_DIR_PARENT_PERMISSION));
      fs.mkdirs(mapredSysDir);
      fs.setPermission(mapredSysDir,
                       new FsPermission(SYSTEM_DIR_PERMISSION));
      fs.setOwner(mapredSysDir, "mr", "mrgroup");

      final MiniDFSCluster finalDFS = dfs;

      // Become MR_UGI to do start the job tracker...
      mr = MR_UGI.doAs(new PrivilegedExceptionAction<MiniMRCluster>() {
        @Override
        public MiniMRCluster run() throws Exception {
          // start mr (i.e jobtracker)
          Configuration mrConf = new Configuration(conf);
          
          FileSystem fs = finalDFS.getFileSystem();
          MiniMRCluster mr2 = new MiniMRCluster(0, 0, 0, fs.getUri().toString(),
              1, null, null, MR_UGI, new JobConf(mrConf));
          JobTracker jobtracker = mr2.getJobTrackerRunner().getJobTracker();
          // add garbage to mapred.system.dir
          Path garbage = new Path(jobtracker.getSystemDir(), "garbage");
          fs.mkdirs(garbage);
          fs.setPermission(garbage, new FsPermission(SYSTEM_DIR_PERMISSION));
          return mr2;
        }
      });
      
      // Drop back to regular user (superuser) to change owner of garbage dir
      final Path garbage = new Path(
          mr.getJobTrackerRunner().getJobTracker().getSystemDir(), "garbage");
      fs.setOwner(garbage, "test", "test-group");
      
      // Again become MR_UGI to start/stop the MR cluster
      final MiniMRCluster mr2 = mr;
      MR_UGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // stop the jobtracker
          mr2.stopJobTracker();
          mr2.getJobTrackerConf().setBoolean(
              "mapred.jobtracker.restart.recover", false);
          // start jobtracker but dont wait for it to be up
          mr2.startJobTracker(false);

          // check 5 times .. each time wait for 2 secs to check if the
          // jobtracker
          // has crashed or not.
          for (int i = 0; i < 5; ++i) {
            LOG.info("Check #" + i);
            if (!mr2.getJobTrackerRunner().isActive()) {
              return null;
            }
            UtilsForTests.waitFor(2000);
          }
          return null;
        }
      });

      assertFalse("JobTracker did not bail out (waited for 10 secs)", 
                  mr.getJobTrackerRunner().isActive());
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();}
    }
  }
}
