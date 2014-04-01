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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * A test-cluster based on {@link MiniMRCluster} that is started with
 * CapacityTaskScheduler. It provides knobs to configure both the cluster as
 * well as the scheduler. Any test that intends to test capacity-scheduler
 * should extend this.
 * 
 */
public class ClusterWithCapacityScheduler extends TestCase {

  static final Log LOG = LogFactory.getLog(ClusterWithCapacityScheduler.class);
  private MiniMRCluster mrCluster;

  private JobConf jobConf;

  static final String MY_SCHEDULER_CONF_PATH_PROPERTY = "my.resource.path";

  protected void startCluster()
      throws IOException {
    startCluster(null, null);
  }

  /**
   * Start the cluster with two TaskTrackers and two DataNodes and configure the
   * cluster with clusterProperties and the scheduler with schedulerProperties.
   * Uses default configuration whenever user provided properties are missing
   * (null/empty)
   * 
   * @param clusterProperties
   * @param schedulerProperties
   * @throws IOException
   */
  protected void startCluster(Properties clusterProperties,
      Properties schedulerProperties)
      throws IOException {
    startCluster(2, clusterProperties, schedulerProperties);
  }

  /**
   * Start the cluster with numTaskTrackers TaskTrackers and numDataNodes
   * DataNodes and configure the cluster with clusterProperties and the
   * scheduler with schedulerProperties. Uses default configuration whenever
   * user provided properties are missing (null/empty)
   * 
   * @param numTaskTrackers
   * @param clusterProperties
   * @param schedulerProperties
   * @throws IOException
   */
  protected void startCluster(int numTaskTrackers,
      Properties clusterProperties, Properties schedulerProperties)
      throws IOException {
    Thread.currentThread().setContextClassLoader(
        new ClusterWithCapacityScheduler.MyClassLoader());
    JobConf clusterConf = new JobConf();
    if (clusterProperties != null) {
      for (Enumeration<?> e = clusterProperties.propertyNames(); e
          .hasMoreElements();) {
        String key = (String) e.nextElement();
        clusterConf.set(key, (String) clusterProperties.get(key));
      }
    }

    if (schedulerProperties != null) {
      setUpSchedulerConfigFile(schedulerProperties);
    }

    clusterConf.set("mapred.jobtracker.taskScheduler",
        CapacityTaskScheduler.class.getName());
    mrCluster =
        new MiniMRCluster(numTaskTrackers, "file:///", 1, null, null,
            clusterConf);

    this.jobConf = mrCluster.createJobConf(clusterConf);
  }

  private void setUpSchedulerConfigFile(Properties schedulerConfProps)
      throws IOException {
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());

    String myResourcePath = System.getProperty("test.build.data");
    Path schedulerConfigFilePath =
        new Path(myResourcePath, CapacitySchedulerConf.SCHEDULER_CONF_FILE);
    OutputStream out = fs.create(schedulerConfigFilePath);

    Configuration config = new Configuration(false);
    for (Enumeration<?> e = schedulerConfProps.propertyNames(); e
        .hasMoreElements();) {
      String key = (String) e.nextElement();
      LOG.debug("Adding " + key + schedulerConfProps.getProperty(key));
      config.set(key, schedulerConfProps.getProperty(key));
    }

    config.writeXml(out);
    out.close();

    LOG.info("setting resource path where capacity-scheduler's config file "
        + "is placed to " + myResourcePath);
    System.setProperty(MY_SCHEDULER_CONF_PATH_PROPERTY, myResourcePath);
  }

  private void cleanUpSchedulerConfigFile() throws IOException {
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());

    String myResourcePath = System.getProperty("test.build.data");
    Path schedulerConfigFilePath =
        new Path(myResourcePath, CapacitySchedulerConf.SCHEDULER_CONF_FILE);
    fs.delete(schedulerConfigFilePath, false);
  }

  protected JobConf getJobConf() {
    return new JobConf(this.jobConf);
  }

  protected JobTracker getJobTracker() {
    return this.mrCluster.getJobTrackerRunner().getJobTracker();
  }

  @Override
  protected void tearDown()
      throws Exception {
    cleanUpSchedulerConfigFile();
    
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
  }

  /**
   * Wait till all the slots in the cluster are occupied with respect to the
   * tasks of type specified isMap.
   * 
   * <p>
   * 
   * <b>Also, it is assumed that the tasks won't finish any time soon, like in
   * the case of tasks of {@link ControlledMapReduceJob}</b>.
   * 
   * @param isMap
   */
  protected void waitTillAllSlotsAreOccupied(boolean isMap)
      throws InterruptedException {
    JobTracker jt = this.mrCluster.getJobTrackerRunner().getJobTracker();
    ClusterStatus clusterStatus = jt.getClusterStatus();
    int currentTasks =
        (isMap ? clusterStatus.getMapTasks() : clusterStatus.getReduceTasks());
    int maxTasks =
        (isMap ? clusterStatus.getMaxMapTasks() : clusterStatus
            .getMaxReduceTasks());
    while (currentTasks != maxTasks) {
      Thread.sleep(1000);
      clusterStatus = jt.getClusterStatus();
      currentTasks =
          (isMap ? clusterStatus.getMapTasks() : clusterStatus
              .getReduceTasks());
      maxTasks =
          (isMap ? clusterStatus.getMaxMapTasks() : clusterStatus
              .getMaxReduceTasks());
      LOG.info("Waiting till cluster reaches steady state. currentTasks : "
          + currentTasks + " total cluster capacity : " + maxTasks);
    }
  }

  /**
   * @return the mrCluster
   */
  public MiniMRCluster getMrCluster() {
    return mrCluster;
  }

  static class MyClassLoader extends ClassLoader {
    @Override
    public URL getResource(String name) {
      if (!name.equals(CapacitySchedulerConf.SCHEDULER_CONF_FILE)) {
        return super.getResource(name);
      }
      return findResource(name);
    }

    @Override
    protected URL findResource(String name) {
      try {
        String resourcePath =
            System
                .getProperty(ClusterWithCapacityScheduler.MY_SCHEDULER_CONF_PATH_PROPERTY);
        // Check the resourcePath directory
        File file = new File(resourcePath, name);
        if (file.exists()) {
          return new URL("file://" + file.getAbsolutePath());
        }
      } catch (MalformedURLException mue) {
        LOG.warn("exception : " + mue);
      }
      return super.findResource(name);
    }
  }
}
