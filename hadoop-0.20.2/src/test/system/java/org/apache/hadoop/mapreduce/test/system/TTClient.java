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

package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.system.process.RemoteProcess;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.UtilsForTests;

/**
 * TaskTracker client for system tests. Assumption of the class is that the
 * configuration key is set for the configuration key : {@code
 * mapred.task.tracker.report.address}is set, only the port portion of the
 * address is used.
 */
public class TTClient extends MRDaemonClient<TTProtocol> {

  TTProtocol proxy;
  static final Log LOG = LogFactory.getLog(TTClient.class);

  public TTClient(Configuration conf, RemoteProcess daemon) 
      throws IOException {
    super(conf, daemon);
  }

  @Override
  public synchronized void connect() throws IOException {
    if (isConnected()) {
      return;
    }
    String sockAddrStr = getConf()
        .get("mapred.task.tracker.report.address");
    if (sockAddrStr == null) {
      throw new IllegalArgumentException(
          "TaskTracker report address is not set");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          "TaskTracker report address not correctly configured");
    }
    String port = splits[1];
    String sockAddr = getHostName() + ":" + port;
    InetSocketAddress bindAddr = NetUtils.createSocketAddr(sockAddr);
    proxy = (TTProtocol) RPC.getProxy(TTProtocol.class, TTProtocol.versionID,
        bindAddr, getConf());
    setConnected(true);
  }

  @Override
  public synchronized void disconnect() throws IOException {
    RPC.stopProxy(proxy);
  }

  @Override
  public synchronized TTProtocol getProxy() {
    return proxy;
  }

  /**
   * Gets the last sent status to the {@link JobTracker}. <br/>
   * 
   * @return the task tracker status.
   * @throws IOException
   */
  public TaskTrackerStatus getStatus() throws IOException {
    return getProxy().getStatus();
  }
  
  /**
   * This methods provides the information on the particular task managed
   * by a task tracker has stopped or not. 
   * @param TaskID is id of the task to get the status.
   * @throws IOException if there is an error. 
   * @return true is stopped. 
   */
  public boolean isTaskStopped(TaskID tID) throws IOException {
    int counter = 0;
    if(tID != null && proxy.getTask(tID) != null) {
      TaskStatus.State tState= proxy.getTask(tID).getTaskStatus().getRunState();
      while ( counter < 60) {
        if(tState != TaskStatus.State.RUNNING && 
            tState != TaskStatus.State.UNASSIGNED) {
          break;
        }
        UtilsForTests.waitFor(1000);
        tState= proxy.getTask(tID).getTaskStatus().getRunState();
        counter++;
      }      
    }
    return (counter != 60)? true : false;
  }

  /**
   * Waits till this Tasktracker daemon process is stopped <br/>
   *
   * @return void
   * @throws IOException
   */
  public void waitForTTStop() throws IOException {
    LOG.info("Waiting for Tasktracker:" + getHostName()
        + " to stop.....");
    while (true) {
      try {
        ping();
        LOG.debug(getHostName() +" is waiting state to stop.");
        UtilsForTests.waitFor(10000);
      } catch (Exception exp) {
        LOG.info("TaskTracker : " + getHostName() + " is stopped...");
        break;
      }
    }
  }

  /**
   * Waits till this Tasktracker daemon process is started <br/>
   *
   * @return void
   * @throws IOException
   */
  public void waitForTTStart() throws
     IOException {
    LOG.debug("Waiting for Tasktracker:" + getHostName() + " to come up.");
    while (true) {
      try {
        ping();
        LOG.debug("TaskTracker : " + getHostName() + " is pinging...");
        break;
      } catch (Exception exp) {
        LOG.info(getHostName() + " is waiting to come up.");
        UtilsForTests.waitFor(10000);
      }
    }
  }
}
