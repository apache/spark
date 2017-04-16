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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.test.system.AbstractDaemonCluster;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster;
import org.apache.hadoop.test.system.process.MultiUserHadoopDaemonRemoteCluster;
import org.apache.hadoop.test.system.process.RemoteProcess;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster.HadoopDaemonInfo;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskID;
import java.util.Collection;
import org.apache.hadoop.mapred.UtilsForTests;

/**
 * Concrete AbstractDaemonCluster representing a Map-Reduce cluster.
 * 
 */
@SuppressWarnings("unchecked")
public class MRCluster extends AbstractDaemonCluster {

  private static final Log LOG = LogFactory.getLog(MRCluster.class);
  public static final String CLUSTER_PROCESS_MGR_IMPL = 
    "test.system.mr.clusterprocess.impl.class";

  /**
   * Key is used to to point to the file containing hostnames of tasktrackers
   */
  public static final String CONF_HADOOP_TT_HOSTFILE_NAME =
    "test.system.hdrc.tt.hostfile";

  private static List<HadoopDaemonInfo> mrDaemonInfos = 
    new ArrayList<HadoopDaemonInfo>();
  private static String TT_hostFileName;
  private static String jtHostName;

  public enum Role {JT, TT};

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  private MRCluster(Configuration conf, ClusterProcessManager rCluster)
      throws IOException {
    super(conf, rCluster);
  }

  /**
   * Factory method to create an instance of the Map-Reduce cluster.<br/>
   * 
   * @param conf
   *          contains all required parameter to create cluster.
   * @return a cluster instance to be managed.
   * @throws Exception
   */
  public static MRCluster createCluster(Configuration conf) 
      throws Exception {
    conf.addResource("system-test.xml");
    TT_hostFileName = conf.get(CONF_HADOOP_TT_HOSTFILE_NAME, "slaves");
    String jtHostPort = conf.get("mapred.job.tracker");
    if (jtHostPort == null) {
      throw new Exception("mapred.job.tracker is not set.");
    }
    jtHostName = jtHostPort.trim().split(":")[0];
    
    mrDaemonInfos.add(new HadoopDaemonInfo("jobtracker", 
        Role.JT, Arrays.asList(new String[]{jtHostName})));
    mrDaemonInfos.add(new HadoopDaemonInfo("tasktracker", 
        Role.TT, TT_hostFileName));
    
    String implKlass = conf.get(CLUSTER_PROCESS_MGR_IMPL);
    if (implKlass == null || implKlass.isEmpty()) {
      implKlass = MRProcessManager.class.getName();
    }
    Class<ClusterProcessManager> klass = (Class<ClusterProcessManager>) Class
      .forName(implKlass);
    ClusterProcessManager clusterProcessMgr = klass.newInstance();
    LOG.info("Created ClusterProcessManager as " + implKlass);
    clusterProcessMgr.init(conf);
    return new MRCluster(conf, clusterProcessMgr);
  }

  protected JTClient createJTClient(RemoteProcess jtDaemon)
      throws IOException {
    return new JTClient(getConf(), jtDaemon);
  }

  protected TTClient createTTClient(RemoteProcess ttDaemon) 
      throws IOException {
    return new TTClient(getConf(), ttDaemon);
  }

  public JTClient getJTClient() {
    Iterator<AbstractDaemonClient> it = getDaemons().get(Role.JT).iterator();
    return (JTClient) it.next();
  }

  public List<TTClient> getTTClients() {
    return (List) getDaemons().get(Role.TT);
  }

  public TTClient getTTClient(String hostname) {
    for (TTClient c : getTTClients()) {
      if (c.getHostName().equals(hostname)) {
        return c;
      }
    }
    return null;
  }
  
  /**
   * This function will give access to one of many TTClient present
   * @return an Instance of TTclient 
   */
  public TTClient getTTClient() {
    for (TTClient c: getTTClients()) {
      if (c != null){
        return c;
      }
    }
    return null;
  }

  @Override
  public void ensureClean() throws IOException {
    //TODO: ensure that no jobs/tasks are running
    //restart the cluster if cleanup fails
    JTClient jtClient = getJTClient();
    JobInfo[] jobs = jtClient.getProxy().getAllJobInfo();
    for(JobInfo job : jobs) {
      jtClient.getClient().killJob(
          org.apache.hadoop.mapred.JobID.downgrade(job.getID()));
    }
  }
  /**
    * Allow the job to continue through MR control job.
    * @param id of the job. 
    * @throws IOException when failed to get task info. 
    */
  public void signalAllTasks(JobID id) throws IOException{
    TaskInfo[] taskInfos = getJTClient().getProxy().getTaskInfo(id);
    if(taskInfos !=null) {
      for (TaskInfo taskInfoRemaining : taskInfos) {
        if(taskInfoRemaining != null) {
          FinishTaskControlAction action = new FinishTaskControlAction(TaskID
              .downgrade(taskInfoRemaining.getTaskID()));
          Collection<TTClient> tts = getTTClients();
          for (TTClient cli : tts) {
            cli.getProxy().sendAction(action);
          }
        }
      }  
    }
  }

  @Override
  protected AbstractDaemonClient createClient(
      RemoteProcess process) throws IOException {
    if (Role.JT.equals(process.getRole())) {
      return createJTClient(process);
    } else if (Role.TT.equals(process.getRole())) {
      return createTTClient(process);
    } else throw new IOException("Role: "+ process.getRole() + "  is not " +
      "applicable to MRCluster");
  }

  public static class MRProcessManager extends HadoopDaemonRemoteCluster{
    public MRProcessManager() {
      super(mrDaemonInfos);
    }
  }

  public static class MultiMRProcessManager
      extends MultiUserHadoopDaemonRemoteCluster {
    public MultiMRProcessManager() {
      super(mrDaemonInfos);
    }
  }

  /**
   * Get a TTClient Instance from a running task <br/>
   * @param Task Information of the running task
   * @return TTClient instance
   * @throws IOException
   */
  public TTClient getTTClientInstance(TaskInfo taskInfo)
      throws IOException {
    JTProtocol remoteJTClient = getJTClient().getProxy();
    String [] taskTrackers = taskInfo.getTaskTrackers();
    int counter = 0;
    TTClient ttClient = null;
    while (counter < 60) {
      if (taskTrackers.length != 0) {
        break;
      }
      UtilsForTests.waitFor(100);
      taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
      taskTrackers = taskInfo.getTaskTrackers();
      counter ++;
    }
    if ( taskTrackers.length != 0 ) {
      String hostName = taskTrackers[0].split("_")[1];
      hostName = hostName.split(":")[0];
      ttClient = getTTClient(hostName);
    }
    return ttClient;
  }

}
