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

package org.apache.hadoop.hdfs.test.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.test.system.AbstractDaemonCluster;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster;
import org.apache.hadoop.test.system.process.MultiUserHadoopDaemonRemoteCluster;
import org.apache.hadoop.test.system.process.RemoteProcess;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster.HadoopDaemonInfo;

public class HDFSCluster extends AbstractDaemonCluster {

  static {
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  private static final Log LOG = LogFactory.getLog(HDFSCluster.class);
  public static final String CLUSTER_PROCESS_MGR_IMPL =
    "test.system.hdfs.clusterprocess.impl.class";

  private HDFSCluster(Configuration conf, ClusterProcessManager rCluster)
    throws IOException {
    super(conf, rCluster);
  }

  /**
   * Key is used to to point to the file containing hostnames of tasktrackers
   */
  public static final String CONF_HADOOP_DN_HOSTFILE_NAME =
    "test.system.hdrc.dn.hostfile";

  private static List<HadoopDaemonInfo> hdfsDaemonInfos;

  private static String nnHostName;
  private static String DN_hostFileName;

  protected enum Role {NN, DN}

  @Override
  protected AbstractDaemonClient
    createClient(RemoteProcess process) throws IOException {
    Enum<?> pRole = process.getRole();
    if (Role.NN.equals(pRole)) {
      return createNNClient(process);
    } else if (Role.DN.equals(pRole)) {
      return createDNClient(process);
    } else throw new IOException("Role " + pRole +
      " is not supported by HDFSCluster");
  }

  protected DNClient createDNClient(RemoteProcess dnDaemon) throws IOException {
    return new DNClient(getConf(), dnDaemon);
  }

  protected NNClient createNNClient(RemoteProcess nnDaemon) throws IOException {
    return new NNClient(getConf(), nnDaemon);
  }

  public NNClient getNNClient () {
    Iterator<AbstractDaemonClient> iter = getDaemons().get(Role.NN).iterator();
    return (NNClient) iter.next();
  }

  public List<DNClient> getDNClients () {
    return (List) getDaemons().get(Role.DN);
  }

  public DNClient getDNClient (String hostname) {
    for (DNClient dnC : getDNClients()) {
      if (dnC.getHostName().equals(hostname))
        return dnC;
    }
    return null;
  }

  public static class HDFSProcessManager extends HadoopDaemonRemoteCluster {
    public HDFSProcessManager() {
      super(hdfsDaemonInfos);
    }
  }

  public static class MultiUserHDFSProcessManager
      extends MultiUserHadoopDaemonRemoteCluster {
    public MultiUserHDFSProcessManager() {
      super(hdfsDaemonInfos);
    }
  }


  public static HDFSCluster createCluster(Configuration conf) throws Exception {
    conf.addResource("system-test.xml");
    String sockAddrStr = FileSystem.getDefaultUri(conf).getAuthority();
    if (sockAddrStr == null) {
      throw new IllegalArgumentException("Namenode IPC address is not set");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          "Namenode report IPC is not correctly configured");
    }
    nnHostName = splits[0];
    DN_hostFileName = conf.get(CONF_HADOOP_DN_HOSTFILE_NAME, "slaves");

    hdfsDaemonInfos = new ArrayList<HadoopDaemonInfo>();
    hdfsDaemonInfos.add(new HadoopDaemonInfo("namenode", 
        Role.NN, Arrays.asList(new String[]{nnHostName})));
    hdfsDaemonInfos.add(new HadoopDaemonInfo("datanode", 
        Role.DN, DN_hostFileName));
    
    String implKlass = conf.get(CLUSTER_PROCESS_MGR_IMPL);
    if (implKlass == null || implKlass.isEmpty()) {
      implKlass = HDFSCluster.HDFSProcessManager.class.getName();
    }
    Class<ClusterProcessManager> klass =
      (Class<ClusterProcessManager>) Class.forName(implKlass);
    ClusterProcessManager clusterProcessMgr = klass.newInstance();
    LOG.info("Created ClusterProcessManager as " + implKlass);
    clusterProcessMgr.init(conf);
    return new HDFSCluster(conf, clusterProcessMgr);
  }
}
