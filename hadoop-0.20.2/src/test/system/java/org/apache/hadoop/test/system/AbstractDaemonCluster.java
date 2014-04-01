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

package org.apache.hadoop.test.system;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Abstract class which represent the cluster having multiple daemons.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractDaemonCluster {

  private static final Log LOG = LogFactory.getLog(AbstractDaemonCluster.class);
  private String [] excludeExpList ;
  private Configuration conf;
  protected ClusterProcessManager clusterManager;
  private Map<Enum<?>, List<AbstractDaemonClient>> daemons = 
    new LinkedHashMap<Enum<?>, List<AbstractDaemonClient>>();
  private String newConfDir = null;  
  File localFolderObj = null;
  private static final  String CONF_HADOOP_LOCAL_DIR =
      "test.system.hdrc.hadoop.local.confdir"; 
  private static final  String CONF_HADOOP_MULTI_USER_LIST =
      "test.system.hdrc.multi-user.list.path";
  private final static Object waitLock = new Object();
  
  /**
   * Constructor to create a cluster client.<br/>
   * 
   * @param conf
   *          Configuration to be used while constructing the cluster.
   * @param rcluster
   *          process manger instance to be used for managing the daemons.
   * 
   * @throws IOException
   */
  public AbstractDaemonCluster(Configuration conf,
      ClusterProcessManager rcluster) throws IOException {
    this.conf = conf;
    this.clusterManager = rcluster;
    createAllClients();
  }

  /**
   * The method returns the cluster manager. The system test cases require an
   * instance of HadoopDaemonRemoteCluster to invoke certain operation on the
   * daemon.
   * 
   * @return instance of clusterManager
   */
  public ClusterProcessManager getClusterManager() {
    return clusterManager;
  }

  protected void createAllClients() throws IOException {
    for (RemoteProcess p : clusterManager.getAllProcesses()) {
      List<AbstractDaemonClient> dms = daemons.get(p.getRole());
      if (dms == null) {
        dms = new ArrayList<AbstractDaemonClient>();
        daemons.put(p.getRole(), dms);
      }
      dms.add(createClient(p));
    }
  }
  
  /**
   * Method to create the daemon client.<br/>
   * 
   * @param process
   *          to manage the daemon.
   * @return instance of the daemon client
   * 
   * @throws IOException
   */
  protected abstract AbstractDaemonClient<DaemonProtocol> 
    createClient(RemoteProcess process) throws IOException;

  /**
   * Get the global cluster configuration which was used to create the 
   * cluster. <br/>
   * 
   * @return global configuration of the cluster.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   *

  /**
   * Return the client handle of all the Daemons.<br/>
   * 
   * @return map of role to daemon clients' list.
   */
  public Map<Enum<?>, List<AbstractDaemonClient>> getDaemons() {
    return daemons;
  }

  /**
   * Checks if the cluster is ready for testing. <br/>
   * Algorithm for checking is as follows : <br/>
   * <ul>
   * <li> Wait for Daemon to come up </li>
   * <li> Check if daemon is ready </li>
   * <li> If one of the daemon is not ready, return false </li>
   * </ul> 
   * 
   * @return true if whole cluster is ready.
   * 
   * @throws IOException
   */
  public boolean isReady() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        waitForDaemon(daemon);
        if (!daemon.isReady()) {
          return false;
        }
      }
    }
    return true;
  }

  protected void waitForDaemon(AbstractDaemonClient d) {
    final int TEN_SEC = 10000;
    while(true) {
      try {
        LOG.info("Waiting for daemon at " + d.getHostName() + " to come up.");
        LOG.info("Daemon might not be " +
            "ready or the call to setReady() method hasn't been " +
            "injected to " + d.getClass() + " ");
        d.connect();
        break;
      } catch (IOException e) {
        try {
          Thread.sleep(TEN_SEC);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Starts the cluster daemons.
   * @throws IOException
   */
  public void start() throws IOException {
    clusterManager.start();
  }

  /**
   * Stops the cluster daemons.
   * @throws IOException
   */
  public void stop() throws IOException {
    clusterManager.stop();
  }

  /**
   * Connect to daemon RPC ports.
   * @throws IOException
   */
  public void connect() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        daemon.connect();
      }
    }
  }

  /**
   * Disconnect to daemon RPC ports.
   * @throws IOException
   */
  public void disconnect() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        daemon.disconnect();
      }
    }
  }

  /**
   * Ping all the daemons of the cluster.
   * @throws IOException
   */
  public void ping() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        LOG.info("Daemon is : " + daemon.getHostName() + " pinging....");
        daemon.ping();
      }
    }
  }

  /**
   * Connect to the cluster and ensure that it is clean to run tests.
   * @throws Exception
   */
  public void setUp() throws Exception {
    while (!isReady()) {
      Thread.sleep(1000);
    }
    connect();
    ping();
    clearAllControlActions();
    ensureClean();
    populateExceptionCounts();
  }
  
  /**
   * This is mainly used for the test cases to set the list of exceptions
   * that will be excluded.
   * @param excludeExpList list of exceptions to exclude
   */
  public void setExcludeExpList(String [] excludeExpList) {
    this.excludeExpList = excludeExpList;
  }
  
  public void clearAllControlActions() throws IOException {
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        LOG.info("Daemon is : " + daemon.getHostName() + " pinging....");
        daemon.getProxy().clearActions();
      }
    }
  }

  /**
   * Ensure that the cluster is clean to run tests.
   * @throws IOException
   */
  public void ensureClean() throws IOException {
  }

  /**
   * Ensure that cluster is clean. Disconnect from the RPC ports of the daemons.
   * @throws IOException
   */
  public void tearDown() throws IOException {
    ensureClean();
    clearAllControlActions();
    assertNoExceptionMessages();
    disconnect();
  }

  /**
   * Populate the exception counts in all the daemons so that it can be checked when 
   * the testcase has finished running.<br/>
   * @throws IOException
   */
  protected void populateExceptionCounts() throws IOException {
    for(List<AbstractDaemonClient> lst : daemons.values()) {
      for(AbstractDaemonClient d : lst) {
        d.populateExceptionCount(excludeExpList);
      }
    }
  }

  /**
   * Assert no exception has been thrown during the sequence of the actions.
   * <br/>
   * @throws IOException
   */
  protected void assertNoExceptionMessages() throws IOException {
    for(List<AbstractDaemonClient> lst : daemons.values()) {
      for(AbstractDaemonClient d : lst) {
        d.assertNoExceptionsOccurred(excludeExpList);
      }
    }
  }

  /**
   * Get the multi users list.
   * @return ArrayList - users list as a array list.
   * @throws IOException - if an I/O error occurs.
   */
  public ArrayList<String> getHadoopMultiUsersList() throws
     IOException {
    String hadoopUserListPath = conf.get(CONF_HADOOP_MULTI_USER_LIST);
    if (hadoopUserListPath == null || hadoopUserListPath.isEmpty()) {
      LOG.error("Proxy user list path has not been passed for "
          + CONF_HADOOP_MULTI_USER_LIST);
      throw new IllegalArgumentException(
          "Proxy user list hasn't been provided.");
    }
    File fileObj = new File(hadoopUserListPath);
    DataInputStream disObj = new DataInputStream(new FileInputStream(fileObj));
    ArrayList<String> usersList = new ArrayList<String>();
    String strLine = null;
    while((strLine = disObj.readLine()) != null){
      usersList.add(strLine.substring(0,strLine.indexOf(',')));
    }
    return usersList;
  }
  
  /**
   * Get the proxy userlist path.
   * @return String - userlist path
   */
  public String getProxyUsersFilePath() {
    return null;
  }

  /**
   * It's a local folder where the config file stores temporarily
   * while serializing the object.
   * @return String temporary local folder path for configuration.
   */
  private String getHadoopLocalConfDir() {
    String hadoopLocalConfDir = conf.get(CONF_HADOOP_LOCAL_DIR);
    if (hadoopLocalConfDir == null || hadoopLocalConfDir.isEmpty()) {
      LOG.error("No configuration "
          + "for the CONF_HADOOP_LOCAL_DIR passed");
      throw new IllegalArgumentException(
          "No Configuration passed for hadoop conf local directory");
    }
    return hadoopLocalConfDir;
  }

  /**
   * It uses to restart the cluster with new configuration at runtime.<br/>
   * @param props attributes for new configuration.
   * @param configFile configuration file.
   * @throws IOException if an I/O error occurs.
   */
  public void restartClusterWithNewConfig(Hashtable<String,?> props, 
      String configFile) throws IOException {
    String mapredConf = null;
    String localDirPath = null; 
    Configuration initConf = new Configuration(getConf());
    Enumeration<String> e = props.keys();
    while (e.hasMoreElements()) {
      String propKey = e.nextElement();
      Object propValue = props.get(propKey);
      initConf.set(propKey,propValue.toString());
    }
    localDirPath = getHadoopLocalConfDir(); 
    writeConfToFile(configFile,localDirPath,initConf);
    newConfDir = clusterManager.pushConfig(localDirPath);
    stop();
    waitForClusterToStop();
    clusterManager.start(newConfDir);
    waitForClusterToStart();
    localFolderObj.delete();
  }
  
  /**
   * It uses to delete the new configuration folder.
   * @param path - configuration directory path.
   * @throws IOException if an I/O error occurs.
   */
  public void cleanupNewConf(String path) throws IOException {
    File file = new File(path);
    file.delete();
  }
  
  /**
   * The method is used to only restart one daemon with new config
   * and this gives a great flexibility in choosing which daemon the
   * test wants to restarting instead of changing the config in all the 
   * daemons. 
   * @param client points to the daemon that will restarted.
   * @param configFile the name of the config file
   * @param conf the Configuration object
   * @param role of remote process such as JT,TT,NN,DN
   * @throws IOException thrown in case of any error 
   */
  public void restartDaemonWithNewConfig(AbstractDaemonClient client, 
      String configFile, Configuration conf, Enum<?> role) throws IOException {    
    String localDirPath = getHadoopLocalConfDir();
    writeConfToFile(configFile,localDirPath,conf);
    RemoteProcess daemon=clusterManager.getDaemonProcess(client.getHostName(), 
        role);    
    newConfDir = daemon.pushConfig(localDirPath);
    daemon.kill();
    waitForDaemonToStop(client);
    daemon.start(newConfDir);
    waitForDaemonToStart(client);    
    localFolderObj.delete();
  }
  
  private void writeConfToFile(String configFile, String localDirPath,
      Configuration conf) throws IOException{      
    localFolderObj = new File(localDirPath);
    if (!localFolderObj.exists()) {
      localFolderObj.mkdir();
    }
    String confXMLFile = localDirPath + File.separator + configFile;
    File xmlFileObj = new File(confXMLFile);
    conf.writeXml(new FileOutputStream(xmlFileObj));
  }
  
  /**
   * It uses to restart the cluster with default configuration.<br/>
   * @throws IOException if an I/O error occurs.
   */
  public void restart() throws 
      IOException {
    stop();
    waitForClusterToStop();
    start();
    waitForClusterToStart();
    cleanupNewConf(newConfDir);
  }
  
  /**
   * Restart only one daemon as opposed to all the daemons
   * @param client points to the daemon that will restarted. 
   * @param role  of remote process such as JT,TT,NN,DN 
   * @throws IOException is thrown when restart fails
   */
  public void restart(AbstractDaemonClient client,Enum<?> role) throws IOException {
    RemoteProcess daemon=clusterManager.getDaemonProcess(client.getHostName(), 
        role);
    daemon.kill();
    waitForDaemonToStop(client);
    daemon.start();
    waitForDaemonToStart(client);
  }
  
  /**
   * It uses to wait until the cluster is stopped.<br/>
   * @throws IOException if an I/O error occurs.
   */
  public void waitForClusterToStop() throws 
      IOException {
    List<Thread> chkDaemonStop = new ArrayList<Thread>();
    for (List<AbstractDaemonClient> set : daemons.values()) {	  
      for (AbstractDaemonClient daemon : set) {
        DaemonStopThread dmStop = new DaemonStopThread(daemon);
        chkDaemonStop.add(dmStop);
        dmStop.start();
      }
    }
    
    for (Thread daemonThread : chkDaemonStop){
      try {
        daemonThread.join();
      } catch(InterruptedException intExp) {
         LOG.warn("Interrupted while thread is joining." + intExp.getMessage());
      }
    }
  }
 
  /**
   * It uses to wait until the cluster is started.<br/>
   * @throws IOException if an I/O error occurs.
   */
  public void  waitForClusterToStart() throws 
      IOException {
    List<Thread> chkDaemonStart = new ArrayList<Thread>();
    for (List<AbstractDaemonClient> set : daemons.values()) {
      for (AbstractDaemonClient daemon : set) {
        DaemonStartThread dmStart = new DaemonStartThread(daemon);
        chkDaemonStart.add(dmStart);;
        dmStart.start();
      }
    }

    for (Thread daemonThread : chkDaemonStart){
      try {
        daemonThread.join();
      } catch(InterruptedException intExp) {
        LOG.warn("Interrupted while thread is joining" + intExp.getMessage());
      }
    }
  }
  /**
   * This will provide synchronization for the daemon to stop
   * @param client identifies the daemon
   * @throws IOException thrown if daemon does not stop 
   */
  public void waitForDaemonToStop(AbstractDaemonClient client) 
      throws IOException {
    while (true) {
      try {
        client.ping();
        LOG.debug(client.getHostName() +" is waiting state to stop.");
        waitFor(5000);
      } catch (Exception exp) {
        LOG.info("Daemon is : " + client.getHostName() + " stopped...");
        break;
      } 
    }   
  }
  
  /**
   * This will provide synchronization for the daemon to start
   * @param client identifies the daemon
   * @throws IOException thrown if the daemon does not start
   */
  public void waitForDaemonToStart(AbstractDaemonClient client) 
      throws IOException {
    
    while (true) {
      try {
        client.ping();
        LOG.info("Daemon is : " + client.getHostName() + " pinging...");
        break;
      } catch (Exception exp) {
        LOG.debug(client.getHostName() + " is waiting to come up.");
        waitFor(5000);
      }
    }    
  }

  /**
   * It waits for specified amount of time.
   * @param duration time in milliseconds.
   * @throws InterruptedException if any thread interrupted the current
   * thread while it is waiting for a notification.
   */
  public void waitFor(long duration) {
    try {
      synchronized (waitLock) {
        waitLock.wait(duration);
      }
    } catch (InterruptedException intExp) {
       LOG.warn("Interrrupeted while thread is waiting" + intExp.getMessage());
    }
  }
  
  class DaemonStartThread extends Thread {
    private AbstractDaemonClient daemon;

    public DaemonStartThread(AbstractDaemonClient daemon) {
      this.daemon = daemon;
    }

    public void run(){
      LOG.info("Waiting for Daemon " + daemon.getHostName() 
          + " to come up.....");
      while (true) { 
        try {
          daemon.ping();
          LOG.info("Daemon is : " + daemon.getHostName() + " pinging...");
          break;
        } catch (Exception exp) {
          LOG.debug(daemon.getHostName() + " is waiting to come up.");
          waitFor(60000);
        }
      }
    }
  }
  
  class DaemonStopThread extends Thread {
    private AbstractDaemonClient daemon;

    public DaemonStopThread(AbstractDaemonClient daemon) {
      this.daemon = daemon;
    }

    public void run() {
      LOG.info("Waiting for Daemon " + daemon.getHostName() 
          + " to stop.....");
      while (true) {
        try {
          daemon.ping();
          LOG.debug(daemon.getHostName() +" is waiting state to stop.");
          waitFor(60000);
        } catch (Exception exp) {
          LOG.info("Daemon is : " + daemon.getHostName() + " stopped...");
          break;
        } 
      }
    }
  }
}

