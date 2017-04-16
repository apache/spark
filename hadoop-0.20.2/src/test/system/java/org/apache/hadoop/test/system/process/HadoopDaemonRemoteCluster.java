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

package org.apache.hadoop.test.system.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * The concrete class which implements the start up and shut down based routines
 * based on the hadoop-daemon.sh. <br/>
 * 
 * Class requires two keys to be present in the Configuration objects passed to
 * it. Look at <code>CONF_HADOOPHOME</code> and
 * <code>CONF_HADOOPCONFDIR</code> for the names of the
 * configuration keys.
 * 
 * Following will be the format which the final command execution would look : 
 * <br/>
 * <code>
 *  ssh host 'hadoop-home/bin/hadoop-daemon.sh --script scriptName 
 *  --config HADOOP_CONF_DIR (start|stop) command'
 * </code>
 */
public abstract class HadoopDaemonRemoteCluster 
    implements ClusterProcessManager {

  private static final Log LOG = LogFactory
      .getLog(HadoopDaemonRemoteCluster.class.getName());

  public static final String CONF_HADOOPNEWCONFDIR =
    "test.system.hdrc.hadoopnewconfdir";
  /**
   * Key used to configure the HADOOP_HOME to be used by the
   * HadoopDaemonRemoteCluster.
   */
  public final static String CONF_HADOOPHOME =
    "test.system.hdrc.hadoophome";

  public final static String CONF_SCRIPTDIR =
    "test.system.hdrc.deployed.scripts.dir";
  /**
   * Key used to configure the HADOOP_CONF_DIR to be used by the
   * HadoopDaemonRemoteCluster.
   */
  public final static String CONF_HADOOPCONFDIR = 
    "test.system.hdrc.hadoopconfdir";

  public final static String CONF_DEPLOYED_HADOOPCONFDIR =
    "test.system.hdrc.deployed.hadoopconfdir";

  private String hadoopHome;
  protected String hadoopConfDir;
  protected String scriptsDir;
  protected String hadoopNewConfDir;
  private final Set<Enum<?>> roles;
  private final List<HadoopDaemonInfo> daemonInfos;
  private List<RemoteProcess> processes;
  protected Configuration conf;
  
  public static class HadoopDaemonInfo {
    public final String cmd;
    public final Enum<?> role;
    public final List<String> hostNames;
    public HadoopDaemonInfo(String cmd, Enum<?> role, List<String> hostNames) {
      super();
      this.cmd = cmd;
      this.role = role;
      this.hostNames = hostNames;
    }

    public HadoopDaemonInfo(String cmd, Enum<?> role, String hostFile) 
        throws IOException {
      super();
      this.cmd = cmd;
      this.role = role;
      File file = new File(getDeployedHadoopConfDir(), hostFile);
      BufferedReader reader = null;
      hostNames = new ArrayList<String>();
      try {
        reader = new BufferedReader(new FileReader(file));
        String host = null;
        while ((host = reader.readLine()) != null) {
          if (host.trim().isEmpty() || host.startsWith("#")) {
            // Skip empty and possible comment lines
            // throw new IllegalArgumentException(
            // "Hostname could not be found in file " + hostFile);
            continue;
          }
          hostNames.add(host.trim());
        }
        if (hostNames.size() < 1) {
          throw new IllegalArgumentException("At least one hostname "
              +
            "is required to be present in file - " + hostFile);
        }
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.warn("Could not close reader");
        }
      }
      LOG.info("Created HadoopDaemonInfo for " + cmd + " " + role + " from " 
          + hostFile);
    }
  }

  @Override
  public String pushConfig(String localDir) throws IOException {
    for (RemoteProcess process : processes){
      process.pushConfig(localDir);
    }
    return hadoopNewConfDir;
  }
  
  @Override
  public RemoteProcess getDaemonProcess(String hostname,Enum<?> role) {
    RemoteProcess daemon=null;
    for (RemoteProcess p : processes) {      
      if(p.getHostName().equals(hostname) && p.getRole() == role){
        daemon =p;
        break;
      }     
    }
    return daemon;
  }

  public HadoopDaemonRemoteCluster(List<HadoopDaemonInfo> daemonInfos) {
    this.daemonInfos = daemonInfos;
    this.roles = new HashSet<Enum<?>>();
    for (HadoopDaemonInfo info : daemonInfos) {
      this.roles.add(info.role);
    }
  }

  @Override
  public void init(Configuration conf) throws IOException {
    this.conf = conf;
    populateDirectories(conf);
    this.processes = new ArrayList<RemoteProcess>();
    populateDaemons();
  }

  @Override
  public List<RemoteProcess> getAllProcesses() {
    return processes;
  }

  @Override
  public Set<Enum<?>> getRoles() {
    return roles;
  }

  /**
   * Method to populate the hadoop home and hadoop configuration directories.
   * 
   * @param conf
   *          Configuration object containing values for
   *          CONF_HADOOPHOME and
   *          CONF_HADOOPCONFDIR
   * 
   * @throws IllegalArgumentException
   *           if the configuration or system property set does not contain
   *           values for the required keys.
   */
  protected void populateDirectories(Configuration conf) {
    hadoopHome = conf.get(CONF_HADOOPHOME);
    hadoopConfDir = conf.get(CONF_HADOOPCONFDIR);
    scriptsDir = conf.get(CONF_SCRIPTDIR);
    hadoopNewConfDir = conf.get(CONF_HADOOPNEWCONFDIR);
    if (hadoopHome == null || hadoopConfDir == null || hadoopHome.isEmpty()
        || hadoopConfDir.isEmpty()) {
      LOG.error("No configuration "
          + "for the HADOOP_HOME and HADOOP_CONF_DIR passed");
      throw new IllegalArgumentException(
          "No Configuration passed for hadoop home " +
          "and hadoop conf directories");
    }
  }

  public static String getDeployedHadoopConfDir() {
    String dir = System.getProperty(CONF_DEPLOYED_HADOOPCONFDIR);
    if (dir == null || dir.isEmpty()) {
      LOG.error("No configuration "
          + "for the CONF_DEPLOYED_HADOOPCONFDIR passed");
      throw new IllegalArgumentException(
          "No Configuration passed for hadoop deployed conf directory");
    }
    return dir;
  }

  @Override
  public void start() throws IOException {
    for (RemoteProcess process : processes) {
      process.start();
    }
  }

  @Override
  public void start(String newConfLocation)throws IOException {
    for (RemoteProcess process : processes) {
      process.start(newConfLocation);
    }
  }

  @Override
  public void stop() throws IOException {
    for (RemoteProcess process : processes) {
      process.kill();
    }
  }

  @Override
  public void stop(String newConfLocation) throws IOException {
    for (RemoteProcess process : processes) {
      process.kill(newConfLocation);
    }
  }

  protected void populateDaemon(HadoopDaemonInfo info) throws IOException {
    for (String host : info.hostNames) {
      InetAddress addr = InetAddress.getByName(host);
      RemoteProcess process = getProcessManager(info, 
          addr.getCanonicalHostName());
      processes.add(process);
    }
  }

  protected void populateDaemons() throws IOException {
   for (HadoopDaemonInfo info : daemonInfos) {
     populateDaemon(info);
   }
  }

  @Override
  public boolean isMultiUserSupported() throws IOException {
    return false;
  }

  protected RemoteProcess getProcessManager(
      HadoopDaemonInfo info, String hostName) {
    RemoteProcess process = new ScriptDaemon(info.cmd, hostName, info.role);
    return process;
  }

  /**
   * The core daemon class which actually implements the remote process
   * management of actual daemon processes in the cluster.
   * 
   */
  class ScriptDaemon implements RemoteProcess {

    private static final String STOP_COMMAND = "stop";
    private static final String START_COMMAND = "start";
    private static final String SCRIPT_NAME = "hadoop-daemon.sh";
    private static final String PUSH_CONFIG ="pushConfig.sh";
    protected final String daemonName;
    protected final String hostName;
    private final Enum<?> role;

    public ScriptDaemon(String daemonName, String hostName, Enum<?> role) {
      this.daemonName = daemonName;
      this.hostName = hostName;
      this.role = role;
    }

    @Override
    public String getHostName() {
      return hostName;
    }

    private String[] getPushConfigCommand(String localDir, String remoteDir,
        File scriptDir) throws IOException{
      ArrayList<String> cmdArgs = new ArrayList<String>();
      cmdArgs.add(scriptDir.getAbsolutePath() + File.separator + PUSH_CONFIG);
      cmdArgs.add(localDir);
      cmdArgs.add(hostName);
      cmdArgs.add(remoteDir);
      cmdArgs.add(hadoopConfDir);
      return (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
    }

    private ShellCommandExecutor buildPushConfig(String local, String remote )
        throws IOException {
      File scriptDir = new File(scriptsDir);
      String[] commandArgs = getPushConfigCommand(local, remote, scriptDir);
      HashMap<String, String> env = new HashMap<String, String>();
      ShellCommandExecutor executor = new ShellCommandExecutor(commandArgs,
          scriptDir, env);
      LOG.info(executor.toString());
      return executor;
    }

    private ShellCommandExecutor createNewConfDir() throws IOException {
      ArrayList<String> cmdArgs = new ArrayList<String>();
      cmdArgs.add("ssh");
      cmdArgs.add(hostName);
      cmdArgs.add("if [ -d "+ hadoopNewConfDir+
          " ];\n then echo Will remove existing directory;  rm -rf "+
          hadoopNewConfDir+";\nmkdir "+ hadoopNewConfDir+"; else \n"+
          "echo " + hadoopNewConfDir + " doesnt exist hence creating" +
          ";  mkdir " + hadoopNewConfDir + ";\n  fi");
      String[] cmd = (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
      ShellCommandExecutor executor = new ShellCommandExecutor(cmd);
      LOG.info(executor.toString());
      return executor;
    }

    @Override
    public String pushConfig(String localDir) throws IOException {
      createNewConfDir().execute();
      buildPushConfig(localDir, hadoopNewConfDir).execute();
      return hadoopNewConfDir;
    }

    private ShellCommandExecutor buildCommandExecutor(String command,
        String confDir) {
      String[] commandArgs = getCommand(command, confDir);
      File cwd = new File(".");
      HashMap<String, String> env = new HashMap<String, String>();
      env.put("HADOOP_CONF_DIR", confDir);
      ShellCommandExecutor executor
        = new ShellCommandExecutor(commandArgs, cwd, env);
      LOG.info(executor.toString());
      return executor;
    }

    private File getBinDir() {
      File binDir = new File(hadoopHome, "bin");
      return binDir;
    }

    protected String[] getCommand(String command, String confDir) {
      ArrayList<String> cmdArgs = new ArrayList<String>();
      File binDir = getBinDir();
      cmdArgs.add("ssh");
      cmdArgs.add(hostName);
      cmdArgs.add(binDir.getAbsolutePath() + File.separator + SCRIPT_NAME);
      cmdArgs.add("--config");
      cmdArgs.add(confDir);
      // XXX Twenty internal version does not support --script option.
      cmdArgs.add(command);
      cmdArgs.add(daemonName);
      return (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
    }

    @Override
    public void kill() throws IOException {
      kill(hadoopConfDir);
    }

    @Override
    public void start() throws IOException {
      start(hadoopConfDir);
    }

    public void start(String newConfLocation) throws IOException {
      ShellCommandExecutor cme = buildCommandExecutor(START_COMMAND,
          newConfLocation);
      cme.execute();
      String output = cme.getOutput();
      if (!output.isEmpty()) { //getOutput() never returns null value
        if (output.toLowerCase().contains("error")) {
          LOG.warn("Error is detected.");
          throw new IOException("Start error\n" + output);
        }
      }
    }

    public void kill(String newConfLocation) throws IOException {
      ShellCommandExecutor cme
        = buildCommandExecutor(STOP_COMMAND, newConfLocation);
      cme.execute();
      String output = cme.getOutput();
      if (!output.isEmpty()) { //getOutput() never returns null value
        if (output.toLowerCase().contains("error")) {
          LOG.info("Error is detected.");
          throw new IOException("Kill error\n" + output);
        }
      }
    }

    @Override
    public Enum<?> getRole() {
      return role;
    }
  }
}
