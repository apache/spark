package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.MRCluster.Role;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster;
import org.junit.AfterClass;
import org.junit.Assert;


/** 
 * This is helper class that is used by the health script test cases
 *
 */
public class HealthScriptHelper  {

  static final Log LOG = LogFactory.getLog(HealthScriptHelper.class);
  
  /**
   * Will verify that given task tracker is not blacklisted
   * @param client tasktracker info
   * @param conf modified configuration object
   * @param cluster mrcluster instance
   * @throws IOException thrown if verification fails
   */
  public void verifyTTNotBlackListed(TTClient client, Configuration conf,
      MRCluster cluster) throws IOException {        
    int interval = conf.getInt("mapred.healthChecker.interval",0);
    Assert.assertTrue("Interval cannot be zero.",interval != 0);
    UtilsForTests.waitFor(interval+2000);
    String defaultHealthScript = conf.get("mapred.healthChecker.script.path");    
    Assert.assertTrue("Task tracker is not healthy",
        nodeHealthStatus(client, true) == true);
    TaskTrackerStatus status = client.getStatus();
    JTClient jclient = cluster.getJTClient();
    Assert.assertTrue("Failed to move task tracker to healthy list",
        jclient.getProxy().isBlackListed(status.getTrackerName()) == false);        
    Assert.assertTrue("Health script was not set",defaultHealthScript != null);
    
  }
  
  /**
   * Verifies that the given task tracker is blacklisted
   * @param conf modified Configuration object
   * @param client tasktracker info
   * @param errorMessage that needs to be asserted
   * @param cluster mr cluster instance
   * @throws IOException is thrown when verification fails
   */
  public void verifyTTBlackList(Configuration conf, TTClient client,
      String errorMessage, MRCluster cluster) throws IOException{   
    int interval = conf.getInt("mapred.healthChecker.interval",0);
    Assert.assertTrue("Interval cannot be zero.",interval != 0);
    UtilsForTests.waitFor(interval+2000);
    //TaskTrackerStatus status = client.getStatus();
    Assert.assertTrue("Task tracker was never blacklisted ",
        nodeHealthStatus(client, false) == true);
    TaskTrackerStatus status = client.getStatus();
    Assert.assertTrue("The custom error message did not appear",
        status.getHealthStatus().getHealthReport().trim().
        equals(errorMessage));
    JTClient jClient = cluster.getJTClient();    
    Assert.assertTrue("Failed to move task tracker to blacklisted list",
        jClient.getProxy().isBlackListed(status.getTrackerName()) == true);    
  }
  
  /**
   * The method return true from the task tracker if it is unhealthy/healthy
   * depending the blacklisted status
   * @param client the tracker tracker instance
   * @param health status information. 
   * @return status of task tracker
   * @throws IOException failed to get the status of task tracker
   */
  public boolean nodeHealthStatus(TTClient client,boolean hStatus) throws IOException {
    int counter = 0;
    TaskTrackerStatus status = client.getStatus();
    while (counter < 60) {
      LOG.info("isNodeHealthy "+status.getHealthStatus().isNodeHealthy());
      if (status.getHealthStatus().isNodeHealthy() == hStatus) {
        break;
      } else {
        UtilsForTests.waitFor(3000);
        status = client.getStatus();
        Assert.assertNotNull("Task tracker status is null",status);
      }
      counter++;
    }
    if(counter != 60) {
      return true;
    }
    return false;
  }
  
  /**
   * This will copy the error inducing health script from local node running
   * the system tests to the node where the task tracker runs
   * @param scriptName name of the scirpt to be copied
   * @param hostname identifies the task tracker
   * @param remoteLocation location in remote task tracker node
   * @param cluster mrcluster instance
   * @throws IOException thrown if copy file fails. 
   */
  public void copyFileToRemoteHost(String scriptName, String hostname,
      String remoteLocation,MRCluster cluster) throws IOException {        
    ArrayList<String> cmdArgs = new ArrayList<String>();
    String scriptDir = cluster.getConf().get(
        HadoopDaemonRemoteCluster.CONF_SCRIPTDIR);
    StringBuffer localFile = new StringBuffer();    
    localFile.append(scriptDir).append(File.separator).append(scriptName);
    cmdArgs.add("scp");
    cmdArgs.add(localFile.toString());
    StringBuffer remoteFile = new StringBuffer();
    remoteFile.append(hostname).append(":");
    remoteFile.append(remoteLocation).append(File.separator).append(scriptName);
    cmdArgs.add(remoteFile.toString());
    executeCommand(cmdArgs);
  }
  
  private void executeCommand(ArrayList<String> cmdArgs) throws IOException{
    String[] cmd = (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
    ShellCommandExecutor executor = new ShellCommandExecutor(cmd);
    LOG.info(executor.toString());
    executor.execute();
    String output = executor.getOutput();    
    if (!output.isEmpty()) { //getOutput() never returns null value
      if (output.toLowerCase().contains("error")) {
        LOG.warn("Error is detected.");
        throw new IOException("Start error\n" + output);
      }
    }
  }
  
  /**
   * cleans up the error inducing health script in the remote node
   * @param path the script that needs to be deleted
   * @param hostname where the script resides. 
   */
  public void deleteFileOnRemoteHost(String path, String hostname) {
    try {
      ArrayList<String> cmdArgs = new ArrayList<String>();
      cmdArgs.add("ssh");
      cmdArgs.add(hostname);
      cmdArgs.add("if [ -f "+ path+
      "  ];\n then echo Will remove existing file "+path+";  rm -f "+
      path+";\n  fi");
      executeCommand(cmdArgs);
    }
    catch (IOException io) {
      LOG.error("Failed to remove the script "+path+" on remote host "+hostname);
    }
  }

}
