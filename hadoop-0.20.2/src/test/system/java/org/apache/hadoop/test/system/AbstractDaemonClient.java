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
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.system.process.RemoteProcess;
/**
 * Abstract class which encapsulates the DaemonClient which is used in the 
 * system tests.<br/>
 * 
 * @param PROXY the proxy implementation of a specific Daemon 
 */
public abstract class AbstractDaemonClient<PROXY extends DaemonProtocol> {
  private Configuration conf;
  private RemoteProcess process;
  private boolean connected;

  private static final Log LOG = LogFactory.getLog(AbstractDaemonClient.class);
  
  /**
   * Create a Daemon client.<br/>
   * 
   * @param conf client to be used by proxy to connect to Daemon.
   * @param process the Daemon process to manage the particular daemon.
   * 
   * @throws IOException on RPC error
   */
  public AbstractDaemonClient(Configuration conf, RemoteProcess process) 
      throws IOException {
    this.conf = conf;
    this.process = process;
  }

  /**
   * Gets if the client is connected to the Daemon <br/>
   * 
   * @return true if connected.
   */
  public boolean isConnected() {
    return connected;
  }

  protected void setConnected(boolean connected) {
    this.connected = connected;
  }

  /**
   * Create an RPC proxy to the daemon <br/>
   * 
   * @throws IOException on RPC error
   */
  public abstract void connect() throws IOException;

  /**
   * Disconnect the underlying RPC proxy to the daemon.<br/>
   * @throws IOException
   */
  public abstract void disconnect() throws IOException;

  /**
   * Get the proxy to connect to a particular service Daemon.<br/>
   * 
   * @return proxy to connect to a particular service Daemon.
   */
  protected abstract PROXY getProxy();

  /**
   * Gets the daemon level configuration.<br/>
   * 
   * @return configuration using which daemon is running
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Gets the host on which Daemon is currently running. <br/>
   * 
   * @return hostname
   */
  public String getHostName() {
    return process.getHostName();
  }

  /**
   * Gets if the Daemon is ready to accept RPC connections. <br/>
   * 
   * @return true if daemon is ready.
   * @throws IOException on RPC error
   */
  public boolean isReady() throws IOException {
    return getProxy().isReady();
  }

  /**
   * Kills the Daemon process <br/>
   * @throws IOException on RPC error
   */
  public void kill() throws IOException {
    process.kill();
  }

  /**
   * Checks if the Daemon process is alive or not <br/>
   * @throws IOException on RPC error
   */
  public void ping() throws IOException {
    getProxy().ping();
  }

  /**
   * Start up the Daemon process. <br/>
   * @throws IOException on RPC error
   */
  public void start() throws IOException {
    process.start();
  }

  /**
   * Get system level view of the Daemon process.
   * 
   * @return returns system level view of the Daemon process.
   * 
   * @throws IOException on RPC error. 
   */
  public ProcessInfo getProcessInfo() throws IOException {
    return getProxy().getProcessInfo();
  }

  /**
   * Return a file status object that represents the path.
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus getFileStatus(String path, boolean local) throws IOException {
    return getProxy().getFileStatus(path, local);
  }

  /**
   * Create a file with full permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFile(String path, String fileName, 
      boolean local) throws IOException {
    getProxy().createFile(path, fileName, null, local);
  }

  /**
   * Create a file with given permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name.
   * @param permission - file permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFile(String path, String fileName, 
     FsPermission permission,  boolean local) throws IOException {
    getProxy().createFile(path, fileName, permission, local);
  }

  /**
   * Create a folder with default permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs. 
   */
  public void createFolder(String path, String folderName, 
     boolean local) throws IOException {
    getProxy().createFolder(path, folderName, null, local);
  }

  /**
   * Create a folder with given permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param permission - folder permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFolder(String path, String folderName, 
     FsPermission permission,  boolean local) throws IOException {
    getProxy().createFolder(path, folderName, permission, local);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return the statuses of the files/directories in the given patch
   * @throws IOException on RPC error. 
   */
  public FileStatus[] listStatus(String path, boolean local) 
    throws IOException {
    return getProxy().listStatus(path, local);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory recursive/nonrecursively depending on parameters
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @param recursive 
   *          whether to recursively get the status
   * @return the statuses of the files/directories in the given patch
   * @throws IOException is thrown on RPC error. 
   */
  public FileStatus[] listStatus(String f, boolean local, boolean recursive) 
    throws IOException {
    List<FileStatus> status = new ArrayList<FileStatus>();
    addStatus(status, f, local, recursive);
    return status.toArray(new FileStatus[0]);
  }

  private void addStatus(List<FileStatus> status, String f, 
      boolean local, boolean recursive) 
    throws IOException {
    FileStatus[] fs = listStatus(f, local);
    if (fs != null) {
      for (FileStatus fileStatus : fs) {
        if (!f.equals(fileStatus.getPath().toString())) {
          status.add(fileStatus);
          if (recursive) {
            addStatus(status, fileStatus.getPath().toString(), local, recursive);
          }
        }
      }
    }
  }

  /**
   * Gets number of times FATAL log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is FATAL. <br/>
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of fatal message.
   * @throws IOException
   */
  public int getNumberOfFatalStatementsInLog(String [] excludeExpList)
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "FATAL";
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of times ERROR log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is ERROR. <br/>
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of error message.
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfErrorStatementsInLog(String[] excludeExpList) 
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "ERROR";    
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of times Warning log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is WARN. <br/>
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of warning message.
   * @throws IOException thrown on RPC error. 
   */
  public int getNumberOfWarnStatementsInLog(String[] excludeExpList) 
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "WARN";
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of time given Exception were present in log file. <br/>
   * 
   * @param e exception class.
   * @param excludeExpList list of exceptions to exclude. 
   * @return number of exceptions in log
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfExceptionsInLog(Exception e,
      String[] excludeExpList) throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = e.getClass().getSimpleName();    
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Number of times ConcurrentModificationException present in log file. 
   * <br/>
   * @param excludeExpList list of exceptions to exclude.
   * @return number of times exception in log file.
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfConcurrentModificationExceptionsInLog(
      String[] excludeExpList) throws IOException {
    return getNumberOfExceptionsInLog(new ConcurrentModificationException(),
        excludeExpList);
  }

  private int errorCount;
  private int fatalCount;
  private int concurrentExceptionCount;

  /**
   * Populate the initial exception counts to be used to assert once a testcase
   * is done there was no exception in the daemon when testcase was run.
   * @param excludeExpList list of exceptions to exclude
   * @throws IOException is thrown on RPC error. 
   */
  protected void populateExceptionCount(String [] excludeExpList) 
      throws IOException {
    errorCount = getNumberOfErrorStatementsInLog(excludeExpList);
    LOG.info("Number of error messages in logs : " + errorCount);
    fatalCount = getNumberOfFatalStatementsInLog(excludeExpList);
    LOG.info("Number of fatal statement in logs : " + fatalCount);
    concurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog(excludeExpList);
    LOG.info("Number of concurrent modification in logs : "
        + concurrentExceptionCount);
  }

  /**
   * Assert if the new exceptions were logged into the log file.
   * <br/>
   * <b><i>
   * Pre-req for the method is that populateExceptionCount() has 
   * to be called before calling this method.</b></i>
   * @param excludeExpList list of exceptions to exclude
   * @throws IOException is thrown on RPC error. 
   */
  protected void assertNoExceptionsOccurred(String [] excludeExpList) 
      throws IOException {
    int newerrorCount = getNumberOfErrorStatementsInLog(excludeExpList);
    LOG.info("Number of error messages while asserting :" + newerrorCount);
    int newfatalCount = getNumberOfFatalStatementsInLog(excludeExpList);
    LOG.info("Number of fatal messages while asserting : " + newfatalCount);
    int newconcurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog(excludeExpList);
    LOG.info("Number of concurrentmodification exception while asserting :"
        + newconcurrentExceptionCount);
    Assert.assertEquals(
        "New Error Messages logged in the log file", errorCount, newerrorCount);
    Assert.assertEquals(
        "New Fatal messages logged in the log file", fatalCount, newfatalCount);
    Assert.assertEquals(
        "New ConcurrentModificationException in log file",
        concurrentExceptionCount, newconcurrentExceptionCount);
  }
}
