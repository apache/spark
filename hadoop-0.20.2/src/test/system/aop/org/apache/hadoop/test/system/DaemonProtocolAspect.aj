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

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * Default DaemonProtocolAspect which is used to provide default implementation
 * for all the common daemon methods. If a daemon requires more specialized
 * version of method, it is responsibility of the DaemonClient to introduce the
 * same in woven classes.
 * 
 */
public aspect DaemonProtocolAspect {

  private boolean DaemonProtocol.ready;
  
  @SuppressWarnings("unchecked")
  private HashMap<Object, List<ControlAction>> DaemonProtocol.actions = 
    new HashMap<Object, List<ControlAction>>();
  private static final Log LOG = LogFactory.getLog(
      DaemonProtocolAspect.class.getName());

  private static FsPermission defaultPermission = new FsPermission(
     FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);

  /**
   * Set if the daemon process is ready or not, concrete daemon protocol should
   * implement pointcuts to determine when the daemon is ready and use the
   * setter to set the ready state.
   * 
   * @param ready
   *          true if the Daemon is ready.
   */
  public void DaemonProtocol.setReady(boolean ready) {
    this.ready = ready;
  }

  /**
   * Checks if the daemon process is alive or not.
   * 
   * @throws IOException
   *           if daemon is not alive.
   */
  public void DaemonProtocol.ping() throws IOException {
  }

  /**
   * Checks if the daemon process is ready to accepting RPC connections after it
   * finishes initialization. <br/>
   * 
   * @return true if ready to accept connection.
   * 
   * @throws IOException
   */
  public boolean DaemonProtocol.isReady() throws IOException {
    return ready;
  }

  /**
   * Returns the process related information regarding the daemon process. <br/>
   * 
   * @return process information.
   * @throws IOException
   */
  public ProcessInfo DaemonProtocol.getProcessInfo() throws IOException {
    int activeThreadCount = Thread.activeCount();
    long currentTime = System.currentTimeMillis();
    long maxmem = Runtime.getRuntime().maxMemory();
    long freemem = Runtime.getRuntime().freeMemory();
    long totalmem = Runtime.getRuntime().totalMemory();
    Map<String, String> envMap = System.getenv();
    Properties sysProps = System.getProperties();
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry entry : sysProps.entrySet()) {
      props.put((String) entry.getKey(), (String) entry.getValue());
    }
    ProcessInfo info = new ProcessInfoImpl(activeThreadCount, currentTime,
        freemem, maxmem, totalmem, envMap, props);
    return info;
  }

  public void DaemonProtocol.enable(List<Enum<?>> faults) throws IOException {
  }

  public void DaemonProtocol.disableAll() throws IOException {
  }

  public abstract Configuration DaemonProtocol.getDaemonConf()
    throws IOException;

  public FileStatus DaemonProtocol.getFileStatus(String path, boolean local) 
    throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFS(p, local);
    p.makeQualified(fs);
    FileStatus fileStatus = fs.getFileStatus(p);
    return cloneFileStatus(fileStatus);
  }

  /**
   * Create a file with given permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name.
   * @param permission - file permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void DaemonProtocol.createFile(String path, String fileName, 
     FsPermission permission, boolean local) throws IOException {
    Path p = new Path(path); 
    FileSystem fs = getFS(p, local);
    Path filePath = new Path(path, fileName);
    fs.create(filePath);
    if (permission == null) {
      fs.setPermission(filePath, defaultPermission);
    } else {
      fs.setPermission(filePath, permission);
    }
    fs.close();
  }

  /**
   * Create a folder with given permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param permission - folder permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void DaemonProtocol.createFolder(String path, String folderName, 
     FsPermission permission, boolean local) throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFS(p, local);
    Path folderPath = new Path(path, folderName);
    fs.mkdirs(folderPath);
    if (permission ==  null) {
      fs.setPermission(folderPath, defaultPermission);
    } else {
      fs.setPermission(folderPath, permission);
    }
    fs.close();
  }

  public FileStatus[] DaemonProtocol.listStatus(String path, boolean local) 
    throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFS(p, local);
    FileStatus[] status = fs.listStatus(p);
    if (status != null) {
      FileStatus[] result = new FileStatus[status.length];
      int i = 0;
      for (FileStatus fileStatus : status) {
        result[i++] = cloneFileStatus(fileStatus);
      }
      return result;
    }
    return status;
  }

  /**
   * FileStatus object may not be serializable. Clone it into raw FileStatus 
   * object.
   */
  private FileStatus DaemonProtocol.cloneFileStatus(FileStatus fileStatus) {
    return new FileStatus(fileStatus.getLen(),
        fileStatus.isDir(),
        fileStatus.getReplication(),
        fileStatus.getBlockSize(),
        fileStatus.getModificationTime(),
        fileStatus.getAccessTime(),
        fileStatus.getPermission(),
        fileStatus.getOwner(),
        fileStatus.getGroup(),
        fileStatus.getPath());
  }

  private FileSystem DaemonProtocol.getFS(final Path path, final boolean local)
      throws IOException {
    FileSystem ret = null;
    try {
      ret = UserGroupInformation.getLoginUser().doAs (
          new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws IOException {
              FileSystem fs = null;
              if (local) {
                fs = FileSystem.getLocal(getDaemonConf());
              } else {
                fs = path.getFileSystem(getDaemonConf());
              }
              return fs;
            }
          });
    } catch (InterruptedException ie) {
    }
    return ret;
  }
  
  @SuppressWarnings("unchecked")
  public ControlAction[] DaemonProtocol.getActions(Writable key) 
    throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(key);
      if(actionList == null) {
        return new ControlAction[0];
      } else {
        return (ControlAction[]) actionList.toArray(new ControlAction[actionList
                                                                      .size()]);
      }
    }
  }


  @SuppressWarnings("unchecked")
  public void DaemonProtocol.sendAction(ControlAction action) 
      throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        actionList = new ArrayList<ControlAction>();
        actions.put(action.getTarget(), actionList);
      }
      actionList.add(action);
    } 
  }
 
  @SuppressWarnings("unchecked")
  public boolean DaemonProtocol.isActionPending(ControlAction action) 
    throws IOException{
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        return false;
      } else {
        return actionList.contains(action);
      }
    }
  }
  
  
  @SuppressWarnings("unchecked")
  public void DaemonProtocol.removeAction(ControlAction action) 
    throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        return;
      } else {
        actionList.remove(action);
      }
    }
  }
  
  public void DaemonProtocol.clearActions() throws IOException {
    synchronized (actions) {
      actions.clear();
    }
  }

  public String DaemonProtocol.getFilePattern() {
    //We use the environment variable HADOOP_LOGFILE to get the
    //pattern to use in the search.
    String logDir = System.getenv("HADOOP_LOG_DIR");
    String daemonLogPattern = System.getenv("HADOOP_LOGFILE");
    if(daemonLogPattern == null && daemonLogPattern.isEmpty()) {
      return "*";
    }
    return  logDir+File.separator+daemonLogPattern+"*";
  }

  public int DaemonProtocol.getNumberOfMatchesInLogFile(String pattern,
      String[] list) throws IOException {
    StringBuffer filePattern = new StringBuffer(getFilePattern());    
    String[] cmd = null;
    if (list != null) {
      StringBuffer filterExpPattern = new StringBuffer();
      int index=0;
      for (String excludeExp : list) {
        if (index++ < list.length -1) {
           filterExpPattern.append("grep -v " + excludeExp + " | ");
        } else {
           filterExpPattern.append("grep -v " + excludeExp + " | wc -l");
        }
      }
      cmd = new String[] {
                "bash",
                "-c",
                "grep "
                + pattern + " " + filePattern + " | "
                + filterExpPattern};
    } else {
      cmd = new String[] {
                "bash",
                "-c",
                "grep -c "
                + pattern + " " + filePattern
                + " | awk -F: '{s+=$2} END {print s}'" };    
    }
    ShellCommandExecutor shexec = new ShellCommandExecutor(cmd);
    shexec.execute();
    String output = shexec.getOutput();
    return Integer.parseInt(output.replaceAll("\n", "").trim());
  }
  
  /**
   * This method is used for suspending the process.
   * @param pid process id
   * @throws IOException if an I/O error occurs.
   * @return true if process is suspended otherwise false.
   */
  public boolean DaemonProtocol.suspendProcess(String pid) throws IOException {
    String suspendCmd = getDaemonConf().get("test.system.hdrc.suspend.cmd",
        "kill -SIGSTOP");
    String [] command = {"bash", "-c", suspendCmd + " " + pid};
    ShellCommandExecutor shexec = new ShellCommandExecutor(command);
    try {
      shexec.execute();
    } catch (Shell.ExitCodeException e) {
      LOG.warn("suspended process throws an exitcode "
          + "exception for not being suspended the given process id.");
      return false;
    }
    LOG.info("The suspend process command is :"
        + shexec.toString()
        + " and the output for the command is "
        + shexec.getOutput());
    return true;
  }

  /**
   * This method is used for resuming the process
   * @param pid process id of suspended process.
   * @throws IOException if an I/O error occurs.
   * @return true if suspeneded process is resumed otherwise false.
   */
  public boolean DaemonProtocol.resumeProcess(String pid) throws IOException {
    String resumeCmd = getDaemonConf().get("test.system.hdrc.resume.cmd",
        "kill -SIGCONT");
    String [] command = {"bash", "-c", resumeCmd + " " + pid};
    ShellCommandExecutor shexec = new ShellCommandExecutor(command);
    try {
      shexec.execute();
    } catch(Shell.ExitCodeException e) {
        LOG.warn("Resume process throws an exitcode "
          + "exception for not being resumed the given process id.");
      return false;
    }
    LOG.info("The resume process command is :"
        + shexec.toString()
        + " and the output for the command is "
        + shexec.getOutput());
    return true;
  }

  private String DaemonProtocol.user = null;
  
  public String DaemonProtocol.getDaemonUser() {
    return user;
  }
  
  public void DaemonProtocol.setUser(String user) {
    this.user = user;
  }
}
