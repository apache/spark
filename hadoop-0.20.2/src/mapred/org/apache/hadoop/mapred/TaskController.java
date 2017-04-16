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
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.util.ProcessTree.Signal;

/**
 * Controls initialization, finalization and clean up of tasks, and
 * also the launching and killing of task JVMs.
 * 
 * This class defines the API for initializing, finalizing and cleaning
 * up of tasks, as also the launching and killing task JVMs.
 * Subclasses of this class will implement the logic required for
 * performing the actual actions.
 * 
 * <br/>
 * 
 * NOTE: This class is internal only class and not intended for users!!
 */
public abstract class TaskController implements Configurable {
  
  private Configuration conf;
  
  public static final Log LOG = LogFactory.getLog(TaskController.class);
  
  //Name of the executable script that will contain the child
  // JVM command line. See writeCommand for details.
  protected static final String COMMAND_FILE = "taskjvm.sh";
  
  protected LocalDirAllocator allocator;
  protected LocalStorage localStorage;

  final public static FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
  FsPermission.createImmutable((short) 0700); // rwx--------
  
  public Configuration getConf() {
    return conf;
  }

  public String[] getLocalDirs() {
    return localStorage.getDirs();
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Does initialization and setup.
   * @param allocator the local dir allocator to use
   * @param localStorage TaskTracker's LocalStorage object
   */
  public abstract void setup(LocalDirAllocator allocator,
      LocalStorage localStorage) throws IOException;

  /**
   * Create all of the directories necessary for the job to start and download
   * all of the job and private distributed cache files.
   * Creates both the user directories and the job log directory.
   * @param user the user name
   * @param jobid the job
   * @param credentials a filename containing the job secrets
   * @param jobConf the path to the localized configuration file
   * @param taskTracker the connection the task tracker
   * @param ttAddr the tasktracker's RPC address
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void initializeJob(String user, String jobid, 
                                     Path credentials, Path jobConf,
                                     TaskUmbilicalProtocol taskTracker,
                                     InetSocketAddress ttAddr) 
  throws IOException, InterruptedException;

  /**
   * Create all of the directories for the task and launches the child jvm.
   * @param user the user name
   * @param jobId the jobId in question
   * @param attemptId the attempt id (cleanup attempts have .cleanup suffix)
   * @param setup list of shell commands to execute before the jvm
   * @param jvmArguments list of jvm arguments
   * @param currentWorkDirectory the full path of the cwd for the task
   * @param stdout the file to redirect stdout to
   * @param stderr the file to redirect stderr to
   * @return the exit code for the task
   * @throws IOException
   */
  public abstract
  int launchTask(String user, 
                 String jobId,
                 String attemptId,
                 List<String> setup,
                 List<String> jvmArguments,
                 File currentWorkDirectory,
                 String stdout,
                 String stderr) throws IOException;
  

  /**
   * Send a signal to a task pid as the user.
   * @param user the user name
   * @param taskPid the pid of the task
   * @param signal the id of the signal to send
   */
  public abstract void signalTask(String user, int taskPid, 
                                  Signal signal) throws IOException;

  /**
   * Delete the user's files under all of the task tracker root directories.
   * @param user the user name
   * @param subDir the path relative to the user's subdirectory under
   *        the task tracker root directories.
   * @throws IOException
   */
  public abstract void deleteAsUser(String user, 
                                    String subDir) throws IOException;

  /**
   * Creates task log dir
   * @param taskID ID of the task
   * @param isCleanup If the task is cleanup task or not
   * @throws IOException
   */
  public abstract void createLogDir(TaskAttemptID taskID,
                                    boolean isCleanup) throws IOException;

  /**
   * Delete the user's files under the userlogs directory.
   * @param user the user to work as
   * @param subDir the path under the userlogs directory.
   * @throws IOException
   */
  public abstract void deleteLogAsUser(String user, 
                                       String subDir) throws IOException;

  /**
   * Run the passed command as the user
   * @param user 
   * @param allAttempts the list of attempts that the JVM ran
   * @throws IOException
   */
  public abstract void truncateLogsAsUser(String user, List<Task> allAttempts) 
  throws IOException;
  
  static class DeletionContext extends CleanupQueue.PathDeletionContext {
    private TaskController controller;
    private boolean isLog;
    private String user;
    private String subDir;
    DeletionContext(TaskController controller, boolean isLog, String user, 
                    String subDir) {
      super(null, null);
      this.controller = controller;
      this.isLog = isLog;
      this.user = user;
      this.subDir = subDir;
    }

    @Override
    protected void deletePath() throws IOException {
      if (isLog) {
        controller.deleteLogAsUser(user, subDir);
      } else {
        controller.deleteAsUser(user, subDir);
      }
    }

    @Override
    public String toString() {
      return (isLog ? "log(" : "dir(") +
        user + "," + subDir + ")";
    }
  }

   /**
    * Returns the local unix user that a given job will run as.
    */
   public String getRunAsUser(JobConf conf) {
     return System.getProperty("user.name");
   }

  //Write the JVM command line to a file under the specified directory
  // Note that the JVM will be launched using a setuid executable, and
  // could potentially contain strings defined by a user. Hence, to
  // prevent special character attacks, we write the command line to
  // a file and execute it.
  protected static String writeCommand(String cmdLine, FileSystem fs,
      Path commandFile) throws IOException {
    String path = commandFile.makeQualified(fs).toUri().getPath();
    FileWriter w = null;
    LOG.info("Writing commands to " + path);
    try {
      File parent = new File(path).getParentFile();
      if (!parent.isDirectory() && !parent.mkdirs()) {
        throw new IOException(
          "Couldn't ensure directory for task script: " + parent);
      }
      w = new FileWriter(path);
      w.write(cmdLine);
    } catch (IOException ioe) {
      LOG.error("Caught IOException while writing JVM command line to file. ",
          ioe);
      throw ioe;
    } finally {
      if (w != null) {
        w.close();
      }
    }
    fs.setPermission(commandFile, TASK_LAUNCH_SCRIPT_PERMISSION);
    return path;
  }
  
  protected void logOutput(String output) {
    String shExecOutput = output;
    if (shExecOutput != null) {
      for (String str : shExecOutput.split("\n")) {
        LOG.info(str);
      }
    }
  }
}
