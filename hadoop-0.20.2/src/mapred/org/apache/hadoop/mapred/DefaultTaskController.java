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
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.util.ProcessTree.Signal;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The default implementation for controlling tasks.
 * 
 * This class provides an implementation for launching and killing 
 * tasks that need to be run as the tasktracker itself. Hence,
 * many of the initializing or cleanup methods are not required here.
 * 
 * <br/>
 * 
 *  NOTE: This class is internal only class and not intended for users!!
 */
public class DefaultTaskController extends TaskController {

  private static final Log LOG = 
      LogFactory.getLog(DefaultTaskController.class);
  private FileSystem fs;
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      fs = FileSystem.getLocal(conf).getRaw();
    } catch (IOException ie) {
      throw new RuntimeException("Failed getting LocalFileSystem", ie);
    }
  }

  @Override
  public void createLogDir(TaskAttemptID taskID, 
                           boolean isCleanup) throws IOException {
    TaskLog.createTaskAttemptLogDir(taskID, isCleanup, localStorage.getDirs());
  }
  
  /**
   * Create all of the directories for the task and launches the child jvm.
   * @param user the user name
   * @param attemptId the attempt id
   * @throws IOException
   */
  @Override
  public int launchTask(String user, 
                                  String jobId,
                                  String attemptId,
                                  List<String> setup,
                                  List<String> jvmArguments,
                                  File currentWorkDirectory,
                                  String stdout,
                                  String stderr) throws IOException {
    ShellCommandExecutor shExec = null;
    try {    	            
      FileSystem localFs = FileSystem.getLocal(getConf());
      
      //create the attempt dirs
      new Localizer(localFs, 
          getConf().getTrimmedStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY)).
          initializeAttemptDirs(user, jobId, attemptId);
      
      // create the working-directory of the task 
      if (!currentWorkDirectory.mkdir()) {
        throw new IOException("Mkdirs failed to create " 
                    + currentWorkDirectory.toString());
      }
      
      //mkdir the loglocation
      String logLocation = TaskLog.getAttemptDir(jobId, attemptId).toString();
      if (!localFs.mkdirs(new Path(logLocation))) {
        throw new IOException("Mkdirs failed to create " 
                   + logLocation);
      }
      //read the configuration for the job
      FileSystem rawFs = FileSystem.getLocal(getConf()).getRaw();
      long logSize = 0; //TODO: Ref BUG:2854624
      // get the JVM command line.
      String cmdLine = 
        TaskLog.buildCommandLine(setup, jvmArguments,
            new File(stdout), new File(stderr), logSize, true);

      // write the command to a file in the
      // task specific cache directory
      // TODO copy to user dir
      Path p = new Path(allocator.getLocalPathForWrite(
          TaskTracker.getPrivateDirTaskScriptLocation(user, jobId, attemptId),
          getConf()), COMMAND_FILE);

      String commandFile = writeCommand(cmdLine, rawFs, p);
      try {
        rawFs.setPermission(p, TaskController.TASK_LAUNCH_SCRIPT_PERMISSION);
      } catch (ExitCodeException ece) {
        // we don't want to confuse this exception with an ExitCodeException
        // from the shExec below.
        throw new IOException("Could not set permissions on " + p, ece);
      }
      shExec = new ShellCommandExecutor(new String[]{
          "bash", "-c", commandFile},
          currentWorkDirectory);
      shExec.execute();
    } catch (ExitCodeException ece) {
      if (shExec != null) {
        logShExecStatus(shExec);
      }
      if (ece.getMessage() != null && !ece.getMessage().isEmpty()) {
        LOG.warn("Task wrapper stderr: " + ece.getMessage());
      }
      return (shExec != null) ? shExec.getExitCode() : -1;
    } catch (Exception e) {
      LOG.warn("Unexpected error launching task JVM", e);
      if (shExec != null) {
        logShExecStatus(shExec);
      }
      return -1;
    }
    return 0;
  }

  private void logShExecStatus(ShellCommandExecutor shExec) {
    LOG.warn("Exit code from task is : " + shExec.getExitCode());
    String stdout = shExec.getOutput().trim();
    if (!stdout.isEmpty()) {
      LOG.info("Output from DefaultTaskController's launchTask follows:");
      logOutput(shExec.getOutput());
    }
  }
    
  /**
   * This routine initializes the local file system for running a job.
   * Details:
   * <ul>
   * <li>Copies the credentials file from the TaskTracker's private space to
   * the job's private space </li>
   * <li>Creates the job work directory and set 
   * {@link TaskTracker#JOB_LOCAL_DIR} in the configuration</li>
   * <li>Downloads the job.jar, unjars it, and updates the configuration to 
   * reflect the localized path of the job.jar</li>
   * <li>Creates a base JobConf in the job's private space</li>
   * <li>Sets up the distributed cache</li>
   * <li>Sets up the user logs directory for the job</li>
   * </ul>
   * This method must be invoked in the access control context of the job owner 
   * user. This is because the distributed cache is also setup here and the 
   * access to the hdfs files requires authentication tokens in case where 
   * security is enabled.
   * @param user the user in question (the job owner)
   * @param jobid the ID of the job in question
   * @param credentials the path to the credentials file that the TaskTracker
   * downloaded
   * @param jobConf the path to the job configuration file that the TaskTracker
   * downloaded
   * @param taskTracker the connection to the task tracker
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initializeJob(String user, String jobid, 
                            Path credentials, Path jobConf, 
                            TaskUmbilicalProtocol taskTracker,
                            InetSocketAddress ttAddr
                            ) throws IOException, InterruptedException {
    final LocalDirAllocator lDirAlloc = allocator;
    FileSystem localFs = FileSystem.getLocal(getConf());
    JobLocalizer localizer = new JobLocalizer((JobConf)getConf(), user, jobid);
    localizer.createLocalDirs();
    localizer.createUserDirs();
    localizer.createJobDirs();

    JobConf jConf = new JobConf(jobConf);
    localizer.createWorkDir(jConf);
    //copy the credential file
    Path localJobTokenFile = lDirAlloc.getLocalPathForWrite(
        TaskTracker.getLocalJobTokenFile(user, jobid), getConf());
    FileUtil.copy(
        localFs, credentials, localFs, localJobTokenFile, false, getConf());


    //setup the user logs dir
    localizer.initializeJobLogDir();

    // Download the job.jar for this job from the system FS
    // setup the distributed cache
    // write job acls
    // write localized config
    localizer.localizeJobFiles(JobID.forName(jobid), jConf, localJobTokenFile, 
                               taskTracker);
  }

  @Override
  public void signalTask(String user, int taskPid, Signal signal) {
    if (ProcessTree.isSetsidAvailable) {
      ProcessTree.killProcessGroup(Integer.toString(taskPid), signal);
    } else {
      ProcessTree.killProcess(Integer.toString(taskPid), signal);      
    }
  }

  /**
   * Delete the user's files under all of the task tracker root directories.
   * @param user the user name
   * @param subDir the path relative to the user's subdirectory under
   *        the task tracker root directories.
   * @throws IOException
   */
  @Override
  public void deleteAsUser(String user, 
                           String subDir) throws IOException {
    String dir = TaskTracker.getUserDir(user) + Path.SEPARATOR + subDir;
    for(Path fullDir: allocator.getAllLocalPathsToRead(dir, getConf())) {
      fs.delete(fullDir, true);
    }
  }
  
  /**
   * Delete the user's files under the userlogs directory.
   * @param user the user to work as
   * @param subDir the path under the userlogs directory.
   * @throws IOException
   */
  @Override
  public void deleteLogAsUser(String user, 
                              String subDir) throws IOException {
    Path dir = new Path(TaskLog.getUserLogDir().getAbsolutePath(), subDir);
    // Delete the subDir in <hadoop.log.dir>/userlogs
    File subDirPath = new File(dir.toString());
    FileUtil.fullyDelete(subDirPath);

    // Delete the subDir in all good <mapred.local.dirs>/userlogs
    for (String localdir : localStorage.getDirs()) {
      String dirPath = localdir + File.separatorChar +
        TaskLog.USERLOGS_DIR_NAME + File.separatorChar + subDir;

      try {
        FileUtil.fullyDelete(new File(dirPath));
      } catch(Exception e){
        // Skip bad dir for later deletion
        LOG.warn("Could not delete dir: " + dirPath +
                 " , Reason : " + e.getMessage());
      }
    }
  }
  
  @Override
  public void truncateLogsAsUser(String user, List<Task> allAttempts)
    throws IOException {
    Task firstTask = allAttempts.get(0);
    TaskLogsTruncater trunc = new TaskLogsTruncater(getConf());

    trunc.truncateLogs(new JVMInfo(
            TaskLog.getAttemptDir(firstTask.getTaskID(), 
                                  firstTask.isTaskCleanupTask()),
                       allAttempts));
  }

  @Override
  public void setup(LocalDirAllocator allocator, LocalStorage localStorage) {
    this.allocator = allocator;
    this.localStorage = localStorage;
  }
  
}
