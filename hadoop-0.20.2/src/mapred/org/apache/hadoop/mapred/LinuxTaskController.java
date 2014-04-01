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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.util.PlatformName;
import org.apache.hadoop.util.ProcessTree.Signal;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A {@link TaskController} that runs the task JVMs as the user 
 * who submits the job.
 * 
 * This class executes a setuid executable to implement methods
 * of the {@link TaskController}, including launching the task 
 * JVM and killing it when needed, and also initializing and
 * finalizing the task environment. 
 * <p> The setuid executable is launched using the command line:</p>
 * <p>task-controller user-name good-local-dirs command command-args,
 * where</p>
 * <p>user-name is the name of the owner who submits the job</p>
 * <p>good-local-dirs is comma separated list of good mapred local dirs</p>
 * <p>command is one of the cardinal value of the 
 * {@link LinuxTaskController.TaskControllerCommands} enumeration</p>
 * <p>command-args depends on the command being launched.</p>
 * 
 * In addition to running and killing tasks, the class also 
 * sets up appropriate access for the directories and files 
 * that will be used by the tasks. 
 */
class LinuxTaskController extends TaskController {

  private static final Log LOG = 
            LogFactory.getLog(LinuxTaskController.class);

  // Path to the setuid executable.
  protected String taskControllerExe;
  private static final String TASK_CONTROLLER_EXEC_KEY =
    "mapreduce.tasktracker.task-controller.exe";
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    File hadoopSbin = new File(System.getenv("HADOOP_HOME"), "sbin");
    File nativeSbin = new File(hadoopSbin, PlatformName.getPlatformName());
    String defaultTaskController = 
        new File(nativeSbin, "task-controller").getAbsolutePath();
    taskControllerExe = conf.get(TASK_CONTROLLER_EXEC_KEY, 
                                 defaultTaskController);       
  }
  
  public LinuxTaskController() {
    super();
  }
  
  /**
   * List of commands that the setuid script will execute.
   */
  enum Commands {
    INITIALIZE_JOB(0),
    LAUNCH_TASK_JVM(1),
    SIGNAL_TASK(2),
    DELETE_AS_USER(3),
    DELETE_LOG_AS_USER(4),
    RUN_COMMAND_AS_USER(5);

    private int value;
    Commands(int value) {
      this.value = value;
    }
    int getValue() {
      return value;
    }
  }

  /**
   * Result codes returned from the C task-controller.
   * These must match the values in task-controller.h.
   */
  enum ResultCode {
    OK(0),
    INVALID_USER_NAME(2),
    INVALID_TASK_PID(9),
    INVALID_CONFIG_FILE(24);

    private final int value;
    ResultCode(int value) {
      this.value = value;
    }
    int getValue() {
      return value;
    }
  }

  @Override
  public void setup(LocalDirAllocator allocator, LocalStorage localStorage)
      throws IOException {

    // Check the permissions of the task-controller binary by running
    // it plainly.  If permissions are correct, it returns an error
    // code 1, else it returns 24 or something else if some other bugs
    // are also present.
    String[] taskControllerCmd =
        new String[] { taskControllerExe };
    ShellCommandExecutor shExec = new ShellCommandExecutor(taskControllerCmd);
    try {
      shExec.execute();
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      if (exitCode != 1) {
        LOG.warn("Exit code from checking binary permissions is : " + exitCode);
        logOutput(shExec.getOutput());
        throw new IOException("Task controller setup failed because of invalid"
          + "permissions/ownership with exit code " + exitCode, e);
      }
    }
    this.allocator = allocator;
    this.localStorage = localStorage;
  }

  @Override
  public void initializeJob(String user, String jobid, Path credentials,
                            Path jobConf, TaskUmbilicalProtocol taskTracker,
                            InetSocketAddress ttAddr
                            ) throws IOException {
    List<String> command = new ArrayList<String>(
      Arrays.asList(taskControllerExe, 
                    user,
                    localStorage.getDirsString(),
                    Integer.toString(Commands.INITIALIZE_JOB.getValue()),
                    jobid,
                    credentials.toUri().getPath().toString(),
                    jobConf.toUri().getPath().toString()));
    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
    command.add(jvm.toString());
    command.add("-classpath");
    command.add(System.getProperty("java.class.path"));
    command.add("-Dhadoop.log.dir=" + TaskLog.getBaseLogDir());
    command.add("-Dhadoop.root.logger=INFO,console");
    command.add(JobLocalizer.class.getName());  // main of JobLocalizer
    command.add(user);
    command.add(jobid);
    // add the task tracker's reporting address
    command.add(ttAddr.getHostName());
    command.add(Integer.toString(ttAddr.getPort()));
    String[] commandArray = command.toArray(new String[0]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("initializeJob: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      logOutput(shExec.getOutput());
      throw new IOException("Job initialization failed (" + exitCode + 
          ") with output: " + shExec.getOutput(), e);
    }
  }

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
      FileSystem rawFs = FileSystem.getLocal(getConf()).getRaw();
      long logSize = 0; //TODO, Ref BUG:2854624
      // get the JVM command line.
      String cmdLine = 
        TaskLog.buildCommandLine(setup, jvmArguments,
            new File(stdout), new File(stderr), logSize, true);

      // write the command to a file in the
      // task specific cache directory
      Path p = new Path(allocator.getLocalPathForWrite(
          TaskTracker.getPrivateDirTaskScriptLocation(user, jobId, attemptId),
          getConf()), COMMAND_FILE);
      String commandFile = writeCommand(cmdLine, rawFs, p); 

      String[] command = 
        new String[]{taskControllerExe, 
          user,
          localStorage.getDirsString(),
          Integer.toString(Commands.LAUNCH_TASK_JVM.getValue()),
          jobId,
          attemptId,
          currentWorkDirectory.toString(),
          commandFile};
      shExec = new ShellCommandExecutor(command);

      if (LOG.isDebugEnabled()) {
        LOG.debug("launchTask: " + Arrays.toString(command));
      }
      shExec.execute();
    } catch (Exception e) {
      if (shExec == null) {
        return -1;
      }
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from task is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the task was
      // terminated/killed forcefully. In all other cases, log the
      // task-controller output
      if (exitCode != 143 && exitCode != 137) {
        LOG.warn("Exception thrown while launching task JVM : "
            + StringUtils.stringifyException(e));
        LOG.info("Output from LinuxTaskController's launchTaskJVM follows:");
        logOutput(shExec.getOutput());
      }
      return exitCode;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Output from LinuxTaskController's launchTask follows:");
      logOutput(shExec.getOutput());
    }
    return 0;
  }

  @Override
  public void deleteAsUser(String user, String subDir) throws IOException {
    String[] command = 
      new String[]{taskControllerExe, 
                   user,
                   localStorage.getDirsString(),
                   Integer.toString(Commands.DELETE_AS_USER.getValue()),
                   subDir};
    ShellCommandExecutor shExec = new ShellCommandExecutor(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteAsUser: " + Arrays.toString(command));
    }
    shExec.execute();
  }

  @Override
  public void createLogDir(TaskAttemptID taskID,
                           boolean isCleanup) throws IOException {
    // Log dirs are created during attempt dir creation when running the task
  }

  @Override
  public void deleteLogAsUser(String user, String subDir) throws IOException {
    String[] command = 
      new String[]{taskControllerExe, 
                   user,
                   localStorage.getDirsString(),
                   Integer.toString(Commands.DELETE_LOG_AS_USER.getValue()),
                   subDir};
    ShellCommandExecutor shExec = new ShellCommandExecutor(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteLogAsUser: " + Arrays.toString(command));
    }
    shExec.execute();
  }

  @Override
  public void signalTask(String user, int taskPid, 
                         Signal signal) throws IOException {
    String[] command = 
      new String[]{taskControllerExe, 
                   user,
                   localStorage.getDirsString(),
                   Integer.toString(Commands.SIGNAL_TASK.getValue()),
                   Integer.toString(taskPid),
                   Integer.toString(signal.getValue())};
    ShellCommandExecutor shExec = new ShellCommandExecutor(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug("signalTask: " + Arrays.toString(command));
    }
    try {
      shExec.execute();
    } catch (ExitCodeException e) {
      int ret_code = shExec.getExitCode();
      if (ret_code != ResultCode.INVALID_TASK_PID.getValue()) {
        logOutput(shExec.getOutput());
        throw new IOException("Problem signalling task " + taskPid + " with " +
                              signal + "; exit = " + ret_code);
      }
    }
  }

  @Override
  public void truncateLogsAsUser(String user, List<Task> allAttempts)
    throws IOException {
    
    Task firstTask = allAttempts.get(0);
    String taskid = firstTask.getTaskID().toString();
    
    LocalDirAllocator ldirAlloc =
        new LocalDirAllocator(JobConf.MAPRED_LOCAL_DIR_PROPERTY);
    String taskRanFile = TaskTracker.TT_LOG_TMP_DIR + Path.SEPARATOR + taskid;
    Configuration conf = getConf();
    
    //write the serialized task information to a file to pass to the truncater
    Path taskRanFilePath = 
      ldirAlloc.getLocalPathForWrite(taskRanFile, conf);
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    FSDataOutputStream out = lfs.create(taskRanFilePath);
    out.writeInt(allAttempts.size());
    for (Task t : allAttempts) {
      out.writeBoolean(t.isMapTask());
      t.write(out);
    }
    out.close();
    lfs.setPermission(taskRanFilePath, 
                      FsPermission.createImmutable((short)0755));
    
    List<String> command = new ArrayList<String>();
    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
    command.add(jvm.toString());
    command.add("-Djava.library.path=" + 
                System.getProperty("java.library.path"));
    command.add("-Dhadoop.log.dir=" + TaskLog.getBaseLogDir());
    command.add("-Dhadoop.root.logger=INFO,console");
    command.add("-classpath");
    command.add(System.getProperty("java.class.path"));
    // main of TaskLogsTruncater
    command.add(TaskLogsTruncater.class.getName()); 
    command.add(taskRanFilePath.toString());
    String[] taskControllerCmd = new String[4 + command.size()];
    taskControllerCmd[0] = taskControllerExe;
    taskControllerCmd[1] = user;
    taskControllerCmd[2] = localStorage.getDirsString();
    taskControllerCmd[3] = Integer.toString(
        Commands.RUN_COMMAND_AS_USER.getValue());

    int i = 4;
    for (String cmdArg : command) {
      taskControllerCmd[i++] = cmdArg;
    }
    if (LOG.isDebugEnabled()) {
      for (String cmd : taskControllerCmd) {
        LOG.debug("taskctrl command = " + cmd);
      }
    }
    ShellCommandExecutor shExec = new ShellCommandExecutor(taskControllerCmd);
    try {
      shExec.execute();
    } catch (Exception e) {
      LOG.warn("Exit code from " + taskControllerExe.toString() + " is : "
          + shExec.getExitCode() + " for truncateLogs");
      LOG.warn("Exception thrown by " + taskControllerExe.toString() + " : "
          + StringUtils.stringifyException(e));
      LOG.info("Output from LinuxTaskController's "
               + taskControllerExe.toString() + " follows:");
      logOutput(shExec.getOutput());
      lfs.delete(taskRanFilePath, false);
      throw new IOException(e);
    }
    lfs.delete(taskRanFilePath, false);
    if (LOG.isDebugEnabled()) {
      LOG.info("Output from LinuxTaskController's "
               + taskControllerExe.toString() + " follows:");
      logOutput(shExec.getOutput());
    }
  }

  @Override
  public String getRunAsUser(JobConf conf) {
    return conf.getUser();
  }
}

