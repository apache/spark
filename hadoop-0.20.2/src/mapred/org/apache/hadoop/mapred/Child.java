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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;

/** 
 * The main() for child processes. 
 */

class Child {

  public static final Log LOG =
    LogFactory.getLog(Child.class);

  static volatile TaskAttemptID taskid = null;
  static volatile boolean currentJobSegmented = true;

  static volatile boolean isCleanup;
  static String cwd;

  private static boolean isChildJvm = false;

  /**
   * Return true if running in a task/child JVM. This should
   * only be used for asserts/safety checks.
   */
  public static boolean isChildJvm() {
    return isChildJvm;
  }

  static boolean logIsSegmented(JobConf job) {
    return (job.getNumTasksToExecutePerJvm() != 1);
  }

  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");
    isChildJvm = true;

    final JobConf defaultConf = new JobConf();
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address = new InetSocketAddress(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    final String logLocation = args[3];
    final int SLEEP_LONGER_COUNT = 5;
    int jvmIdInt = Integer.parseInt(args[4]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),firstTaskid.isMap(),jvmIdInt);
    
    cwd = System.getenv().get(TaskRunner.HADOOP_WORK_DIR);
    if (cwd == null) {
      throw new IOException("Environment variable " + 
                             TaskRunner.HADOOP_WORK_DIR + " is not set");
    }

    // file name is passed thru env
    String jobTokenFile = 
      System.getenv().get(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    Credentials credentials = 
      TokenCache.loadTokens(jobTokenFile, defaultConf);
    LOG.debug("loading token. # keys =" +credentials.numberOfSecretKeys() + 
        "; from file=" + jobTokenFile);
    
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    jt.setService(new Text(address.getAddress().getHostAddress() + ":"
        + address.getPort()));
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);

    UserGroupInformation taskOwner 
     = UserGroupInformation.createRemoteUser(firstTaskid.getJobID().toString());
    taskOwner.addToken(jt);
    
    // Set the credentials
    defaultConf.setCredentials(credentials);
    
    final TaskUmbilicalProtocol umbilical = 
      taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
              TaskUmbilicalProtocol.versionID,
              address,
              defaultConf);
        }
    });
    
    int numTasksToExecute = -1; //-1 signifies "no limit"
    int numTasksExecuted = 0;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (taskid != null) {
            TaskLog.syncLogs
              (logLocation, taskid, isCleanup, currentJobSegmented);
          }
        } catch (Throwable throwable) {
        }
      }
    });
    Thread t = new Thread() {
      public void run() {
        //every so often wake up and syncLogs so that we can track
        //logs of the currently running task
        while (true) {
          try {
            Thread.sleep(5000);
            if (taskid != null) {
              TaskLog.syncLogs
                (logLocation, taskid, isCleanup, currentJobSegmented);
            }
          } catch (InterruptedException ie) {
          } catch (IOException iee) {
            LOG.error("Error in syncLogs: " + iee);
            System.exit(-1);
          }
        }
      }
    };
    t.setName("Thread for syncLogs");
    t.setDaemon(true);
    t.start();
    
    String pid = "";
    if (!Shell.WINDOWS) {
      pid = System.getenv().get("JVM_PID");
    }
    JvmContext context = new JvmContext(jvmId, pid);
    int idleLoopCount = 0;
    Task task = null;
    
    UserGroupInformation childUGI = null;
   
    final JvmContext jvmContext = context;
    try {
      while (true) {
        taskid = null;
        currentJobSegmented = true;

        JvmTask myTask = umbilical.getTask(context);
        if (myTask.shouldDie()) {
          break;
        } else {
          if (myTask.getTask() == null) {
            taskid = null;
            currentJobSegmented = true;

            if (++idleLoopCount >= SLEEP_LONGER_COUNT) {
              //we sleep for a bigger interval when we don't receive
              //tasks for a while
              Thread.sleep(1500);
            } else {
              Thread.sleep(500);
            }
            continue;
          }
        }
        idleLoopCount = 0;
        task = myTask.getTask();
        task.setJvmContext(jvmContext);
        taskid = task.getTaskID();

        // Create the JobConf and determine if this job gets segmented task logs
        final JobConf job = new JobConf(task.getJobFile());
        currentJobSegmented = logIsSegmented(job);

        isCleanup = task.isTaskCleanupTask();
        // reset the statistics for the task
        FileSystem.clearStatistics();
        
        // Set credentials
        job.setCredentials(defaultConf.getCredentials());
        //forcefully turn off caching for localfs. All cached FileSystems
        //are closed during the JVM shutdown. We do certain
        //localfs operations in the shutdown hook, and we don't
        //want the localfs to be "closed"
        job.setBoolean("fs.file.impl.disable.cache", false);

        // set the jobTokenFile into task
        task.setJobTokenSecret(JobTokenSecretManager.
            createSecretKey(jt.getPassword()));

        // setup the child's mapred-local-dir. The child is now sandboxed and
        // can only see files down and under attemtdir only.
        TaskRunner.setupChildMapredLocalDirs(task, job);
        
        // setup the child's attempt directories
        localizeTask(task, job, logLocation);

        //setupWorkDir actually sets up the symlinks for the distributed
        //cache. After a task exits we wipe the workdir clean, and hence
        //the symlinks have to be rebuilt.
        TaskRunner.setupWorkDir(job, new File(cwd));
        
        //create the index file so that the log files 
        //are viewable immediately
        TaskLog.syncLogs
          (logLocation, taskid, isCleanup, logIsSegmented(job));
        
        numTasksToExecute = job.getNumTasksToExecutePerJvm();
        assert(numTasksToExecute != 0);

        task.setConf(job);

        // Initiate Java VM metrics
        JvmMetrics.init(task.getPhase().toString(), job.getSessionId());
        LOG.debug("Creating remote user to execute task: " + job.get("user.name"));
        childUGI = UserGroupInformation.createRemoteUser(job.get("user.name"));
        // Add tokens to new user so that it may execute its task correctly.
        for(Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
          childUGI.addToken(token);
        }
        
        // Create a final reference to the task for the doAs block
        final Task taskFinal = task;
        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            try {
              // use job-specified working directory
              FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
              taskFinal.run(job, umbilical);        // run the task
            } finally {
              TaskLog.syncLogs
                (logLocation, taskid, isCleanup, logIsSegmented(job));
              TaskLogsTruncater trunc = new TaskLogsTruncater(defaultConf);
              trunc.truncateLogs(new JVMInfo(
                  TaskLog.getAttemptDir(taskFinal.getTaskID(),
                    taskFinal.isTaskCleanupTask()), Arrays.asList(taskFinal)));
            }

            return null;
          }
        });
        if (numTasksToExecute > 0 && ++numTasksExecuted == numTasksToExecute) {
          break;
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskid, e.getMessage(), jvmContext);
    } catch (Exception exception) {
      LOG.warn("Error running child", exception);
      try {
        if (task != null) {
          // do cleanup for the task
          if(childUGI == null) {
            task.taskCleanup(umbilical);
          } else {
            final Task taskFinal = task;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                taskFinal.taskCleanup(umbilical);
                return null;
              }
            });
          }
        }
      } catch (Exception e) {
        LOG.info("Error cleaning up", e);
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskid != null) {
        umbilical.reportDiagnosticInfo(taskid, baos.toString(), jvmContext);
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
                + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        Throwable tCause = throwable.getCause();
        String cause = tCause == null 
                       ? throwable.getMessage() 
                       : StringUtils.stringifyException(tCause);
        umbilical.fatalError(taskid, cause, jvmContext);
      }
    } finally {
      RPC.stopProxy(umbilical);
      MetricsContext metricsContext = MetricsUtil.getContext("mapred");
      metricsContext.close();
      // Shutting down log4j of the child-vm... 
      // This assumes that on return from Task.run() 
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  static void localizeTask(Task task, JobConf jobConf, String logLocation) 
  throws IOException{
    
    // Do the task-type specific localization
    task.localizeConfiguration(jobConf);
    
    //write the localized task jobconf
    LocalDirAllocator lDirAlloc = 
      new LocalDirAllocator(JobConf.MAPRED_LOCAL_DIR_PROPERTY);
    Path localTaskFile =
      lDirAlloc.getLocalPathForWrite(TaskTracker.JOBFILE, jobConf);
    JobLocalizer.writeLocalJobFile(localTaskFile, jobConf);
    task.setJobFile(localTaskFile.toString());
    task.setConf(jobConf);
  }
}
