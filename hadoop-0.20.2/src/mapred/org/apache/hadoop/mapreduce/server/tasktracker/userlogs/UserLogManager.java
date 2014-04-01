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
package org.apache.hadoop.mapreduce.server.tasktracker.userlogs;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.DefaultTaskController;
import org.apache.hadoop.mapred.JobLocalizer;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLogsTruncater;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.UserLogCleaner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This manages user logs on the {@link TaskTracker}.
 */
public class UserLogManager {
  private static final Log LOG = LogFactory.getLog(UserLogManager.class);
  private BlockingQueue<UserLogEvent> userLogEvents = 
    new LinkedBlockingQueue<UserLogEvent>();
  private TaskLogsTruncater taskLogsTruncater;
  private UserLogCleaner userLogCleaner;
  private final TaskController taskController;

  private Thread monitorLogEvents = new Thread() {
    @Override
    public void run() {
      while (true) {
        try {
          monitor();
        } catch (Exception e) {
          LOG.warn("Exception while monitoring user log events", e);
        }
      }
    }
  };

  /**
   * Create the user log manager to manage user logs on {@link TaskTracker}.
   * 
   * It should be explicitly started using {@link #start()} to start functioning
   * 
   * @param conf The {@link Configuration}
   * @param taskController The task controller to delete the log files
   * 
   * @throws IOException
   */
  public UserLogManager(Configuration conf,
                        TaskController taskController) throws IOException {
    this.taskController = taskController;
    setFields(conf);
  }
  
  /**
   * Create the user log manager to manage user logs on {@link TaskTracker}.
   * This constructor is there mainly for unit tests.
   * 
   * @param conf The {@link Configuration}
   *
   * @throws IOException
   */
  public UserLogManager(Configuration conf) throws IOException {
    Class<? extends TaskController> taskControllerClass = 
      conf.getClass("mapred.task.tracker.task-controller", 
                     DefaultTaskController.class, TaskController.class);
    TaskController taskController = 
     (TaskController) ReflectionUtils.newInstance(taskControllerClass, conf);
    this.taskController = taskController;
    setFields(conf);
  }
  
  private void setFields(Configuration conf) throws IOException {
    taskLogsTruncater = new TaskLogsTruncater(conf);
    userLogCleaner = new UserLogCleaner(this, conf);
    monitorLogEvents.setDaemon(true);
  }

  /**
   * Get the taskController for deleting logs.
   * @return the TaskController
   */
  public TaskController getTaskController() {
    return taskController;
  }

  /**
   * Starts managing the logs
   */
  public void start() {
    userLogCleaner.start();
    monitorLogEvents.start();
  }

  protected void monitor() throws Exception {
    UserLogEvent event = userLogEvents.take();
    processEvent(event);
  }

  protected void processEvent(UserLogEvent event) throws IOException {
    if (event instanceof JvmFinishedEvent) {
      doJvmFinishedAction((JvmFinishedEvent) event);
    } else if (event instanceof JobCompletedEvent) {
      doJobCompletedAction((JobCompletedEvent) event);
    } else if (event instanceof JobStartedEvent) {
      doJobStartedAction((JobStartedEvent) event);
    } else if (event instanceof DeleteJobEvent) {
      doDeleteJobAction((DeleteJobEvent) event);
    } else { 
      LOG.warn("Unknown event " + event.getEventType() + " passed.");
    }
  }

  /**
   * Called during TaskTracker restart/re-init.
   * 
   * @param conf
   *          TT's conf
   * @throws IOException
   */
  public void clearOldUserLogs(Configuration conf) throws IOException {
    userLogCleaner.clearOldUserLogs(conf);
  }

  private void doJvmFinishedAction(JvmFinishedEvent event) throws IOException {
    //check whether any of the logs are over the limit, and if so
    //invoke the truncator to run as the user
    if (taskLogsTruncater.shouldTruncateLogs(event.getJvmInfo())) {
      String user = event.getJvmInfo().getAllAttempts().get(0).getUser();
      taskController.truncateLogsAsUser(user, 
                                        event.getJvmInfo().getAllAttempts());
    }
  }

  private void doJobStartedAction(JobStartedEvent event) {
    userLogCleaner.unmarkJobFromLogDeletion(event.getJobID());
  }

  private void doJobCompletedAction(JobCompletedEvent event) {
    userLogCleaner.markJobLogsForDeletion(event.getJobCompletionTime(), event
        .getRetainHours(), event.getJobID());
  }

  private void doDeleteJobAction(DeleteJobEvent event) throws IOException {
    userLogCleaner.deleteJobLogs(event.getJobID());
  }

  /**
   * Add the {@link UserLogEvent} for processing.
   * 
   * @param event
   */
  public void addLogEvent(UserLogEvent event) {
    userLogEvents.add(event);
  }

  /**
   * Get {@link UserLogCleaner}.
   * 
   * This method is called only from unit tests.
   * 
   * @return {@link UserLogCleaner}
   */
  public UserLogCleaner getUserLogCleaner() {
    return userLogCleaner;
  }
}
