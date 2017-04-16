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
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.util.StringUtils;

/**
 * A servlet that is run by the TaskTrackers to provide the task logs via http.
 */
public class TaskLogServlet extends HttpServlet {
  private static final long serialVersionUID = -6615764817774487321L;
  
  private static final Log LOG =
    LogFactory.getLog(TaskLog.class);
  
  private boolean haveTaskLog(TaskAttemptID taskId, boolean isCleanup,
      TaskLog.LogName type) {
    File f = TaskLog.getTaskLogFile(taskId, isCleanup, type);
    return f.canRead();
  }

  /**
   * Construct the taskLogUrl
   * @param taskTrackerHostName
   * @param httpPort
   * @param taskAttemptID
   * @return the taskLogUrl
   */
  public static String getTaskLogUrl(String taskTrackerHostName,
      String httpPort, String taskAttemptID) {
    return ("http://" + taskTrackerHostName + ":" + httpPort
        + "/tasklog?attemptid=" + taskAttemptID);
  }

  private void printTaskLog(HttpServletResponse response,
                            OutputStream out, TaskAttemptID taskId, 
                            long start, long end, boolean plainText, 
                            TaskLog.LogName filter, boolean isCleanup) 
  throws IOException {
    if (!plainText) {
      out.write(("<br><b><u>" + filter + " logs</u></b><br>\n" +
                 "<pre>\n").getBytes());
    }

    try {
      InputStream taskLogReader = 
        new TaskLog.Reader(taskId, filter, start, end, isCleanup);
      byte[] b = new byte[65536];
      int result;
      while (true) {
        result = taskLogReader.read(b);
        if (result > 0) {
          if (plainText) {
            out.write(b, 0, result); 
          } else {
            HtmlQuoting.quoteHtmlChars(out, b, 0, result);
          }
        } else {
          break;
        }
      }
      taskLogReader.close();
      if( !plainText ) {
        out.write("</pre></td></tr></table><hr><br>\n".getBytes());
      }
    } catch (IOException ioe) {
      if (filter == TaskLog.LogName.DEBUGOUT) {
        if (!plainText) {
           out.write("</pre><hr><br>\n".getBytes());
         }
        // do nothing
      }
      else {
        String msg = "Failed to retrieve " + filter + " log for task: " + 
                     taskId;
        LOG.warn(msg, ioe);
        response.sendError(HttpServletResponse.SC_GONE, msg);
      }
    }
  }

  /**
   * Validates if the given user has job view permissions for this job.
   * conf contains jobOwner and job-view-ACLs.
   * We allow jobOwner, superUser(i.e. mrOwner) and cluster administrators and
   * users and groups specified in configuration using
   * mapreduce.job.acl-view-job to view job.
   */
  private void checkAccessForTaskLogs(JobConf conf, String user, String jobId,
      TaskTracker tracker) throws AccessControlException {

    if (!tracker.areACLsEnabled()) {
      return;
    }

    // build job view ACL by reading from conf
    AccessControlList jobViewACL = tracker.getJobACLsManager().
        constructJobACLs(conf).get(JobACL.VIEW_JOB);

    // read job queue name from conf
    String queue = conf.getQueueName();

    // build queue admins ACL by reading from conf
    AccessControlList queueAdminsACL = new AccessControlList(
        conf.get(QueueManager.toFullPropertyName(queue,
            QueueACL.ADMINISTER_JOBS.getAclName()), " "));

    String jobOwner = conf.get("user.name");
    UserGroupInformation callerUGI =
        UserGroupInformation.createRemoteUser(user);

    // check if user is queue admin or cluster admin or jobOwner or member of
    // job-view-acl
    if (!queueAdminsACL.isUserAllowed(callerUGI)) {
      tracker.getACLsManager().checkAccess(jobId, callerUGI, queue,
          Operation.VIEW_TASK_LOGS, jobOwner, jobViewACL);
    }
  }

  /**
   * Builds a JobConf object by reading the job-acls.xml file.
   * This doesn't load the default resources.
   *
   * Returns null if job-acls.xml is not there in userlogs/$jobid on
   * local file system. This can happen when we restart the cluster with job
   * level authorization enabled(but was disabled on earlier cluster) and
   * viewing task logs of old jobs(i.e. jobs finished on earlier unsecure
   * cluster).
   */
  static JobConf getConfFromJobACLsFile(JobID jobId) {
    Path jobAclsFilePath = new Path(
        TaskLog.getJobDir(jobId).toString(),
        TaskTracker.jobACLsFile);
    JobConf conf = null;
    if (new File(jobAclsFilePath.toUri().getPath()).exists()) {
      conf = new JobConf(false);
      conf.addResource(jobAclsFilePath);
    }
    return conf;
  }

  /**
   * Get the logs via http.
   */
  @Override
  public void doGet(HttpServletRequest request, 
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    long start = 0;
    long end = -1;
    boolean plainText = false;
    TaskLog.LogName filter = null;
    boolean isCleanup = false;

    String attemptIdStr = request.getParameter("attemptid");
    if (attemptIdStr == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
                         "Argument attemptid is required");
      return;
    }

    String logFilter = request.getParameter("filter");
    if (logFilter != null) {
      try {
        filter = TaskLog.LogName.valueOf(TaskLog.LogName.class, 
                                         logFilter.toUpperCase());
      } catch (IllegalArgumentException iae) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                           "Illegal value for filter: " + logFilter);
        return;
      }
    }
    
    String sLogOff = request.getParameter("start");
    if (sLogOff != null) {
      start = Long.valueOf(sLogOff).longValue();
    }
    
    String sLogEnd = request.getParameter("end");
    if (sLogEnd != null) {
      end = Long.valueOf(sLogEnd).longValue();
    }
    
    String sPlainText = request.getParameter("plaintext");
    if (sPlainText != null) {
      plainText = Boolean.valueOf(sPlainText);
    }
    
    String sCleanup = request.getParameter("cleanup");
    if (sCleanup != null) {
      isCleanup = Boolean.valueOf(sCleanup);
    }
    
    TaskAttemptID attemptId = TaskAttemptID.forName(attemptIdStr);
    if (!TaskLog.getAttemptDir(attemptId, isCleanup).exists()) {
      response.sendError(HttpServletResponse.SC_GONE,
          "Task log directory for task " + attemptId +
          " does not exist. May be cleaned up by Task Tracker, if older logs.");
      return;
    }

    // get user name who is accessing
    String user = request.getRemoteUser();
    if (user != null) {
      ServletContext context = getServletContext();
      TaskTracker taskTracker = (TaskTracker) context.getAttribute(
          "task.tracker");
      JobID jobId = attemptId.getJobID();

      // get jobACLConf from ACLs file
      JobConf jobACLConf = getConfFromJobACLsFile(jobId);
      // Ignore authorization if job-acls.xml is not found
      if (jobACLConf != null) {
        try {
          checkAccessForTaskLogs(jobACLConf, user,
              jobId.toString(), taskTracker);
        } catch (AccessControlException e) {
          String errMsg = "User " + user + " failed to view tasklogs of job " +
              jobId + "!\n\n" + e.getMessage();
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED, errMsg);
          return;
        }
      }
    }

    OutputStream out = response.getOutputStream();
    response.setContentType("text/html; charset=utf-8");
    if( !plainText ) {
      out.write(("<html>\n" +
                 "<title>Task Logs: '" + attemptId + "'</title>\n" +
                 "<body>\n" +
                 "<h1>Task Logs: '" +  attemptId +  "'</h1><br>\n").getBytes()); 

      if (filter == null) {
        printTaskLog(response, out, attemptId, start, end, plainText, 
                     TaskLog.LogName.STDOUT, isCleanup);
        printTaskLog(response, out, attemptId, start, end, plainText, 
                     TaskLog.LogName.STDERR, isCleanup);
        if (haveTaskLog(attemptId, isCleanup, TaskLog.LogName.SYSLOG)) {
          printTaskLog(response, out, attemptId, start, end, plainText,
              TaskLog.LogName.SYSLOG, isCleanup);
        }
        if (haveTaskLog(attemptId, isCleanup, TaskLog.LogName.DEBUGOUT)) {
          printTaskLog(response, out, attemptId, start, end, plainText, 
                       TaskLog.LogName.DEBUGOUT, isCleanup);
        }
        if (haveTaskLog(attemptId, isCleanup, TaskLog.LogName.PROFILE)) {
          printTaskLog(response, out, attemptId, start, end, plainText, 
                       TaskLog.LogName.PROFILE, isCleanup);
        }
      } else {
        printTaskLog(response, out, attemptId, start, end, plainText, filter,
                     isCleanup);
      }
      
      out.write("</body></html>\n".getBytes());
      out.close();
    } else if (filter == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST,
          "You must supply a value for `filter' (STDOUT, STDERR, or SYSLOG) if you set plainText = true");
    } else {
      response.setContentType("text/plain; charset=utf-8");
      printTaskLog(response, out, attemptId, start, end, plainText, filter, 
                   isCleanup);
    } 
  }
}
