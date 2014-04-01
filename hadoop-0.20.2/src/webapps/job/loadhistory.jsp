<%
/*
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
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
    PathFilter jobLogFileFilter = new PathFilter() {
      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml"));
      }
    };
    
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String jobId = request.getParameter("jobid");
    JobHistory.JobInfo job = (JobHistory.JobInfo)
                               request.getSession().getAttribute("job");
    // if session attribute of JobInfo exists and is of different job's,
    // then remove the attribute
    // if the job has not yet finished, remove the attribute sothat it 
    // gets refreshed.
    boolean isJobComplete = false;
    if (null != job) {
      String jobStatus = job.get(Keys.JOB_STATUS);
      isJobComplete = Values.SUCCESS.name() == jobStatus
                      || Values.FAILED.name() == jobStatus
                      || Values.KILLED.name() == jobStatus;
    }
    if (null != job && 
       (!jobId.equals(job.get(Keys.JOBID)) 
         || !isJobComplete)) {
      // remove jobInfo from session, keep only one job in session at a time
      request.getSession().removeAttribute("job"); 
      job = null ; 
    }
	
    if (null == job) {
      String jobLogFile = request.getParameter("logFile");
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobLogFile, job, fs) ; 
      request.getSession().setAttribute("job", job);
      request.getSession().setAttribute("fs", fs);
    }
%>
