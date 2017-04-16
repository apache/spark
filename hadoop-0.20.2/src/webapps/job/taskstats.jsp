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
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String attemptid = request.getParameter("attemptid");
  TaskAttemptID attemptidObj = TaskAttemptID.forName(attemptid);
  // Obtain tipid for attemptId, if attemptId is available.
  TaskID tipidObj =
      (attemptidObj == null) ? TaskID.forName(request.getParameter("tipid"))
                             : attemptidObj.getTaskID();
  // Obtain jobid from tipid
  final JobID jobidObj = tipidObj.getJobID();
  String jobid = jobidObj.toString();
  
  JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
      request, response);
  if (!myJob.isViewJobAllowed()) {
    return; // user is not authorized to view this job
  }

  JobInProgress job = myJob.getJob();
  if (job == null) {
    out.print("<b>Job " + jobid + " not found.</b><br>\n");
    return;
  }
  
  Format decimal = new DecimalFormat();
  Counters counters;
  if (attemptid == null) {
    counters = tracker.getTipCounters(tipidObj);
    attemptid = tipidObj.toString(); // for page title etc
  }
  else {
    TaskStatus taskStatus = tracker.getTaskStatus(attemptidObj);
    counters = taskStatus.getCounters();
  }
%>

<html>
  <head>
    <title>Counters for <%=attemptid%></title>
    <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
    <link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
  </head>
<body>
<h1>Counters for <%=attemptid%></h1>

<hr>

<%
  if ( counters == null ) {
%>
    <h3>No counter information found for this task</h3>
<%
  } else {    
%>
    <table>
<%
      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();
%>
        <tr>
          <td colspan="3"><br/><b>
          <%=HtmlQuoting.quoteHtmlChars(displayGroupName)%></b></td>
        </tr>
<%
        for (Counters.Counter counter : group) {
          String displayCounterName = counter.getDisplayName();
          long value = counter.getCounter();
%>
          <tr>
            <td width="50"></td>
            <td><%=HtmlQuoting.quoteHtmlChars(displayCounterName)%></td>
            <td align="right"><%=decimal.format(value)%></td>
          </tr>
<%
        }
      }
%>
    </table>
<%
  }
%>

<hr>
<a href="jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
