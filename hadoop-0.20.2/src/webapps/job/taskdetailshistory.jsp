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
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapred.JobHistory.*"
%>

<%!	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ; %>
<%!	private static final long serialVersionUID = 1L;
%>

<%	
  String logFile = request.getParameter("logFile");
  String tipid = request.getParameter("tipid");
  if (logFile == null || tipid == null) {
    out.println("Missing job!!");
    return;
  }
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String jobid = JSPUtil.getJobID(new Path(encodedLogFileName).getName());
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
  JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobTracker, fs, new Path(logFile));
  if (job == null) {
    return;
  }
  JobHistory.Task task = job.getAllTasks().get(tipid); 
  String type = task.get(Keys.TASK_TYPE);
%>
<html>
<head>
<title><%=tipid %> attempts for <%=jobid %></title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>
<h2><%=tipid %> attempts for <a href="jobdetailshistory.jsp?logFile=<%=encodedLogFileName%>"> <%=jobid %> </a></h2>
<center>
<table class="jobtasks datatable">
<thead>
<tr><th>Task Id</th><th>Start Time</th>
<%	
  if (Values.REDUCE.name().equals(type)) {
%>
    <th>Shuffle Finished</th><th>Sort Finished</th>
<%
  }
%>
<th>Finish Time</th><th>Host</th><th>Error</th><th>Task Logs</th>
<th>Counters</th></tr>
</thead>
<tbody>
<%
  for (JobHistory.TaskAttempt attempt : task.getTaskAttempts().values()) {
    printTaskAttempt(attempt, type, out, encodedLogFileName);
  }
%>
</tbody>
</table>
</center>
<%	
  if (Values.MAP.name().equals(type)) {
%>
<h3>Input Split Locations</h3>
<table border="2" cellpadding="5" cellspacing="2">
<%
    for (String split : StringUtils.split(task.get(Keys.SPLITS)))
    {
      out.println("<tr><td>" + split + "</td></tr>");
    }
%>
</table>    
<%
  }
%>
<%!
  private void printTaskAttempt(JobHistory.TaskAttempt taskAttempt,
                                String type, JspWriter out,
                                String logFile) 
  throws Exception {
    out.print("<tr>"); 
    out.print("<td>" + taskAttempt.get(Keys.TASK_ATTEMPT_ID) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.START_TIME), 0 ) + "</td>"); 
    if (Values.REDUCE.name().equals(type)) {
      JobHistory.ReduceAttempt reduceAttempt = 
            (JobHistory.ReduceAttempt)taskAttempt; 
      out.print("<td>" + 
                StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED), 
                reduceAttempt.getLong(Keys.START_TIME)) + "</td>"); 
      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SORT_FINISHED), 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED)) + "</td>"); 
    }
    out.print("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.FINISH_TIME), 
              taskAttempt.getLong(Keys.START_TIME) ) + "</td>"); 
    out.print("<td>" + taskAttempt.get(Keys.HOSTNAME) + "</td>");
    out.print("<td>" + HtmlQuoting.quoteHtmlChars(taskAttempt.get(Keys.ERROR)) +
        "</td>");

    // Print task log urls
    out.print("<td>");	
    String taskLogsUrl = JobHistory.getTaskLogsUrl(taskAttempt);
    if (taskLogsUrl != null) {
	    String tailFourKBUrl = taskLogsUrl + "&start=-4097";
	    String tailEightKBUrl = taskLogsUrl + "&start=-8193";
	    String entireLogUrl = taskLogsUrl + "&all=true";
	    out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
	    out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
	    out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
    } else {
        out.print("n/a");
    }
    out.print("</td>");
    Counters counters = 
      Counters.fromEscapedCompactString(taskAttempt.get(Keys.COUNTERS));
    if (counters != null) {
      TaskAttemptID attemptId = 
        TaskAttemptID.forName(taskAttempt.get(Keys.TASK_ATTEMPT_ID));
      TaskID tipid = attemptId.getTaskID();
      org.apache.hadoop.mapreduce.JobID jobId = tipid.getJobID();
      out.print("<td>" 
       + "<a href=\"/taskstatshistory.jsp?attemptid=" + attemptId
           + "&logFile=" + logFile + "\">"
           + counters.size() + "</a></td>");
    } else {
      out.print("<td></td>");
    }
    out.print("</tr>"); 
  }
%>
</body>
</html>
