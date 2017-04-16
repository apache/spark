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

<%!	
  private static SimpleDateFormat dateFormat =
                                    new SimpleDateFormat("d/MM HH:mm:ss") ; 
%>
<%!	private static final long serialVersionUID = 1L;
%>

<%	
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String jobid = JSPUtil.getJobID(new Path(logFile).getName());
  String taskStatus = request.getParameter("status"); 
  String taskType = request.getParameter("taskType"); 
  
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
  JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobTracker, fs, new Path(logFile));
  if (job == null) {
    return;
  }
  Map<String, JobHistory.Task> tasks = job.getAllTasks(); 
%>
<html>
<head>
  <title><%=taskStatus%> <%=taskType %> task list for <%=jobid %></title>
  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  <link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>
<h2><%=taskStatus%> <%=taskType %> task list for <a href="jobdetailshistory.jsp?logFile=<%=encodedLogFileName%>"><%=jobid %> </a></h2>
<center>
<table class="jobtasks datatable">
<thead>
<tr><th>Task Id</th><th>Start Time</th><th>Finish Time<br/></th><th>Error</th></tr>
</thead>
<tbody>
<%
  for (JobHistory.Task task : tasks.values()) {
    if (taskType.equals(task.get(Keys.TASK_TYPE))){
      Map <String, TaskAttempt> taskAttempts = task.getTaskAttempts();
      for (JobHistory.TaskAttempt taskAttempt : taskAttempts.values()) {
        if (taskStatus.equals(taskAttempt.get(Keys.TASK_STATUS)) || 
          taskStatus.equals("all")){
          printTask(encodedLogFileName, taskAttempt, out); 
        }
      }
    }
  }
%>
</tbody>
</table>
<%!
  private void printTask(String logFile,
    JobHistory.TaskAttempt attempt, JspWriter out) throws IOException{
    out.print("<tr>"); 
    out.print("<td>" + "<a href=\"taskdetailshistory.jsp?logFile="+ logFile
     +"&tipid="+attempt.get(Keys.TASKID)+"\">" +
          attempt.get(Keys.TASKID) + "</a></td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
          attempt.getLong(Keys.START_TIME), 0 ) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
          attempt.getLong(Keys.FINISH_TIME),
          attempt.getLong(Keys.START_TIME) ) + "</td>");
    out.print("<td>" + HtmlQuoting.quoteHtmlChars(attempt.get(Keys.ERROR)) +
        "</td>");
    out.print("</tr>"); 
  }
%>
</center>
</body>
</html>
