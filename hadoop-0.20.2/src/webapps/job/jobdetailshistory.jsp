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
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.*"
  import="org.apache.hadoop.mapred.JobHistory.*"
  import="java.security.PrivilegedExceptionAction"
  import="org.apache.hadoop.security.AccessControlException"
  import="org.apache.hadoop.mapreduce.JobACL"
  import="org.apache.hadoop.security.authorize.AccessControlList"
%>
<%!	private static final long serialVersionUID = 1L;
%>

<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
    String logFile = request.getParameter("logFile");

    String jobid = JSPUtil.getJobID(new Path(logFile).getName());
	
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
    JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
        response, jobTracker, fs, new Path(logFile));
    if (job == null) {
      return;
    }

  	String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
%>
<html>
<head>
<title>
Hadoop Job <%=jobid %> on History Viewer
</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>

<h2>Hadoop Job <%=jobid %> on <a href="jobhistory.jsp">History Viewer</a></h2>

<b>User: </b> <%=HtmlQuoting.quoteHtmlChars(job.get(Keys.USER)) %><br/> 
<b>JobName: </b> <%=HtmlQuoting.quoteHtmlChars(job.get(Keys.JOBNAME)) %><br/>  
<b>JobConf: </b> <a href="jobconf_history.jsp?logFile=<%=encodedLogFileName%>"> 
                 <%=job.get(Keys.JOBCONF) %></a><br/> 
<%         
  Map<JobACL, AccessControlList> jobAcls = job.getJobACLs();
  JSPUtil.printJobACLs(jobTracker, jobAcls, out);
%> 
<b>Submitted At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 )  %><br/> 
<b>Launched At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) %><br/>
<b>Finished At: </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) %><br/>
<b>Status: </b> <%= ((job.get(Keys.JOB_STATUS) == "")?"Incomplete" :job.get(Keys.JOB_STATUS)) %><br/> 
<%
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    int totalMaps = 0 ; 
    int totalReduces = 0;
    int totalCleanups = 0; 
    int totalSetups = 0; 
    int numFailedMaps = 0; 
    int numKilledMaps = 0;
    int numFailedReduces = 0 ; 
    int numKilledReduces = 0;
    int numFinishedCleanups = 0;
    int numFailedCleanups = 0;
    int numKilledCleanups = 0;
    int numFinishedSetups = 0;
    int numFailedSetups = 0;
    int numKilledSetups = 0;
	
    long mapStarted = 0 ; 
    long mapFinished = 0 ; 
    long reduceStarted = 0 ; 
    long reduceFinished = 0;
    long cleanupStarted = 0;
    long cleanupFinished = 0; 
    long setupStarted = 0;
    long setupFinished = 0; 
        
    Map <String,String> allHosts = new TreeMap<String,String>();
    for (JobHistory.Task task : tasks.values()) {
      Map<String, TaskAttempt> attempts = task.getTaskAttempts();
      allHosts.put(task.get(Keys.HOSTNAME), "");
      for (TaskAttempt attempt : attempts.values()) {
        long startTime = attempt.getLong(Keys.START_TIME) ; 
        long finishTime = attempt.getLong(Keys.FINISH_TIME) ; 
        if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))){
          if (mapStarted==0 || mapStarted > startTime ) {
            mapStarted = startTime; 
          }
          if (mapFinished < finishTime ) {
            mapFinished = finishTime ; 
          }
          totalMaps++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedMaps++; 
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledMaps++;
          }
        } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) {
          if (reduceStarted==0||reduceStarted > startTime) {
            reduceStarted = startTime ; 
          }
          if (reduceFinished < finishTime) {
            reduceFinished = finishTime; 
          }
          totalReduces++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedReduces++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledReduces++;
          }
        } else if (Values.CLEANUP.name().equals(task.get(Keys.TASK_TYPE))) {
          if (cleanupStarted==0||cleanupStarted > startTime) {
            cleanupStarted = startTime ; 
          }
          if (cleanupFinished < finishTime) {
            cleanupFinished = finishTime; 
          }
          totalCleanups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedCleanups++;
          } else if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedCleanups++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledCleanups++;
          } 
        } else if (Values.SETUP.name().equals(task.get(Keys.TASK_TYPE))) {
          if (setupStarted==0||setupStarted > startTime) {
            setupStarted = startTime ; 
          }
          if (setupFinished < finishTime) {
            setupFinished = finishTime; 
          }
          totalSetups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedSetups++;
          } else if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedSetups++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledSetups++;
          }
        }
      }
    }
%>
<b><a href="analysejobhistory.jsp?logFile=<%=encodedLogFileName%>">Analyse This Job</a></b> 
<hr/>
<center>
<table>
<tr>
<th>Kind</th><th>Total Tasks(successful+failed+killed)</th><th>Successful tasks</th><th>Failed tasks</th><th>Killed tasks</th><th>Start Time</th><th>Finish Time</th>
</tr>
<tr>
<th>Setup</th>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.SETUP.name() %>&status=all">
        <%=totalSetups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.SETUP.name() %>&status=<%=Values.SUCCESS %>">
        <%=numFinishedSetups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.SETUP.name() %>&status=<%=Values.FAILED %>">
        <%=numFailedSetups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.SETUP.name() %>&status=<%=Values.KILLED %>">
        <%=numKilledSetups%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, setupStarted, 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, setupFinished, setupStarted) %></td>
</tr>
<tr>
<th>Map</th>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.MAP.name() %>&status=all">
        <%=totalMaps %></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.MAP.name() %>&status=<%=Values.SUCCESS %>">
        <%=job.getInt(Keys.FINISHED_MAPS) %></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.MAP.name() %>&status=<%=Values.FAILED %>">
        <%=numFailedMaps %></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.MAP.name() %>&status=<%=Values.KILLED %>">
        <%=numKilledMaps %></a></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, mapStarted, 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, mapFinished, mapStarted) %></td>
</tr>
<tr>
<th>Reduce</th>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.REDUCE.name() %>&status=all">
        <%=totalReduces%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.SUCCESS %>">
        <%=job.getInt(Keys.FINISHED_REDUCES)%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.FAILED %>">
        <%=numFailedReduces%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.KILLED %>">
        <%=numKilledReduces%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, reduceStarted, 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, reduceFinished, reduceStarted) %></td>
</tr>
<tr>
<th>Cleanup</th>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.CLEANUP.name() %>&status=all">
        <%=totalCleanups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.CLEANUP.name() %>&status=<%=Values.SUCCESS %>">
        <%=numFinishedCleanups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.CLEANUP.name() %>&status=<%=Values.FAILED %>">
        <%=numFailedCleanups%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=encodedLogFileName%>&taskType=<%=Values.CLEANUP.name() %>&status=<%=Values.KILLED %>">
        <%=numKilledCleanups%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, cleanupStarted, 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, cleanupFinished, cleanupStarted) %></td>
</tr>
</table>

<br>
<br>

<table border=2 cellpadding="5" cellspacing="2">
  <tr>
  <th><br/></th>
  <th>Counter</th>
  <th>Map</th>
  <th>Reduce</th>
  <th>Total</th>
</tr>

<%  

 Counters totalCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.COUNTERS));
 Counters mapCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.MAP_COUNTERS));
 Counters reduceCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.REDUCE_COUNTERS));

 if (totalCounters != null) {
   for (String groupName : totalCounters.getGroupNames()) {
     Counters.Group totalGroup = totalCounters.getGroup(groupName);
     Counters.Group mapGroup = mapCounters.getGroup(groupName);
     Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
  
     Format decimal = new DecimalFormat();
  
     boolean isFirst = true;
     Iterator<Counters.Counter> ctrItr = totalGroup.iterator();
     while(ctrItr.hasNext()) {
       Counters.Counter counter = ctrItr.next();
       String name = counter.getDisplayName();
       String mapValue = 
         decimal.format(mapGroup.getCounter(name));
       String reduceValue = 
         decimal.format(reduceGroup.getCounter(name));
       String totalValue = decimal.format(counter.getCounter());
%>
       <tr>
<%
       if (isFirst) {
         isFirst = false;
%>
         <td rowspan="<%=totalGroup.size()%>">
         <%=HtmlQuoting.quoteHtmlChars(totalGroup.getDisplayName())%></td>
<%
       }
%>
       <td><%=HtmlQuoting.quoteHtmlChars(counter.getDisplayName())%></td>
       <td align="right"><%=mapValue%></td>
       <td align="right"><%=reduceValue%></td>
       <td align="right"><%=totalValue%></td>
     </tr>
<%
      }
    }
  }
%>
</table>
<br>

<br/>
 <%
    DefaultJobHistoryParser.FailedOnNodesFilter filter = 
                 new DefaultJobHistoryParser.FailedOnNodesFilter();
    JobHistory.parseHistoryFromFS(logFile, filter, fs); 
    Map<String, Set<String>> badNodes = filter.getValues(); 
    if (badNodes.size() > 0) {
 %>
<h3>Failed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Failed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<String> failedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
        for (String t : failedTasks) {
%>
          <a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=t %>"><%=t %></a>,&nbsp;
<%		  
        }
%>	
        </td>
        </tr>
<%	  
      }
	}
 %>
</table>
<br/>

 <%
    DefaultJobHistoryParser.KilledOnNodesFilter killedFilter =
                 new DefaultJobHistoryParser.KilledOnNodesFilter();
    JobHistory.parseHistoryFromFS(logFile, filter, fs); 
    badNodes = killedFilter.getValues(); 
    if (badNodes.size() > 0) {
 %>
<h3>Killed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Killed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<String> killedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
        for (String t : killedTasks) {
%>
          <a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=t %>"><%=t %></a>,&nbsp;
<%		  
        }
%>	
        </td>
        </tr>
<%	  
      }
    }
%>
</table>
</center>
</body></html>
