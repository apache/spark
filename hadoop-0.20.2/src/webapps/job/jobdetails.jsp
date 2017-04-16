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
  import="java.text.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck"
  import="org.apache.hadoop.mapreduce.TaskType"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.mapreduce.JobACL"
  import="org.apache.hadoop.security.UserGroupInformation"
  import="java.security.PrivilegedExceptionAction"
  import="org.apache.hadoop.security.AccessControlException"
  import="org.apache.hadoop.security.authorize.AccessControlList"
%>

<%!	private static final long serialVersionUID = 1L;
%>

<%
  final JobTracker tracker = (JobTracker) application.getAttribute(
      "job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<%!
 
  private void printTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                double completePercent,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failedTaskAttempts = 0;
    int killedTaskAttempts = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failedTaskAttempts += task.numTaskFailures();
      killedTaskAttempts += task.numKilledTasks();
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print("<tr><th><a href=\"jobtasks.jsp?jobid=" + jobId + 
              "&type="+ kind + "&pagenum=1\">" + kind + 
              "</a></th><td align=\"right\">" + 
              StringUtils.formatPercent(completePercent, 2) +
              ServletUtil.percentageGraph((int)(completePercent * 100), 80) +
              "</td><td align=\"right\">" + 
              totalTasks + 
              "</td><td align=\"right\">" + 
              ((pendingTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=pending\">" + pendingTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((runningTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + runningTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + finishedTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + killedTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((failedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=failed\">" + failedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              " / " +
              ((killedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=killed\">" + killedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              "</td></tr>\n");
  }

  private void printJobLevelTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.isFailed()) {
        killedTasks += 1;
      }
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print(((runningTasks > 0)  
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + " Running" + 
                 "</a>" 
               : ((pendingTasks > 0) ? " Pending" :
                 ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + " Successful"
                 + "</a>" 
               : ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + " Failed" 
                + "</a>" : "None")))));
  }
  
  private void printConfirm(JspWriter out, String jobId) throws IOException{
    String url = "jobdetails.jsp?jobid=" + jobId;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url+"\"></head>"
        + "<body><h3> Are you sure you want to kill " + jobId
        + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"kill\" />"
        + "<input type=\"submit\" name=\"kill\" value=\"Kill\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }
  
%>       
<%   
    String jobId = request.getParameter("jobid"); 
    String refreshParam = request.getParameter("refresh");
    if (jobId == null) {
      out.println("<h2>Missing 'jobid'!</h2>");
      return;
    }
    
    int refresh = 60; // refresh every 60 seconds by default
    if (refreshParam != null) {
        try {
            refresh = Integer.parseInt(refreshParam);
        }
        catch (NumberFormatException ignored) {
        }
    }
    final JobID jobIdObj = JobID.forName(jobId);
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
                                                     request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();

    final String newPriority = request.getParameter("prio");
    String user = request.getRemoteUser();
    UserGroupInformation ugi = null;
    if (user != null) {
      ugi = UserGroupInformation.createRemoteUser(user);
    }

    String action = request.getParameter("action");
    if(JSPUtil.privateActionsAllowed(tracker.conf) && 
        "changeprio".equalsIgnoreCase(action) 
        && request.getMethod().equalsIgnoreCase("POST")) {
      if (ugi != null) {
        try {
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws IOException{

              // checks job modify permission
              tracker.setJobPriority(jobIdObj, 
                  JobPriority.valueOf(newPriority));
              return null;
            }
          });
        } catch(AccessControlException e) {
          String errMsg = "User " + user + " failed to modify priority of " +
              jobIdObj + "!<br><br>" + e.getMessage() +
              "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
              "\">Go back to Job</a><br>";
          JSPUtil.setErrorAndForward(errMsg, request, response);
          return;
        }
      }
      else {// no authorization needed
        tracker.setJobPriority(jobIdObj,
             JobPriority.valueOf(newPriority));;
      }
    }
    
    if(JSPUtil.privateActionsAllowed(tracker.conf)) {
      action = request.getParameter("action");
      if(action!=null && action.equalsIgnoreCase("confirm")) {
        printConfirm(out, jobId);
        return;
      }
      else if(action != null && action.equalsIgnoreCase("kill") &&
          request.getMethod().equalsIgnoreCase("POST")) {
        if (ugi != null) {
          try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                // checks job modify permission
                tracker.killJob(jobIdObj);// checks job modify permission
                return null;
              }
            });
          } catch(AccessControlException e) {
            String errMsg = "User " + user + " failed to kill " + jobIdObj +
                "!<br><br>" + e.getMessage() +
                "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
                "\">Go back to Job</a><br>";
            JSPUtil.setErrorAndForward(errMsg, request, response);
            return;
          }
        }
        else {// no authorization needed
          tracker.killJob(jobIdObj);
        }
      }
    }
%>

<%@page import="org.apache.hadoop.mapred.TaskGraphServlet"%>
<html>
<head>
  <% 
  if (refresh != 0) {
      %>
      <meta http-equiv="refresh" content="<%=refresh%>">
      <%
  }
  %>
<title>Hadoop <%=jobId%> on <%=trackerName%></title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>
<h1>Hadoop <%=jobId%> on <a href="jobtracker.jsp"><%=trackerName%></a></h1>

<% 
    if (job == null) {
      String historyFile = JobHistory.getHistoryFilePath(jobIdObj);
      if (historyFile == null) {
        out.println("<h2>Job " + jobId + " not known!</h2>");
        return;
      }
      String historyUrl = "/jobdetailshistory.jsp?logFile=" + 
          JobHistory.JobInfo.encodeJobHistoryFilePath(historyFile);
      response.sendRedirect(response.encodeRedirectURL(historyUrl));
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
    int flakyTaskTrackers = job.getNoOfBlackListedTrackers();
    out.print("<b>User:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getUser()) + "<br>\n");
    out.print("<b>Job Name:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getJobName()) + "<br>\n");
    out.print("<b>Job File:</b> <a href=\"jobconf.jsp?jobid=" + jobId + "\">" +
        profile.getJobFile() + "</a><br>\n");
    out.print("<b>Submit Host:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostName()) + "<br>\n");
    out.print("<b>Submit Host Address:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostAddress()) + "<br>\n");

    Map<JobACL, AccessControlList> jobAcls = status.getJobACLs();
    JSPUtil.printJobACLs(tracker, jobAcls, out);
    out.print("<b>Job Setup:</b>");
    printJobLevelTaskSummary(out, jobId, "setup", 
                             job.getTasks(TaskType.JOB_SETUP));
    out.print("<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Status:</b> Running<br>\n");
      out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
      out.print("<b>Running for:</b> " + StringUtils.formatTimeDiff(
          System.currentTimeMillis(), job.getStartTime()) + "<br>\n");
    } else {
      if (runState == JobStatus.SUCCEEDED) {
        out.print("<b>Status:</b> Succeeded<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Finished at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Finished in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.FAILED) {
        out.print("<b>Status:</b> Failed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Failed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Failed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.KILLED) {
        out.print("<b>Status:</b> Killed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Killed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Killed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      }
    }
    out.print("<b>Job Cleanup:</b>");
    printJobLevelTaskSummary(out, jobId, "cleanup", 
                             job.getTasks(TaskType.JOB_CLEANUP));
    out.print("<br>\n");
    if (flakyTaskTrackers > 0) {
      out.print("<b>Black-listed TaskTrackers:</b> " + 
          "<a href=\"jobblacklistedtrackers.jsp?jobid=" + jobId + "\">" +
          flakyTaskTrackers + "</a><br>\n");
    }
    if (job.getSchedulingInfo() != null) {
      out.print("<b>Job Scheduling information: </b>" +
          job.getSchedulingInfo().toString() +"\n");
    }
    out.print("<hr>\n");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th>" +
              "<th>Pending</th><th>Running</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"jobfailures.jsp?jobid=" + jobId + 
              "\">Failed/Killed<br>Task Attempts</a></th></tr>\n");
    printTaskSummary(out, jobId, "map", status.mapProgress(), 
                     job.getTasks(TaskType.MAP));
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getTasks(TaskType.REDUCE));
    out.print("</table>\n");
    
    %>
    <p/>
    <table border=2 cellpadding="5" cellspacing="2">
    <tr>
      <th><br/></th>
      <th>Counter</th>
      <th>Map</th>
      <th>Reduce</th>
      <th>Total</th>
    </tr>
    <%
    boolean isFine = true;
    Counters mapCounters = new Counters();
    isFine = job.getMapCounters(mapCounters);
    mapCounters = (isFine? mapCounters: new Counters());
    Counters reduceCounters = new Counters();
    isFine = job.getReduceCounters(reduceCounters);
    reduceCounters = (isFine? reduceCounters: new Counters());
    Counters totalCounters = new Counters();
    isFine = job.getCounters(totalCounters);
    totalCounters = (isFine? totalCounters: new Counters());
        
    for (String groupName : totalCounters.getGroupNames()) {
      Counters.Group totalGroup = totalCounters.getGroup(groupName);
      Counters.Group mapGroup = mapCounters.getGroup(groupName);
      Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
      
      Format decimal = new DecimalFormat();
      
      boolean isFirst = true;
      for (Counters.Counter counter : totalGroup) {
        String name = counter.getDisplayName();
        String mapValue = decimal.format(mapGroup.getCounter(name));
        String reduceValue = decimal.format(reduceGroup.getCounter(name));
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
          <td><%=HtmlQuoting.quoteHtmlChars(name)%></td>
          <td align="right"><%=mapValue%></td>
          <td align="right"><%=reduceValue%></td>
          <td align="right"><%=totalValue%></td>
        </tr>
        <%
      }
    }
    %>
    </table>

<hr>Map Completion Graph - 
<%
if("off".equals(request.getParameter("map.graph"))) {
  session.setAttribute("map.graph", "off");
} else if("on".equals(request.getParameter("map.graph"))){
  session.setAttribute("map.graph", "on");
}
if("off".equals(request.getParameter("reduce.graph"))) {
  session.setAttribute("reduce.graph", "off");
} else if("on".equals(request.getParameter("reduce.graph"))){
  session.setAttribute("reduce.graph", "on");
}

if("off".equals(session.getAttribute("map.graph"))) { %>
<a href="/jobdetails.jsp?jobid=<%=jobId%>&refresh=<%=refresh%>&map.graph=on" > open </a>
<%} else { %> 
<a href="/jobdetails.jsp?jobid=<%=jobId%>&refresh=<%=refresh%>&map.graph=off" > close </a>
<br><embed src="/taskgraph?type=map&jobid=<%=jobId%>" 
       width="<%=TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin%>" 
       height="<%=TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin%>"
       style="width:100%" type="image/svg+xml" pluginspage="http://www.adobe.com/svg/viewer/install/" />
<%}%>

<%if(job.getTasks(TaskType.REDUCE).length > 0) { %>
<hr>Reduce Completion Graph -
<%if("off".equals(session.getAttribute("reduce.graph"))) { %>
<a href="/jobdetails.jsp?jobid=<%=jobId%>&refresh=<%=refresh%>&reduce.graph=on" > open </a>
<%} else { %> 
<a href="/jobdetails.jsp?jobid=<%=jobId%>&refresh=<%=refresh%>&reduce.graph=off" > close </a>
 
 <br><embed src="/taskgraph?type=reduce&jobid=<%=jobId%>" 
       width="<%=TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin%>" 
       height="<%=TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin%>" 
       style="width:100%" type="image/svg+xml" pluginspage="http://www.adobe.com/svg/viewer/install/" />
<%} }%>

<hr>
<% if(JSPUtil.privateActionsAllowed(tracker.conf)) { %>
  <table border="0"> <tr> <td>
  Change priority from <%=job.getPriority()%> to:
  <form action="jobdetails.jsp" method="post">
  <input type="hidden" name="action" value="changeprio"/>
  <input type="hidden" name="jobid" value="<%=jobId%>"/>
  </td><td> <select name="prio"> 
  <%
    JobPriority jobPrio = job.getPriority();
    for (JobPriority prio : JobPriority.values()) {
      if(jobPrio != prio) {
        %> <option value=<%=prio%>><%=prio%></option> <%
      }
    }
  %>
  </select> </td><td><input type="submit" value="Submit"> </form></td></tr> </table>
<% } %>

<table border="0"> <tr>
    
<% if(JSPUtil.privateActionsAllowed(tracker.conf) 
    	&& runState == JobStatus.RUNNING) { %>
	<br/><a href="jobdetails.jsp?action=confirm&jobid=<%=jobId%>"> Kill this job </a>
<% } %>

<hr>

<hr>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
