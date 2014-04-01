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
  import="java.util.Vector"
  import="java.util.Collection"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.util.ServletUtil"
%>
<%!
private static final long serialVersionUID = 526456771152222127L; 
%>
<%
  JobTracker tracker = 
    (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
    StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String queueName = request.getParameter("queueName");
  TaskScheduler scheduler = tracker.getTaskScheduler();
  Collection<JobInProgress> jobs = scheduler.getJobs(queueName);
  JobQueueInfo schedInfo = tracker.getQueueInfo(queueName);
%>
<html>
<head>
<title>Queue details for
<%=queueName!=null?queueName:""%> </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>
<% JSPUtil.processButtons(request, response, tracker); %>
<%
  String schedulingInfoString = schedInfo.getSchedulingInfo();
%>
<h1>Hadoop Job Queue Scheduling Information on 
  <a href="jobtracker.jsp"><%=trackerName%></a>
</h1>
<div>
State : <%= schedInfo.getQueueState() %> <br/>
Scheduling Information :
<%= HtmlQuoting.quoteHtmlChars(schedulingInfoString).replaceAll("\n","<br/>") %>
</div>
<hr/>
<%
if(jobs == null || jobs.isEmpty()) {
%>
<center>
<h2> No Jobs found for the Queue ::
<%=queueName!=null?queueName:""%> </h2>
<hr/>
</center>
<%
}else {
%>
<center>
<h2> Job Summary for the Queue ::
<%=queueName!=null?queueName:"" %> </h2>
</center>
<div style="text-align: center;text-indent: center;font-style: italic;">
(In the order maintained by the scheduler)
</div>
<br/>
<hr/>
<%=
  JSPUtil.generateJobTable("Job List", jobs, 30, 0, tracker.conf)
%>
<hr>
<% } %>

<%
out.println(ServletUtil.htmlFooter());
%>

