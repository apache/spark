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

<%!	private static SimpleDateFormat dateFormat 
                              = new SimpleDateFormat("d/MM HH:mm:ss") ; 
%>
<%!	private static final long serialVersionUID = 1L;
%>
<html>
<%
  String logFile = request.getParameter("logFile");
  if (logFile == null) {
    out.println("Missing job!!");
    return;
  }
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String jobid = JSPUtil.getJobID(new Path(encodedLogFileName).getName());
  String numTasks = request.getParameter("numTasks");
  int showTasks = 10 ; 
  if (numTasks != null) {
    showTasks = Integer.parseInt(numTasks);  
  }
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
  JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobTracker, fs, new Path(logFile));
  if (job == null) {
    return;
  }%>
<head>
  <title>Analyze Job - Hadoop Job <%=jobid %></title>
  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  <link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>

<body>
<h2>Hadoop Job <a href="jobdetailshistory.jsp?logFile=<%=encodedLogFileName%>"><%=jobid %> </a></h2>
<b>User : </b> <%=HtmlQuoting.quoteHtmlChars(job.get(Keys.USER)) %><br/> 
<b>JobName : </b> <%=HtmlQuoting.quoteHtmlChars(job.get(Keys.JOBNAME)) %><br/> 
<b>JobConf : </b> <%=job.get(Keys.JOBCONF) %><br/> 
<b>Submitted At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 ) %><br/> 
<b>Launched At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) %><br/>
<b>Finished At : </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) %><br/>
<b>Status : </b> <%= ((job.get(Keys.JOB_STATUS) == null)?"Incomplete" :job.get(Keys.JOB_STATUS)) %><br/> 
<hr/>
<center>
<%
  if (!Values.SUCCESS.name().equals(job.get(Keys.JOB_STATUS))) {
    out.print("<h3>No Analysis available as job did not finish</h3>");
    return;
  }
  Map<String, JobHistory.Task> tasks = job.getAllTasks();
  int finishedMaps = job.getInt(Keys.FINISHED_MAPS)  ;
  int finishedReduces = job.getInt(Keys.FINISHED_REDUCES) ;
  JobHistory.Task [] mapTasks = new JobHistory.Task[finishedMaps]; 
  JobHistory.Task [] reduceTasks = new JobHistory.Task[finishedReduces]; 
  int mapIndex = 0 , reduceIndex=0; 
  long avgMapTime = 0;
  long avgReduceTime = 0;
  long avgShuffleTime = 0;

  for (JobHistory.Task task : tasks.values()) {
    Map<String, TaskAttempt> attempts = task.getTaskAttempts();
    for (JobHistory.TaskAttempt attempt : attempts.values()) {
      if (attempt.get(Keys.TASK_STATUS).equals(Values.SUCCESS.name())) {
        long avgFinishTime = (attempt.getLong(Keys.FINISH_TIME) -
      		                attempt.getLong(Keys.START_TIME));
        if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))) {
          mapTasks[mapIndex++] = attempt ; 
          avgMapTime += avgFinishTime;
        } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) { 
          reduceTasks[reduceIndex++] = attempt;
          avgShuffleTime += (attempt.getLong(Keys.SHUFFLE_FINISHED) - 
                             attempt.getLong(Keys.START_TIME));
          avgReduceTime += (attempt.getLong(Keys.FINISH_TIME) -
                            attempt.getLong(Keys.SHUFFLE_FINISHED));
        }
        break;
      }
    }
  }
	 
  if (finishedMaps > 0) {
    avgMapTime /= finishedMaps;
  }
  if (finishedReduces > 0) {
    avgReduceTime /= finishedReduces;
    avgShuffleTime /= finishedReduces;
  }
  Comparator<JobHistory.Task> cMap = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - t2.getLong(Keys.START_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  Comparator<JobHistory.Task> cShuffle = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED) - 
                t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED) - 
                t2.getLong(Keys.START_TIME); 
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  };
  Comparator<JobHistory.Task> cFinishMapRed = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  };

  if (mapTasks.length > 0) {
    Arrays.sort(mapTasks, cMap);
    JobHistory.Task minMap = mapTasks[mapTasks.length-1] ;
%>

<h3>Time taken by best performing Map task 
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=minMap.get(Keys.TASKID)%>">
<%=minMap.get(Keys.TASKID) %></a> : <%=StringUtils.formatTimeDiff(minMap.getLong(Keys.FINISH_TIME), minMap.getLong(Keys.START_TIME) ) %></h3>
<h3>Average time taken by Map tasks: 
<%=StringUtils.formatTimeDiff(avgMapTime, 0) %></h3>
<h3>Worse performing map tasks</h3>
<table class="jobtasks datatable">
<thead>
<tr><th>Task Id</th><th>Time taken</th></tr>
</thead>
<tbody>
<%
  for (int i=0;i<showTasks && i<mapTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=mapTasks[i].get(Keys.TASKID)%>">
        <%=mapTasks[i].get(Keys.TASKID) %></a></td>
    <td><%=StringUtils.formatTimeDiff(mapTasks[i].getLong(Keys.FINISH_TIME), mapTasks[i].getLong(Keys.START_TIME)) %></td>
    </tr>
<%
  }
%>
</tbody>
</table>
<%  

    Arrays.sort(mapTasks, cFinishMapRed);
    JobHistory.Task lastMap = mapTasks[0] ;
%>

<h3>The last Map task 
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>
&tipid=<%=lastMap.get(Keys.TASKID)%>"><%=lastMap.get(Keys.TASKID) %></a> 
finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat, 
                              lastMap.getLong(Keys.FINISH_TIME), 
                              job.getLong(Keys.LAUNCH_TIME) ) %></h3>
<hr/>

<%
  }//end if(mapTasks.length > 0)

  if (reduceTasks.length <= 0) return;
  Arrays.sort(reduceTasks, cShuffle); 
  JobHistory.Task minShuffle = reduceTasks[reduceTasks.length-1] ;
%>
<h3>Time taken by best performing shufflejobId
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>
&tipid=<%=minShuffle.get(Keys.TASKID)%>"><%=minShuffle.get(Keys.TASKID)%></a> : 
<%=StringUtils.formatTimeDiff(minShuffle.getLong(Keys.SHUFFLE_FINISHED), 
                              minShuffle.getLong(Keys.START_TIME) ) %></h3>
<h3>Average time taken by Shuffle: 
<%=StringUtils.formatTimeDiff(avgShuffleTime, 0) %></h3>
<h3>Worse performing Shuffle(s)</h3>
<table class="jobtasks datatable">
<thead>
<tr><th>Task Id</th><th>Time taken</th></tr>
</thead><tbody>
<%
  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=
<%=encodedLogFileName%>&tipid=<%=reduceTasks[i].get(Keys.TASKID)%>">
<%=reduceTasks[i].get(Keys.TASKID) %></a></td>
    <td><%=
           StringUtils.formatTimeDiff(
                       reduceTasks[i].getLong(Keys.SHUFFLE_FINISHED),
                       reduceTasks[i].getLong(Keys.START_TIME)) %>
    </td>
    </tr>
<%
  }
%>
</tbody>
</table>
<%  
  Comparator<JobHistory.Task> cFinishShuffle = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED); 
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  };
  Arrays.sort(reduceTasks, cFinishShuffle);
  JobHistory.Task lastShuffle = reduceTasks[0] ;
%>

<h3>The last Shuffle  
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>
&tipid=<%=lastShuffle.get(Keys.TASKID)%>"><%=lastShuffle.get(Keys.TASKID)%>
</a> finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastShuffle.getLong(Keys.SHUFFLE_FINISHED), 
                              job.getLong(Keys.LAUNCH_TIME) ) %></h3>

<%
  Comparator<JobHistory.Task> cReduce = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - 
                t1.getLong(Keys.SHUFFLE_FINISHED); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - 
                t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  Arrays.sort(reduceTasks, cReduce); 
  JobHistory.Task minReduce = reduceTasks[reduceTasks.length-1] ;
%>
<hr/>
<h3>Time taken by best performing Reduce task : 
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=minReduce.get(Keys.TASKID)%>">
<%=minReduce.get(Keys.TASKID) %></a> : 
<%=StringUtils.formatTimeDiff(minReduce.getLong(Keys.FINISH_TIME),
    minReduce.getLong(Keys.SHUFFLE_FINISHED) ) %></h3>

<h3>Average time taken by Reduce tasks: 
<%=StringUtils.formatTimeDiff(avgReduceTime, 0) %></h3>
<h3>Worse performing reduce tasks</h3>
<table class="jobtasks datatable">
<thead>
<tr><th>Task Id</th><th>Time taken</th></tr>
</thead>
<tbody>
<%
  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>&tipid=<%=reduceTasks[i].get(Keys.TASKID)%>">
        <%=reduceTasks[i].get(Keys.TASKID) %></a></td>
    <td><%=StringUtils.formatTimeDiff(
             reduceTasks[i].getLong(Keys.FINISH_TIME), 
             reduceTasks[i].getLong(Keys.SHUFFLE_FINISHED)) %></td>
    </tr>
<%
  }
%>
</tbody>
</table>
<%  
  Arrays.sort(reduceTasks, cFinishMapRed);
  JobHistory.Task lastReduce = reduceTasks[0] ;
%>

<h3>The last Reduce task 
<a href="taskdetailshistory.jsp?logFile=<%=encodedLogFileName%>
&tipid=<%=lastReduce.get(Keys.TASKID)%>"><%=lastReduce.get(Keys.TASKID)%>
</a> finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastReduce.getLong(Keys.FINISH_TIME), 
                              job.getLong(Keys.LAUNCH_TIME) ) %></h3>
</center>
</body></html>
