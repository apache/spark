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

package org.apache.hadoop.mapreduce.server.jobtracker;

import java.io.IOException;
import java.util.List;
import java.text.DecimalFormat;

import javax.servlet.jsp.JspWriter;
import javax.servlet.http.*;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;

/**
 * Methods to help format output for JobTracker XML JSPX
 */
public class JobTrackerJspHelper {

  public JobTrackerJspHelper() {
    percentFormat = new DecimalFormat("##0.00");
  }

  private DecimalFormat percentFormat;

  /**
   * Returns an XML-formatted table of the jobs in the list.
   * This is called repeatedly for different lists of jobs (e.g., running, completed, failed).
   */
  public void generateJobTable(JspWriter out, String label, List<JobInProgress> jobs)
      throws IOException {
    if (jobs.size() > 0) {
      for (JobInProgress job : jobs) {
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String name = profile.getJobName();

        out.print("<" + label + "_job jobid=\"" + jobid + "\">\n");
        out.print("  <jobid>" + jobid + "</jobid>\n");
        out.print("  <user>" + profile.getUser() + "</user>\n");
        out.print("  <name>" + ("".equals(name) ? "&nbsp;" : name) + "</name>\n");
        out.print("  <map_complete>" + StringUtils.formatPercent(status.mapProgress(), 2) + "</map_complete>\n");
        out.print("  <map_total>" + desiredMaps + "</map_total>\n");
        out.print("  <maps_completed>" + completedMaps + "</maps_completed>\n");
        out.print("  <reduce_complete>" + StringUtils.formatPercent(status.reduceProgress(), 2) + "</reduce_complete>\n");
        out.print("  <reduce_total>" + desiredReduces + "</reduce_total>\n");
        out.print("  <reduces_completed>" + completedReduces + "</reduces_completed>\n");
        out.print("</" + label + "_job>\n");
      }
    }
  }

  /**
   * Generates an XML-formatted block that summarizes the state of the JobTracker.
   */
  public void generateSummaryTable(JspWriter out,
                                   JobTracker tracker) throws IOException {
    ClusterStatus status = tracker.getClusterStatus();
    int maxMapTasks = status.getMaxMapTasks();
    int maxReduceTasks = status.getMaxReduceTasks();
    int numTaskTrackers = status.getTaskTrackers();
    String tasksPerNodeStr;
    if (numTaskTrackers > 0) {
      double tasksPerNodePct = (double) (maxMapTasks + maxReduceTasks) / (double) numTaskTrackers;
      tasksPerNodeStr = percentFormat.format(tasksPerNodePct);
    } else {
      tasksPerNodeStr = "-";
    }
    out.print("<maps>" + status.getMapTasks() + "</maps>\n" +
            "<reduces>" + status.getReduceTasks() + "</reduces>\n" +
            "<total_submissions>" + tracker.getTotalSubmissions() + "</total_submissions>\n" +
            "<nodes>" + status.getTaskTrackers() + "</nodes>\n" +
            "<map_task_capacity>" + status.getMaxMapTasks() + "</map_task_capacity>\n" +
            "<reduce_task_capacity>" + status.getMaxReduceTasks() + "</reduce_task_capacity>\n" +
            "<avg_tasks_per_node>" + tasksPerNodeStr + "</avg_tasks_per_node>\n");
  }
}
