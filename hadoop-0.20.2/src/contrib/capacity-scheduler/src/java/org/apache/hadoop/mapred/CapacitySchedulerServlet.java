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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at [job tracker
 * URL]/scheduler when the {@link FairScheduler} is in use.
 * 
 * The main features are viewing each job's task count and fair share, ability
 * to change job priorities and pools from the UI, and ability to switch the
 * scheduler to FIFO mode without restarting the JobTracker if this is required
 * for any reason.
 * 
 * There is also an "advanced" view for debugging that can be turned on by going
 * to [job tracker URL]/scheduler?advanced.
 */
public class CapacitySchedulerServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;

  private transient CapacityTaskScheduler scheduler;
  private transient  JobTracker jobTracker;

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (CapacityTaskScheduler) servletContext
        .getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // Print out the normal response
    response.setContentType("text/html");

    // Because the client may read arbitrarily slow, and we hold locks while
    // the servlet output, we want to write to our own buffer which we know
    // won't block.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(baos);
    String hostname = StringUtils.simpleHostname(jobTracker
        .getJobTrackerMachine());
    out.print("<html><head>");
    out.printf("<title>%s Job Scheduler Admininstration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" "
        + "href=\"/static/hadoop.css\">\n");
    out.print("<script type=\"text/javascript\" "
        + "src=\"/static/sorttable.js\"></script> \n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> "
        + "Job Scheduler Administration</h1>\n", hostname);
    showQueues(out);
    out.print("</body></html>\n");
    out.close();

    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print a view of pools to the given output writer.
   */

  private void showQueues(PrintWriter out) 
      throws IOException {
    synchronized(scheduler) {
      out.print("<h2>Queues</h2>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" " + 
                " cellspacing=\"2\" class=\"sortable\"> \n");
      out.print("<tr><th>Queue</th>" +
      		      "<th>Running Jobs</th>" + 
                "<th>Pending Jobs</th>" + 
      		      "<th>Capacity Percentage</th>" +
      		      "<th>Map Task Capacity</th>" +
      		      "<th>Map Task Used Capacity</th>" +
      		      "<th>Running Maps</th>" +
      		      "<th>Reduce Task Capacity</th>" + 
                "<th>Reduce Task Used Capacity</th>" +
                "<th>Running Reduces </tr>\n");
      for (CapacitySchedulerQueue queue : scheduler.getQueueInfoMap().values()) {
        String queueName = queue.getQueueName();
        out.print("<tr>\n");
        out.printf(
            "<td><a href=\"jobqueue_details.jsp?queueName=%s\">%s</a></td>\n",
            queueName, queueName);
        out.printf("<td>%s</td>\n", 
            (queue.getNumRunningJobs() + queue.getNumInitializingJobs()));
        out.printf("<td>%s</td>\n", queue.getNumWaitingJobs());
        out.printf("<td>%.1f%%</td>\n", queue.getCapacityPercent());
        int mapCapacity = queue.getCapacity(TaskType.MAP);
        int mapSlotsOccupied = queue.getNumSlotsOccupied(TaskType.MAP);
        int reduceSlotsOccupied = queue.getNumSlotsOccupied(TaskType.REDUCE);
        float occupiedSlotsAsPercent = 
            mapCapacity != 0 ? ((float) mapSlotsOccupied * 100 / mapCapacity)
            : 0;
        out.printf("<td>%s</td>\n", mapCapacity);
        out.printf("<td>%s (%.1f%% of Capacity)</td>\n", mapSlotsOccupied,
            occupiedSlotsAsPercent);
        out.printf("<td>%s</td>\n", queue.getNumRunningTasks(TaskType.MAP));
        int reduceCapacity = queue.getCapacity(TaskType.REDUCE);
        float redOccupiedSlotsAsPercent = 
          (reduceCapacity != 0 ? ((float)reduceSlotsOccupied*100 / mapCapacity)
            : 0);
        out.printf("<td>%s</td>\n", reduceCapacity);
        out.printf("<td>%s (%.1f%% of Capacity)</td>\n", reduceSlotsOccupied,
            redOccupiedSlotsAsPercent);
        out.printf("<td>%s</td>\n", queue.getNumRunningTasks(TaskType.REDUCE));
      }
      out.print("</table>\n");
    }
  }
}
