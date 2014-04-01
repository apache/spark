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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

/**
 * System test methods for the fair scheduler.
 */
public abstract class FairSchedulerSystemTestBase {

  protected void runSleepJob(JobConf conf, int sleepTimeMillis) throws Exception {
    String[] args = { "-m", "1", "-r", "1", "-mt", "" + sleepTimeMillis,
        "-rt", "" + sleepTimeMillis };
    assertEquals(0, ToolRunner.run(conf, new SleepJob(), args));
  }
  
  /**
   * Check the fair scheduler servlet for good status code and smoke test
   * for contents.
   */
  protected void checkServlet(boolean advanced, boolean poolNameIsGroupName,
      int jobTrackerInfoPort) throws Exception {
    String jtURL = "http://localhost:" + jobTrackerInfoPort;
    URL url = new URL(jtURL + "/scheduler" +
                      (advanced ? "?advanced" : ""));
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    assertEquals(200, connection.getResponseCode());

    // Just to be sure, slurp the content and make sure it looks like the scheduler
    String contents = slurpContents(connection);
    assertTrue("Bad contents for fair scheduler servlet: " + contents,
      contents.contains("Fair Scheduler Administration"));

    if (poolNameIsGroupName) {
      String userGroups[] = UserGroupInformation.getCurrentUser().getGroupNames();
      String primaryGroup = ">" + userGroups[0] + "<";
      assertTrue("Pool name not group name, expected " + userGroups[0] +
          " but contents was:\n" + contents,
          contents.contains(primaryGroup));
    }
  }

  protected void checkTaskGraphServlet(JobID job, int jobTrackerInfoPort)
      throws Exception {
    String jtURL = "http://localhost:" + jobTrackerInfoPort;
    URL url = new URL(jtURL + "/taskgraph?jobid=" + job.toString() + "&type=map");
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    assertEquals(200, connection.getResponseCode());

    // Just to be sure, slurp the content and make sure it looks like a graph
    String contents = slurpContents(connection);
    if (contents.trim().length() > 0) {
      assertTrue("Bad contents for job " + job + ":\n" + contents,
          contents.contains("</svg>"));
    }
  }

  protected String slurpContents(HttpURLConnection connection) throws Exception {
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(connection.getInputStream()));
    StringBuilder sb = new StringBuilder();

    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line).append('\n');
    }

    return sb.toString();
  }

}
