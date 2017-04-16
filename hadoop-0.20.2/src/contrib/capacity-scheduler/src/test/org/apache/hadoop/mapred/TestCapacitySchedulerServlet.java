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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.net.URL;

public class TestCapacitySchedulerServlet extends
    ClusterWithCapacityScheduler {

  /**
   * Test case checks CapacitySchedulerServlet. Check if queues are 
   * initialized {@link CapacityTaskScheduler} 
   * 
   * @throws IOException
   */
  public void testCapacitySchedulerServlet() throws IOException {
    Properties schedulerProps = new Properties();
    String[] queues = new String[] { "Q1", "Q2" };
    for (String q : queues) {
      schedulerProps.put(CapacitySchedulerConf
          .toFullPropertyName(q, "capacity"), "50");
      schedulerProps.put(CapacitySchedulerConf.toFullPropertyName(q,
          "minimum-user-limit-percent"), "100");
    }
    Properties clusterProps = new Properties();
    clusterProps.put("mapred.tasktracker.map.tasks.maximum", String.valueOf(2));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(2));
    clusterProps.put("mapred.queue.names", queues[0] + "," + queues[1]);
    startCluster(2, clusterProps, schedulerProps);

    JobTracker jt = getJobTracker();
    int port = jt.getInfoPort();
    String host = jt.getJobTrackerMachine();
    URL url = new URL("http://" + host + ":" + port + "/scheduler");
    String queueData = readOutput(url);
    assertTrue(queueData.contains("Q1"));
    assertTrue(queueData.contains("Q2"));
    assertTrue(queueData.contains("50.0%"));
  }

  private String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }
}
