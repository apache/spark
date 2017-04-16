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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;

import junit.framework.TestCase;

public class TestNodeHealthService extends TestCase {

  private static volatile Log LOG = LogFactory
      .getLog(TestNodeHealthService.class);

  private static final String nodeHealthConfigPath = System.getProperty(
      "test.build.extraconf", "build/test/extraconf");

  final static File nodeHealthConfigFile = new File(nodeHealthConfigPath,
      "mapred-site.xml");

  private String testRootDir = new File(System.getProperty("test.build.data",
      "/tmp")).getAbsolutePath();

  private File nodeHealthscriptFile = new File(testRootDir, "failingscript.sh");

  @Override
  protected void tearDown() throws Exception {
    if (nodeHealthConfigFile.exists()) {
      nodeHealthConfigFile.delete();
    }
    if (nodeHealthscriptFile.exists()) {
      nodeHealthscriptFile.delete();
    }
    super.tearDown();
  }

  private Configuration getConfForNodeHealthScript() {
    Configuration conf = new Configuration();
    conf.set(NodeHealthCheckerService.HEALTH_CHECK_SCRIPT_PROPERTY,
        nodeHealthscriptFile.getAbsolutePath());
    conf.setLong(NodeHealthCheckerService.HEALTH_CHECK_INTERVAL_PROPERTY, 500);
    conf.setLong(
        NodeHealthCheckerService.HEALTH_CHECK_FAILURE_INTERVAL_PROPERTY, 1000);
    return conf;
  }

  private void writeNodeHealthScriptFile(String scriptStr, boolean setExecutable)
      throws IOException {
    PrintWriter pw = new PrintWriter(new FileOutputStream(nodeHealthscriptFile));
    pw.println(scriptStr);
    pw.flush();
    pw.close();
    nodeHealthscriptFile.setExecutable(setExecutable);
  }

  public void testNodeHealthScriptShouldRun() throws IOException {
    // Node health script should not start if there is no property called
    // node health script path.
    assertFalse("Health checker should not have started",
        NodeHealthCheckerService.shouldRun(new Configuration()));
    Configuration conf = getConfForNodeHealthScript();
    // Node health script should not start if the node health script does not
    // exists
    assertFalse("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
    // Create script path.
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    writeNodeHealthScriptFile("", false);
    // Node health script should not start if the node health script is not
    // executable.
    assertFalse("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
    writeNodeHealthScriptFile("", true);
    assertTrue("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
  }

  public void testNodeHealthScript() throws Exception {
    TaskTrackerHealthStatus healthStatus = new TaskTrackerHealthStatus();
    String errorScript = "echo ERROR\n echo \"Tracker not healthy\"";
    String normalScript = "echo \"I am all fine\"";
    String timeOutScript = "sleep 4\n echo\"I am fine\"";
    Configuration conf = getConfForNodeHealthScript();
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));

    NodeHealthCheckerService nodeHealthChecker = new NodeHealthCheckerService(
        conf);
    TimerTask timer = nodeHealthChecker.getTimer();
    writeNodeHealthScriptFile(normalScript, true);
    timer.run();

    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking initial healthy condition");
    // Check proper report conditions.
    assertTrue("Node health status reported unhealthy", healthStatus
        .isNodeHealthy());
    assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().isEmpty());

    // write out error file.
    // Healthy to unhealthy transition
    writeNodeHealthScriptFile(errorScript, true);
    // Run timer
    timer.run();
    // update health status
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking Healthy--->Unhealthy");
    assertFalse("Node health status reported healthy", healthStatus
        .isNodeHealthy());
    assertFalse("Node health status reported healthy", healthStatus
        .getHealthReport().isEmpty());
    
    // Check unhealthy to healthy transitions.
    writeNodeHealthScriptFile(normalScript, true);
    timer.run();
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking UnHealthy--->healthy");
    // Check proper report conditions.
    assertTrue("Node health status reported unhealthy", healthStatus
        .isNodeHealthy());
    assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().isEmpty());

    // Healthy to timeout transition.
    writeNodeHealthScriptFile(timeOutScript, true);
    timer.run();
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking Healthy--->timeout");
    assertFalse("Node health status reported healthy even after timeout",
        healthStatus.isNodeHealthy());
    assertEquals("Node time out message not propogated", healthStatus
        .getHealthReport(),
        NodeHealthCheckerService.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
  }

}
