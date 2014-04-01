/** Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

public class TestCapacitySchedulerConf extends TestCase {

  private static String testDataDir = System.getProperty("test.build.data");
  private static String testConfFile;
  
  private Map<String, String> defaultProperties;
  private CapacitySchedulerConf testConf;
  private PrintWriter writer;
  
  static {
    if (testDataDir == null) {
      testDataDir = ".";
    } else {
      new File(testDataDir).mkdirs();
    }
    testConfFile = new File(testDataDir, "test-conf.xml").getAbsolutePath();
  }
  
  public TestCapacitySchedulerConf() {
    defaultProperties = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent",
                       "maximum-initialized-jobs-per-user"}, 
        new String[] { "100", 
                        "false", 
                        "100",
                        "2" }
                      );
  }

  
  public void setUp() throws IOException {
    openFile();
  }
  
  public void tearDown() throws IOException {
    File confFile = new File(testConfFile);
    if (confFile.exists()) {
      confFile.delete();  
    }
  }
  
  public void testDefaults() {
    testConf = new CapacitySchedulerConf();
    Map<String, Map<String, String>> queueDetails
                            = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", defaultProperties);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueues() {

    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent",
                       "maximum-initialized-jobs-per-user"}, 
        new String[] { "10", 
                        "true",
                        "25",
                        "4"}
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent",
                       "maximum-initialized-jobs-per-user"}, 
        new String[] { "100", 
                        "false", 
                        "50",
                        "1"}
                      );

    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("research", q2Props);
    endConfig();

    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("research", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueueWithDefaultProperties() {
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity", 
                       "minimum-user-limit-percent" }, 
        new String[] { "20", 
                        "75" }
                      );
    startConfig();
    writeQueueDetails("default", q1Props);
    endConfig();

    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    Map<String, String> expProperties = new HashMap<String, String>();
    for (String key : q1Props.keySet()) {
      expProperties.put(key, q1Props.get(key));
    }
    expProperties.put("supports-priority", "false");
    expProperties.put("maximum-initialized-jobs-per-user", "2");
    queueDetails.put("default", expProperties);
    checkQueueProperties(testConf, queueDetails);
  }

  public void testReload() throws IOException {
    // use the setup in the test case testQueues as a base...
    testQueues();
    
    // write new values to the file...
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "20.5", 
                        "true", 
                        "40" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "100", 
                        "false",
                        "50" }
                      );

    openFile();
    startConfig();
    writeDefaultConfiguration();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("production", q2Props);
    endConfig();
    testConf.reloadConfiguration();
    Map<String, Map<String, String>> queueDetails 
                      = new HashMap<String, Map<String, String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("production", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }

  public void testQueueWithUserDefinedDefaultProperties() throws IOException {
    openFile();
    startConfig();
    writeUserDefinedDefaultConfiguration();
    endConfig();

    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "-1", 
                        "true", 
                        "50" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "capacity",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "-1", 
                        "true",
                        "50" }
                      );
    
    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    
    queueDetails.put("default", q1Props);
    queueDetails.put("production", q2Props);
    
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueueWithDefaultPropertiesOverriden() throws IOException {
    openFile();
    startConfig();
    writeUserDefinedDefaultConfiguration();
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "-1", 
                        "true", 
                        "50" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "40", 
                        "true",
                        "50" }
                      );
    Map<String, String> q3Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "40", 
                        "true",
                        "50" }
                      );
    writeQueueDetails("production", q2Props);
    writeQueueDetails("test", q3Props);
    endConfig();
    testConf = new CapacitySchedulerConf(new Path(testConfFile));
    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("production", q2Props);
    queueDetails.put("test", q3Props);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testInvalidUserLimit() throws IOException {
    openFile();
    startConfig();
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "capacity", 
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "-1",
                        "true", 
                        "-50" }
                      );
    writeQueueDetails("default", q1Props);
    endConfig();
    try {
      testConf = new CapacitySchedulerConf(new Path(testConfFile));
      testConf.getMinimumUserLimitPercent("default");
      fail("Expect Invalid user limit to raise Exception");
    }catch(IllegalArgumentException e) {
      assertTrue(true);
    }
  }
  
  public void testInvalidMaxCapacity() throws IOException {
    openFile();
    startConfig();
    writeProperty(
      "mapred.capacity-scheduler.queue.default.capacity", "70");
    writeProperty(
      "mapred.capacity-scheduler.queue.default.maximum-capacity", "50");
    endConfig();
    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    try {
      testConf.getMaxCapacity("default");
      fail(" getMaxCapacity worked " + testConf.getCapacity("default"));
    } catch (IllegalArgumentException e) {
      assertEquals(
        CapacitySchedulerConf.MAX_CAPACITY_PROPERTY + " 50.0"+
          " for a queue should be greater than or equal to capacity ", e.getMessage());
    }
  }
  
  public void testInitializationPollerProperties() 
    throws Exception {
    /*
     * Test case to check properties of poller when no configuration file
     * is present.
     */
    testConf = new CapacitySchedulerConf();
    long pollingInterval = testConf.getSleepInterval();
    int maxWorker = testConf.getMaxWorkerThreads();
    assertTrue("Invalid polling interval ",pollingInterval > 0);
    assertTrue("Invalid working thread pool size" , maxWorker > 0);
    
    //test case for custom values configured for initialization 
    //poller.
    openFile();
    startConfig();
    writeProperty("mapred.capacity-scheduler.init-worker-threads", "1");
    writeProperty("mapred.capacity-scheduler.init-poll-interval", "1");
    endConfig();
    
    testConf = new CapacitySchedulerConf(new Path(testConfFile));
    
    pollingInterval = testConf.getSleepInterval();
    
    maxWorker = testConf.getMaxWorkerThreads();
    
    assertEquals("Invalid polling interval ",pollingInterval ,1);
    assertEquals("Invalid working thread pool size" , maxWorker, 1);
    
    //Test case for invalid values configured for initialization
    //poller
    openFile();
    startConfig();
    writeProperty("mapred.capacity-scheduler.init-worker-threads", "0");
    writeProperty("mapred.capacity-scheduler.init-poll-interval", "0");
    endConfig();
    
    testConf = new CapacitySchedulerConf(new Path(testConfFile));
    
    try {
      pollingInterval = testConf.getSleepInterval();
      fail("Polling interval configured is illegal");
    } catch (IllegalArgumentException e) {}
    try {
      maxWorker = testConf.getMaxWorkerThreads();
      fail("Max worker thread configured is illegal");
    } catch (IllegalArgumentException e) {}
  }
  

  private void checkQueueProperties(
                        CapacitySchedulerConf testConf,
                        Map<String, Map<String, String>> queueDetails) {
    for (String queueName : queueDetails.keySet()) {
      Map<String, String> map = queueDetails.get(queueName);
      assertEquals(Float.parseFloat(map.get("capacity")),
           testConf.getCapacity(queueName));
      assertEquals(Integer.parseInt(map.get("minimum-user-limit-percent")),
          testConf.getMinimumUserLimitPercent(queueName));
      assertEquals(Boolean.parseBoolean(map.get("supports-priority")),
          testConf.isPrioritySupported(queueName));
    }
  }
  
  private Map<String, String> setupQueueProperties(String[] keys, 
                                                String[] values) {
    HashMap<String, String> map = new HashMap<String, String>();
    for(int i=0; i<keys.length; i++) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  private void openFile() throws IOException {
    
    if (testDataDir != null) {
      File f = new File(testDataDir);
      f.mkdirs();
    }
    FileWriter fw = new FileWriter(testConfFile);
    BufferedWriter bw = new BufferedWriter(fw);
    writer = new PrintWriter(bw);
  }
  
  private void startConfig() {
    writer.println("<?xml version=\"1.0\"?>");
    writer.println("<configuration>");
  }
  
  private void writeQueueDetails(String queue, Map<String, String> props) {
    for (String key : props.keySet()) {
      writer.println("<property>");
      writer.println("<name>mapred.capacity-scheduler.queue." 
                        + queue + "." + key +
                    "</name>");
      writer.println("<value>"+props.get(key)+"</value>");
      writer.println("</property>");
    }
  }
  
  
  private void writeDefaultConfiguration() {
    writeProperty("mapred.capacity-scheduler.default-supports-priority"
        , "false");
    writeProperty("mapred.capacity-scheduler.default-minimum-user-limit-percent"
        , "100");
  }


  private void writeUserDefinedDefaultConfiguration() {
    writeProperty("mapred.capacity-scheduler.default-supports-priority"
        , "true");
    writeProperty("mapred.capacity-scheduler.default-minimum-user-limit-percent"
        , "50");
  }


  private void writeProperty(String name, String value) {
    writer.println("<property>");
    writer.println("<name> " + name + "</name>");
    writer.println("<value>"+ value+"</value>");
    writer.println("</property>");
    
  }
  
  private void endConfig() {
    writer.println("</configuration>");
    writer.close();
  }
  
}
