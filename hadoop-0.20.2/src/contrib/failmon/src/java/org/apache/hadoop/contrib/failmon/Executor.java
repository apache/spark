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

package org.apache.hadoop.contrib.failmon;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

/**********************************************************
 * This class executes monitoring jobs on all nodes of the
 * cluster, on which we intend to gather failure metrics. 
 * It is basically a thread that sleeps and periodically wakes
 * up to execute monitoring jobs and ship all gathered data to 
 * a "safe" location, which in most cases will be the HDFS 
 * filesystem of the monitored cluster.
 * 
 **********************************************************/

public class Executor implements Runnable {

  public static final int DEFAULT_LOG_INTERVAL = 3600;

  public static final int DEFAULT_POLL_INTERVAL = 360;

  public static int MIN_INTERVAL = 5;

  public static int instances = 0;

  LocalStore lstore;

  ArrayList<MonitorJob> monitors;
  
  int interval;

  int upload_interval;
  int upload_counter;
  
  /**
   * Create an instance of the class and read the configuration
   * file to determine the set of jobs that will be run and the 
   * maximum interval for which the thread can sleep before it 
   * wakes up to execute a monitoring job on the node.
   * 
   */ 

  public Executor(Configuration conf) {
    
    Environment.prepare("conf/failmon.properties");
    
    String localTmpDir;
    
    if (conf == null) {
      // running as a stand-alone application
      localTmpDir = System.getProperty("java.io.tmpdir");
      Environment.setProperty("local.tmp.dir", localTmpDir);
    } else {
      // running from within Hadoop
      localTmpDir = conf.get("hadoop.tmp.dir");
      String hadoopLogPath = System.getProperty("hadoop.log.dir") + "/" + System.getProperty("hadoop.log.file");
      Environment.setProperty("hadoop.log.file", hadoopLogPath);
      Environment.setProperty("local.tmp.dir", localTmpDir);
    }
    
    monitors = Environment.getJobs();
    interval = Environment.getInterval(monitors);
    upload_interval = LocalStore.UPLOAD_INTERVAL;
    lstore = new LocalStore();
    
    if (Environment.getProperty("local.upload.interval") != null) 
     upload_interval = Integer.parseInt(Environment.getProperty("local.upload.interval"));

    instances++;
  }

  public void run() {
    upload_counter = upload_interval;

    Environment.logInfo("Failmon Executor thread started successfully.");
    while (true) {
      try {
        Thread.sleep(interval * 1000);
        for (int i = 0; i < monitors.size(); i++) {
          monitors.get(i).counter -= interval;
          if (monitors.get(i).counter <= 0) {
            monitors.get(i).reset();
            Environment.logInfo("Calling " + monitors.get(i).job.getInfo() + "...\t");
            monitors.get(i).job.monitor(lstore);
          }
        }
        upload_counter -= interval;
        if (upload_counter <= 0) {
          lstore.upload();
          upload_counter = upload_interval;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void cleanup() {
    instances--;   
  }
}
