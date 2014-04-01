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

/**********************************************************
* Runs a set of monitoring jobs once for the local node. The set of
* jobs to be run is the intersection of the jobs specifed in the
* configuration file and the set of jobs specified in the --only
* command line argument.
 **********************************************************/ 

public class RunOnce {

  LocalStore lstore;

  ArrayList<MonitorJob> monitors;
  
  boolean uploading = true;
  
  public RunOnce(String confFile) {
    
    Environment.prepare(confFile);
    
    String localTmpDir;
    
    // running as a stand-alone application
    localTmpDir = System.getProperty("java.io.tmpdir");
    Environment.setProperty("local.tmp.dir", localTmpDir);
        
    monitors = Environment.getJobs();
    lstore = new LocalStore();
    uploading  = true;
  }

  private void filter (String [] ftypes) {
    ArrayList<MonitorJob> filtered = new ArrayList<MonitorJob>();
    boolean found;
    
    // filter out unwanted monitor jobs
    for (MonitorJob job : monitors) {
      found = false;
      for (String ftype : ftypes)
	if (job.type.equalsIgnoreCase(ftype))
	    found = true;
      if (found)
	filtered.add(job);
    }

    // disable uploading if not requested
    found = false;
    for (String ftype : ftypes)
      if (ftype.equalsIgnoreCase("upload"))
	found = true;

    if (!found)
      uploading = false;
    
    monitors = filtered;
  }
  
  private void run() {
    
    Environment.logInfo("Failmon started successfully.");

    for (int i = 0; i < monitors.size(); i++) {
      Environment.logInfo("Calling " + monitors.get(i).job.getInfo() + "...\t");
      monitors.get(i).job.monitor(lstore);
    }

    if (uploading)
      lstore.upload();

    lstore.close();
  }

  public void cleanup() {
    // nothing to be done
  }

  
  public static void main (String [] args) {

    String configFilePath = "./conf/failmon.properties";
    String [] onlyList = null;
    
    // Parse command-line parameters
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equalsIgnoreCase("--config"))
	configFilePath = args[i + 1];
      else if (args[i].equalsIgnoreCase("--only"))
	onlyList = args[i + 1].split(",");
    }

    RunOnce ro = new RunOnce(configFilePath);
    // only keep the requested types of jobs
    if (onlyList != null)
      ro.filter(onlyList);
    // run once only
    ro.run();
  }

}
