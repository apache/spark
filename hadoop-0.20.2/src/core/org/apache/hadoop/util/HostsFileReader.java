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

package org.apache.hadoop.util;

import java.io.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
public class HostsFileReader {
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;
  
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  private void readFileToSet(String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    if (!file.exists()) {
      return;
    }
    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            if (!nodes[i].equals("")) {
              set.add(nodes[i]);  // might need to add canonical name
            }
          }
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public synchronized void refresh() throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    if (!includesFile.equals("")) {
      Set<String> newIncludes = new HashSet<String>();
      readFileToSet(includesFile, newIncludes);
      // switch the new hosts that are to be included
      includes = newIncludes;
    }
    if (!excludesFile.equals("")) {
      Set<String> newExcludes = new HashSet<String>();
      readFileToSet(excludesFile, newExcludes);
      // switch the excluded hosts
      excludes = newExcludes;
    }
  }

  public synchronized Set<String> getHosts() {
    return includes;
  }

  public synchronized Set<String> getExcludedHosts() {
    return excludes;
  }

  public synchronized void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public synchronized void updateFileNames(String includesFile, 
                                           String excludesFile) 
                                           throws IOException {
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
}
