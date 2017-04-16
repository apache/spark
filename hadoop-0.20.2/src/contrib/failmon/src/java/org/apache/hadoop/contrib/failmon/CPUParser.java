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

import java.net.InetAddress;
import java.util.Calendar;

/**********************************************************
 * Objects of this class parse the /proc/cpuinfo file to 
 * gather information about present processors in the system.
 *
 **********************************************************/


public class CPUParser extends ShellParser {

 /**
  * Constructs a CPUParser
  */
  public CPUParser() {
    super();
  }

  /**
   * Reads and parses /proc/cpuinfo and creates an appropriate 
   * EventRecord that holds the desirable information.
   * 
   * @param s unused parameter
   * 
   * @return the EventRecord created
   */
  public EventRecord query(String s) throws Exception {
    StringBuffer sb = Environment.runCommand("cat /proc/cpuinfo");
    EventRecord retval = new EventRecord(InetAddress.getLocalHost()
        .getCanonicalHostName(), InetAddress.getAllByName(InetAddress.getLocalHost()
        .getHostName()), Calendar.getInstance(), "CPU", "Unknown", "CPU", "-");

    retval.set("processors", findAll("\\s*processor\\s*:\\s*(\\d+)", sb
        .toString(), 1, ", "));

    retval.set("model name", findPattern("\\s*model name\\s*:\\s*(.+)", sb
        .toString(), 1));

    retval.set("frequency", findAll("\\s*cpu\\s*MHz\\s*:\\s*(\\d+)", sb
        .toString(), 1, ", "));

    retval.set("physical id", findAll("\\s*physical\\s*id\\s*:\\s*(\\d+)", sb
        .toString(), 1, ", "));

    retval.set("core id", findAll("\\s*core\\s*id\\s*:\\s*(\\d+)", sb
        .toString(), 1, ", "));

    return retval;
  }

  /**
   * Invokes query() to do the parsing and handles parsing errors. 
   * 
   * @return an array of EventRecords that holds one element that represents
   * the current state of /proc/cpuinfo
   */
  
  public EventRecord[] monitor() {

    EventRecord[] recs = new EventRecord[1];

    try {
      recs[0] = query(null);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return recs;
  }
  
  /**
   * Return a String with information about this class
   * 
   * @return A String describing this class
   */
  public String getInfo() {
    return ("CPU Info parser");
  }

}
