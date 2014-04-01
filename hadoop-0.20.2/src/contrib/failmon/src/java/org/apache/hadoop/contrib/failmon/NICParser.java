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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;

/**********************************************************
 * Objects of this class parse the output of ifconfig to 
 * gather information about present Network Interface Cards
 * in the system. The list of NICs to poll is specified in the 
 * configuration file.
 * 
 **********************************************************/


public class NICParser extends ShellParser {

  String[] nics;

  /**
   * Constructs a NICParser and reads the list of NICs to query
   */
  public NICParser() {
    super();
    nics = Environment.getProperty("nic.list").split(",\\s*");
  }

  /**
   * Reads and parses the output of ifconfig for a specified NIC and 
   * creates an appropriate EventRecord that holds the desirable 
   * information for it.
   * 
   * @param device the NIC device name to query
   * 
   * @return the EventRecord created
   */
  public EventRecord query(String device) throws UnknownHostException {
    StringBuffer sb = Environment.runCommand("/sbin/ifconfig " + device);
    EventRecord retval = new EventRecord(InetAddress.getLocalHost()
        .getCanonicalHostName(), InetAddress.getAllByName(InetAddress.getLocalHost()
        .getHostName()), Calendar.getInstance(), "NIC", "Unknown", device, "-");

    retval.set("hwAddress", findPattern("HWaddr\\s*([\\S{2}:]{17})", sb
        .toString(), 1));

    retval.set("ipAddress", findPattern("inet\\s+addr:\\s*([\\w.?]*)", sb
        .toString(), 1));

    String tmp = findPattern("inet\\s+addr:\\s*([\\w.?]*)", sb.toString(), 1);
    retval.set("status", (tmp == null) ? "DOWN" : "UP");
    if (tmp != null)
      retval.set("ipAddress", tmp);

    retval.set("rxPackets", findPattern("RX\\s*packets\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("rxErrors", findPattern("RX.+errors\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("rxDropped", findPattern("RX.+dropped\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("rxOverruns", findPattern("RX.+overruns\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("rxFrame", findPattern("RX.+frame\\s*:\\s*(\\d+)",
        sb.toString(), 1));

    retval.set("txPackets", findPattern("TX\\s*packets\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("txErrors", findPattern("TX.+errors\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("txDropped", findPattern("TX.+dropped\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("txOverruns", findPattern("TX.+overruns\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("txCarrier", findPattern("TX.+carrier\\s*:\\s*(\\d+)", sb
        .toString(), 1));

    retval.set("collisions", findPattern("\\s+collisions\\s*:\\s*(\\d+)", sb
        .toString(), 1));

    retval.set("rxBytes", findPattern("RX\\s*bytes\\s*:\\s*(\\d+)", sb
        .toString(), 1));
    retval.set("txBytes", findPattern("TX\\s*bytes\\s*:\\s*(\\d+)", sb
        .toString(), 1));

    return retval;
  }

  /**
   * Invokes query() to do the parsing and handles parsing errors for 
   * each one of the NICs specified in the configuration. 
   * 
   * @return an array of EventRecords that holds one element that represents
   * the current state of network interfaces.
   */
  public EventRecord[] monitor() {
    ArrayList<EventRecord> recs = new ArrayList<EventRecord>();

    for (String nic : nics) {
      try {
        recs.add(query(nic));
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }

    EventRecord[] T = new EventRecord[recs.size()];

    return recs.toArray(T);
  }
  
  /**
   * Return a String with information about this class
   * 
   * @return A String describing this class
   */
  public String getInfo() {
    String retval = "ifconfig parser for interfaces: ";
    for (String nic : nics)
      retval += nic + " ";
    return retval;
  }
}
