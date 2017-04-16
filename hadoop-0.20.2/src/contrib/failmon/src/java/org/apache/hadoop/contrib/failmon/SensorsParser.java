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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**********************************************************
 * Objects of this class parse the output of the lm-sensors utility 
 * to gather information about fan speed, temperatures for cpus
 * and motherboard etc.
 *
 **********************************************************/

public class SensorsParser extends ShellParser {

  /**
   * Reads and parses the output of the 'sensors' command 
   * and creates an appropriate EventRecord that holds 
   * the desirable information.
   * 
   * @param s unused parameter
   * 
   * @return the EventRecord created
   */
  public EventRecord query(String s) throws Exception {
    StringBuffer sb;

    //sb = Environment.runCommand("sensors -A");
     sb = Environment.runCommand("cat sensors.out");

    EventRecord retval = new EventRecord(InetAddress.getLocalHost()
        .getCanonicalHostName(), InetAddress.getAllByName(InetAddress.getLocalHost()
        .getHostName()), Calendar.getInstance(), "lm-sensors", "Unknown",
        "sensors -A", "-");
    readGroup(retval, sb, "fan");
    readGroup(retval, sb, "in");
    readGroup(retval, sb, "temp");
    readGroup(retval, sb, "Core");

    return retval;
  }

  /**
   * Reads and parses lines that provide the output
   * of a group of sensors with the same functionality.
   * 
   * @param er the EventRecord to which the new attributes are added
   * @param sb the text to parse
   * @param prefix a String prefix specifying the common prefix of the
   * sensors' names in the group (e.g. "fan", "in", "temp"
   * 
   * @return the EventRecord created
   */
  private EventRecord readGroup(EventRecord er, StringBuffer sb, String prefix) {

    Pattern pattern = Pattern.compile(".*(" + prefix
        + "\\s*\\d*)\\s*:\\s*(\\+?\\d+)", Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(sb);

    while (matcher.find())
      er.set(matcher.group(1), matcher.group(2));

    return er;
  }

  /**
   * Invokes query() to do the parsing and handles parsing errors. 
   * 
   * @return an array of EventRecords that holds one element that represents
   * the current state of the hardware sensors
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
    return ("lm-sensors parser");
  }

}
