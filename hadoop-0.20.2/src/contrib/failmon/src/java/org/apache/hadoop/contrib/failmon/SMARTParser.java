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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**********************************************************
 * Objects of this class parse the output of smartmontools to 
 * gather information about the state of disks in the system. The
 * smartmontools utility reads the S.M.A.R.T. attributes from
 * the disk devices and reports them to the user. Note that since
 * running smartctl requires superuser provileges, one should  
 * grand sudo privileges to the running user for the command smartctl
 * (without a password). Alternatively, one can set up a cron  job that 
 * periodically dumps the output of smartctl into a user-readable file.
 * See the configuration file for details.
 *
 **********************************************************/

public class SMARTParser extends ShellParser {

  String[] devices;

  /**
   * Constructs a SMARTParser and reads the list of disk 
   * devices to query
   */
  public SMARTParser() {
    super();
    String devicesStr = Environment.getProperty("disks.list");
    System.out.println("skato " + devicesStr);
    if (devicesStr != null)
      devices = devicesStr.split(",\\s*");
  }

  /**
   * Reads and parses the output of smartctl for a specified disk and 
   * creates an appropriate EventRecord that holds the desirable 
   * information for it. Since the output of smartctl is different for 
   * different kinds of disks, we try to identify as many attributes as 
   * posssible for all known output formats. 
   * 
   * @param device the disk device name to query
   * 
   * @return the EventRecord created
   */
  public EventRecord query(String device) throws Exception {
    String conf = Environment.getProperty("disks." + device + ".source");
    StringBuffer sb;

    if (conf == null)
      sb = Environment.runCommand("sudo smartctl --all " + device);
    else
      sb = Environment.runCommand("cat " + conf);

    EventRecord retval = new EventRecord(InetAddress.getLocalHost()
        .getCanonicalHostName(), InetAddress.getAllByName(InetAddress.getLocalHost()
        .getHostName()), Calendar.getInstance(), "SMART", "Unknown",
        (conf == null ? "sudo smartctl --all " + device : "file " + conf), "-");
    // IBM SCSI disks
    retval.set("model", findPattern("Device\\s*:\\s*(.*)", sb.toString(), 1));
    retval.set("serial", findPattern("Serial\\s+Number\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("firmware", findPattern("Firmware\\s+Version\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("capacity", findPattern("User\\s+Capacity\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("status", findPattern("SMART\\s*Health\\s*Status:\\s*(.*)", sb
        .toString(), 1));
    retval.set("current_temperature", findPattern(
        "Current\\s+Drive\\s+Temperature\\s*:\\s*(.*)", sb.toString(), 1));
    retval.set("trip_temperature", findPattern(
        "Drive\\s+Trip\\s+Temperature\\s*:\\s*(.*)", sb.toString(), 1));
    retval.set("start_stop_count", findPattern(
        "start\\s+stop\\s+count\\s*:\\s*(\\d*)", sb.toString(), 1));

    String[] var = { "read", "write", "verify" };
    for (String s : var) {
      retval.set(s + "_ecc_fast", findPattern(s + "\\s*:\\s*(\\d*)", sb
          .toString(), 1));
      retval.set(s + "_ecc_delayed", findPattern(s
          + "\\s*:\\s*(\\d+\\s+){1}(\\d+)", sb.toString(), 2));
      retval.set(s + "_rereads", findPattern(
          s + "\\s*:\\s*(\\d+\\s+){2}(\\d+)", sb.toString(), 2));
      retval.set(s + "_GBs", findPattern(s
          + "\\s*:\\s*(\\d+\\s+){5}(\\d+.?\\d*)", sb.toString(), 2));
      retval.set(s + "_uncorrected",
          findPattern(s + "\\s*:\\s*(\\d+\\s+){5}(\\d+.?\\d*){1}\\s+(\\d+)", sb
              .toString(), 3));
    }

    // Hitachi IDE, SATA
    retval.set("model", findPattern("Device\\s*Model\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("serial", findPattern("Serial\\s+number\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("protocol", findPattern("Transport\\s+protocol\\s*:\\s*(.*)", sb
        .toString(), 1));
    retval.set("status", "PASSED".equalsIgnoreCase(findPattern(
        "test\\s*result\\s*:\\s*(.*)", sb.toString(), 1)) ? "OK" : "FAILED");

    readColumns(retval, sb);

    return retval;
  }

  /**
   * Reads attributes in the following format:
   * 
   * ID# ATTRIBUTE_NAME          FLAG     VALUE WORST THRESH TYPE      UPDATED  WHEN_FAILED RAW_VALUE
   * 3 Spin_Up_Time             0x0027   180   177   063    Pre-fail  Always       -       10265
   * 4 Start_Stop_Count         0x0032   253   253   000    Old_age   Always       -       34
   * 5 Reallocated_Sector_Ct    0x0033   253   253   063    Pre-fail  Always       -       0
   * 6 Read_Channel_Margin      0x0001   253   253   100    Pre-fail  Offline      -       0
   * 7 Seek_Error_Rate          0x000a   253   252   000    Old_age   Always       -       0
   * 8 Seek_Time_Performance    0x0027   250   224   187    Pre-fail  Always       -       53894
   * 9 Power_On_Minutes         0x0032   210   210   000    Old_age   Always       -       878h+00m
   * 10 Spin_Retry_Count        0x002b   253   252   157    Pre-fail  Always       -       0
   * 11 Calibration_Retry_Count 0x002b   253   252   223    Pre-fail  Always       -       0
   * 12 Power_Cycle_Count       0x0032   253   253   000    Old_age   Always       -       49
   * 192 PowerOff_Retract_Count 0x0032   253   253   000    Old_age   Always       -       0
   * 193 Load_Cycle_Count       0x0032   253   253   000    Old_age   Always       -       0
   * 194 Temperature_Celsius    0x0032   037   253   000    Old_age   Always       -       37
   * 195 Hardware_ECC_Recovered 0x000a   253   252   000    Old_age   Always       -       2645
   * 
   * This format is mostly found in IDE and SATA disks.
   * 
   * @param er the EventRecord in which to store attributes found
   * @param sb the StringBuffer with the text to parse
   * 
   * @return the EventRecord in which new attributes are stored.
   */
  private EventRecord readColumns(EventRecord er, StringBuffer sb) {

    Pattern pattern = Pattern.compile("^\\s{0,2}(\\d{1,3}\\s+.*)$",
        Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(sb);

    while (matcher.find()) {
      String[] tokens = matcher.group(1).split("\\s+");
      boolean failed = false;
      // check if this attribute is a failed one
      if (!tokens[8].equals("-"))
        failed = true;
      er.set(tokens[1].toLowerCase(), (failed ? "FAILED:" : "") + tokens[9]);
    }

    return er;
  }

  /**
   * Invokes query() to do the parsing and handles parsing errors for 
   * each one of the disks specified in the configuration. 
   * 
   * @return an array of EventRecords that holds one element that represents
   * the current state of the disk devices.
   */
  public EventRecord[] monitor() {
    ArrayList<EventRecord> recs = new ArrayList<EventRecord>();

    for (String device : devices) {
      try {
        recs.add(query(device));
      } catch (Exception e) {
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
    String retval = "S.M.A.R.T. disk attributes parser for disks ";
    for (String device : devices)
      retval += device + " ";
    return retval;
  }

}
