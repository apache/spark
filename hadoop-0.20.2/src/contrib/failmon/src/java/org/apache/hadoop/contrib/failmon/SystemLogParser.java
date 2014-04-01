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

import java.io.IOException;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**********************************************************
 * An object of this class parses a Unix system log file to create
 * appropriate EventRecords. Currently, only the syslogd logging 
 * daemon is supported.
 * 
 **********************************************************/

public class SystemLogParser extends LogParser {

  static String[] months = { "January", "February", "March", "April", "May",
      "June", "July", "August", "September", "October", "November", "December" };
  /**
   * Create a new parser object .
   */  
  public SystemLogParser(String fname) {
    super(fname);
    if ((dateformat = Environment.getProperty("log.system.dateformat")) == null)
      dateformat = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(\\d+)";
    if ((timeformat = Environment.getProperty("log.system.timeformat")) == null)
      timeformat = "\\d{2}:\\d{2}:\\d{2}";
  }

  /**
   * Parses one line of the log. If the line contains a valid 
   * log entry, then an appropriate EventRecord is returned, after all
   * relevant fields have been parsed.
   *
   *  @param line the log line to be parsed
   *    
   *  @return the EventRecord representing the log entry of the line. If 
   *  the line does not contain a valid log entry, then the EventRecord 
   *  returned has isValid() = false. When the end-of-file has been reached,
   *  null is returned to the caller.
   */
  public EventRecord parseLine(String line) throws IOException {

    EventRecord retval = null;

    if (line != null) {
      // process line
      String patternStr = "(" + dateformat + ")";
      patternStr += "\\s+";
      patternStr += "(" + timeformat + ")";
      patternStr += "\\s+(\\S*)\\s"; // for hostname
//      patternStr += "\\s*([\\w+\\.?]+)"; // for source
      patternStr += ":?\\s*(.+)"; // for the message
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(line);
      if (matcher.find() && matcher.groupCount() >= 0) {
        retval = new EventRecord(hostname, ips, parseDate(matcher.group(1),
            matcher.group(4)), "SystemLog", "Unknown", // loglevel
            "Unknown", // source
            matcher.group(6)); // message
      } else {
        retval = new EventRecord();
      }
    }

    return retval;
  }

  /**
   * Parse a date found in the system log.
   * 
   * @return a Calendar representing the date
   */
  protected Calendar parseDate(String strDate, String strTime) {
    Calendar retval = Calendar.getInstance();
    // set date
    String[] fields = strDate.split("\\s+");
    retval.set(Calendar.MONTH, parseMonth(fields[0]));
    retval.set(Calendar.DATE, Integer.parseInt(fields[1]));
    // set time
    fields = strTime.split(":");
    retval.set(Calendar.HOUR_OF_DAY, Integer.parseInt(fields[0]));
    retval.set(Calendar.MINUTE, Integer.parseInt(fields[1]));
    retval.set(Calendar.SECOND, Integer.parseInt(fields[2]));
    return retval;
  }

  /**
   * Convert the name of a month to the corresponding int value.
   * 
   * @return the int representation of the month.
   */
  private int parseMonth(String month) {
    for (int i = 0; i < months.length; i++)
      if (months[i].startsWith(month))
        return i;
    return -1;
  }
  
  /**
   * Return a String with information about this class
   * 
   * @return A String describing this class
   */
  public String getInfo() {
    return ("System Log Parser for file : " + file.getAbsoluteFile());
  }
}
