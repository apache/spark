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
 * An object of this class parses a Hadoop log file to create
 * appropriate EventRecords. The log file can either be the log 
 * of a NameNode or JobTracker or DataNode or TaskTracker.
 * 
 **********************************************************/

public class HadoopLogParser extends LogParser {

  /**
   * Create a new parser object and try to find the hostname
   * of the node that generated the log
   */
  public HadoopLogParser(String fname) {
    super(fname);
    if ((dateformat = Environment.getProperty("log.hadoop.dateformat")) == null)
      dateformat = "\\d{4}-\\d{2}-\\d{2}";
    if ((timeformat = Environment.getProperty("log.hadoop.timeformat")) == null)
      timeformat = "\\d{2}:\\d{2}:\\d{2}";
    findHostname();
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
      patternStr += ".{4}\\s(\\w*)\\s"; // for logLevel
      patternStr += "\\s*([\\w+\\.?]+)"; // for source
      patternStr += ":\\s+(.+)"; // for the message
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(line);

      if (matcher.find(0) && matcher.groupCount() >= 5) {
        retval = new EventRecord(hostname, ips, parseDate(matcher.group(1),
            matcher.group(2)),
	    "HadoopLog",
	    matcher.group(3), // loglevel
            matcher.group(4), // source
            matcher.group(5)); // message
      } else {
        retval = new EventRecord();
      }
    }

    return retval;
  }

  /**
   * Parse a date found in the Hadoop log.
   * 
   * @return a Calendar representing the date
   */
  protected Calendar parseDate(String strDate, String strTime) {
    Calendar retval = Calendar.getInstance();
    // set date
    String[] fields = strDate.split("-");
    retval.set(Calendar.YEAR, Integer.parseInt(fields[0]));
    retval.set(Calendar.MONTH, Integer.parseInt(fields[1]));
    retval.set(Calendar.DATE, Integer.parseInt(fields[2]));
    // set time
    fields = strTime.split(":");
    retval.set(Calendar.HOUR_OF_DAY, Integer.parseInt(fields[0]));
    retval.set(Calendar.MINUTE, Integer.parseInt(fields[1]));
    retval.set(Calendar.SECOND, Integer.parseInt(fields[2]));
    return retval;
  }

  /**
   * Attempt to determine the hostname of the node that created the
   * log file. This information can be found in the STARTUP_MSG lines 
   * of the Hadoop log, which are emitted when the node starts.
   * 
   */
  private void findHostname() {
    String startupInfo = Environment.runCommand(
        "grep --max-count=1 STARTUP_MSG:\\s*host " + file.getName()).toString();
    Pattern pattern = Pattern.compile("\\s+(\\w+/.+)\\s+");
    Matcher matcher = pattern.matcher(startupInfo);
    if (matcher.find(0)) {
      hostname = matcher.group(1).split("/")[0];
      ips = new String[1];
      ips[0] = matcher.group(1).split("/")[1];
    }
  }
  
  /**
   * Return a String with information about this class
   * 
   * @return A String describing this class
   */
  public String getInfo() {
    return ("Hadoop Log Parser for file: " + file.getName());
  }

}
