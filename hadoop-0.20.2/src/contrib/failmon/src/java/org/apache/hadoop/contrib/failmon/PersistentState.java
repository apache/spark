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

import java.util.Properties;
import java.util.Calendar;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**********************************************************
 * This class takes care of the information that needs to be
 * persistently stored locally on nodes. Bookkeeping is done for the
 * state of parsing of log files, so that the portion of the file that
 * has already been parsed in previous calls will not be parsed again.
 * For each log file, we maintain the byte offset of the last
 * character parsed in previous passes. Also, the first entry in the
 * log file is stored, so that FailMon can determine when a log file
 * has been rotated (and thus parsing needs to start from the
 * beginning of the file). We use a property file to store that
 * information. For each log file we create a property keyed by the
 * filename, the value of which contains the byte offset and first log
 * entry separated by a SEPARATOR.
 * 
 **********************************************************/

public class PersistentState {

  private final static String SEPARATOR = "###";
  
  static String filename;
  static Properties persData = new Properties();
  
  /**
   * Read the state of parsing for all open log files from a property
   * file.
   * 
   * @param fname the filename of the property file to be read
   */

  public static void readState(String fname) {

    filename = fname;
    
    try {
      persData.load(new FileInputStream(filename));
    } catch (FileNotFoundException e1) {
      // ignore
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

   /**
   * Read and return the state of parsing for a particular log file.
   * 
   * @param fname the log file for which to read the state
   */
  public static ParseState getState(String fname) {
    String [] fields = persData.getProperty(fname, "null" + SEPARATOR + "0").split(SEPARATOR, 2);
    String firstLine;
    long offset;
    
    if (fields.length < 2) {
      System.err.println("Malformed persistent state data found");
      Environment.logInfo("Malformed persistent state data found");
      firstLine = null;
      offset = 0;
    } else {
      firstLine = (fields[0].equals("null") ? null : fields[0]);
      offset = Long.parseLong(fields[1]);
    }

    return new ParseState(fname, firstLine, offset);
  }

  /**
   * Set the state of parsing for a particular log file.
   * 
   * @param state the ParseState to set
   */
  public static void setState(ParseState state) {

    if (state == null) {
      System.err.println("Null state found");
      Environment.logInfo("Null state found");
    }

    persData.setProperty(state.filename, state.firstLine + SEPARATOR + state.offset);
  }

  /**
   * Upadate the state of parsing for a particular log file.
   * 
   * @param filename the log file for which to update the state
   * @param firstLine the first line of the log file currently
   * @param offset the byte offset of the last character parsed
   */ 
  public static void updateState(String filename, String firstLine, long offset) {

    ParseState ps = getState(filename);

    if (firstLine != null)
      ps.firstLine = firstLine;

    ps.offset = offset;

    setState(ps);
  }

  /**
   * Write the state of parsing for all open log files to a property
   * file on disk.
   * 
   * @param fname the filename of the property file to write to
   */
  public static void writeState(String fname) {
    try {
      persData.store(new FileOutputStream(fname), Calendar.getInstance().getTime().toString());
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}

/**********************************************************
 * This class represents the state of parsing for a particular log
 * file.
 * 
 **********************************************************/

class ParseState {

  public String filename;
  public String firstLine;
  public long offset;

  public ParseState(String _filename, String _firstLine, long _offset) {
    this.filename = _filename;
    this.firstLine = _firstLine;
    this.offset = _offset;
  }
}
