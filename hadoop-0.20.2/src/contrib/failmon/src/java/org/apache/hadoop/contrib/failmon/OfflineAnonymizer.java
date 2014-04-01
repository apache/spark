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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**********************************************************
 * This class can be used to anonymize logs independently of
 * Hadoop and the Executor. It parses the specified log file to
 * create log records for it and then passes them to the Anonymizer.
 * After they are anonymized, they are written to a local file,
 * which is then compressed and stored locally.
 * 
 **********************************************************/

public class OfflineAnonymizer {

  public enum LogType {
    HADOOP, SYSTEM
  };

  LogType logtype;

  File logfile;

  LogParser parser;

  /**
   * Creates an OfflineAnonymizer for a specific log file.
   * 
   * @param logtype the type of the log file. This can either be
   * LogFile.HADOOP or LogFile.SYSTEM
   * @param filename the path to the log file
   * 
   */  
  public OfflineAnonymizer(LogType logtype, String filename) {

    logfile = new File(filename);

    if (!logfile.exists()) {
      System.err.println("Input file does not exist!");
      System.exit(0);
    }

    if (logtype == LogType.HADOOP)
      parser = new HadoopLogParser(filename);
    else
      parser = new SystemLogParser(filename);
  }

  /**
   * Performs anonymization for the log file. Log entries are
   * read one by one and EventRecords are created, which are then
   * anonymized and written to the output.
   * 
   */
  public void anonymize() throws Exception {
    EventRecord er = null;
    SerializedRecord sr = null;

    BufferedWriter bfw = new BufferedWriter(new FileWriter(logfile.getName()
        + ".anonymized"));

    System.out.println("Anonymizing log records...");
    while ((er = parser.getNext()) != null) {
      if (er.isValid()) {
        sr = new SerializedRecord(er);
        Anonymizer.anonymize(sr);
        bfw.write(LocalStore.pack(sr).toString());
        bfw.write(LocalStore.RECORD_SEPARATOR);
      }
    }
    bfw.flush();
    bfw.close();
    System.out.println("Anonymized log records written to " + logfile.getName()
        + ".anonymized");

    System.out.println("Compressing output file...");
    LocalStore.zipCompress(logfile.getName() + ".anonymized");
    System.out.println("Compressed output file written to " + logfile.getName()
        + ".anonymized" + LocalStore.COMPRESSION_SUFFIX);
  }

  public static void main(String[] args) {

    if (args.length < 2) {
      System.out.println("Usage: OfflineAnonymizer <log_type> <filename>");
      System.out
          .println("where <log_type> is either \"hadoop\" or \"system\" and <filename> is the path to the log file");
      System.exit(0);
    }

    LogType logtype = null;

    if (args[0].equalsIgnoreCase("-hadoop"))
      logtype = LogType.HADOOP;
    else if (args[0].equalsIgnoreCase("-system"))
      logtype = LogType.SYSTEM;
    else {
      System.err.println("Invalid first argument.");
      System.exit(0);
    }

    OfflineAnonymizer oa = new OfflineAnonymizer(logtype, args[1]);

    try {
      oa.anonymize();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return;
  }
}
