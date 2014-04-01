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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Event log used by the fair scheduler for machine-readable debug info.
 * This class uses a log4j rolling file appender to write the log, but uses
 * a custom tab-separated event format of the form:
 * <pre>
 * DATE    EVENT_TYPE   PARAM_1   PARAM_2   ...
 * </pre>
 * Various event types are used by the fair scheduler. The purpose of logging
 * in this format is to enable tools to parse the history log easily and read
 * internal scheduler variables, rather than trying to make the log human
 * readable. The fair scheduler also logs human readable messages in the
 * JobTracker's main log.
 * 
 * Constructing this class creates a disabled log. It must be initialized
 * using {@link FairSchedulerEventLog#init(Configuration, String)} to begin
 * writing to the file.
 */
class FairSchedulerEventLog {
  private static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.mapred.FairSchedulerEventLog");
  
  /** Set to true if logging is disabled due to an error. */
  private boolean logDisabled = true;
  
  /**
   * Log directory, set by mapred.fairscheduler.eventlog.location in conf file;
   * defaults to {hadoop.log.dir}/fairscheduler.
   */
  private String logDir;
  
  /** 
   * Active log file, which is {LOG_DIR}/hadoop-{user}-fairscheduler.{host}.log.
   * Older files are also stored as {LOG_FILE}.date (date format YYYY-MM-DD).
   */ 
  private String logFile;
  
  /** Log4j appender used to write to the log file */
  private DailyRollingFileAppender appender;

  boolean init(Configuration conf, String jobtrackerHostname) {
    try {
      logDir = conf.get("mapred.fairscheduler.eventlog.location",
          new File(System.getProperty("hadoop.log.dir")).getAbsolutePath()
          + File.separator + "fairscheduler");
      Path logDirPath = new Path(logDir);
      FileSystem fs = logDirPath.getFileSystem(conf);
      if (!fs.exists(logDirPath)) {
        if (!fs.mkdirs(logDirPath)) {
          throw new IOException(
              "Mkdirs failed to create " + logDirPath.toString());
        }
      }
      String username = System.getProperty("user.name");
      logFile = String.format("%s%shadoop-%s-fairscheduler-%s.log",
          logDir, File.separator, username, jobtrackerHostname);
      logDisabled = false;
      PatternLayout layout = new PatternLayout("%d{ISO8601}\t%m%n");
      appender = new DailyRollingFileAppender(layout, logFile, "'.'yyyy-MM-dd");
      appender.activateOptions();
      LOG.info("Initialized fair scheduler event log, logging to " + logFile);
    } catch (IOException e) {
      LOG.error(
          "Failed to initialize fair scheduler event log. Disabling it.", e);
      logDisabled = true;
    }
    return !(logDisabled);
  }
  
  /**
   * Log an event, writing a line in the log file of the form
   * <pre>
   * DATE    EVENT_TYPE   PARAM_1   PARAM_2   ...
   * </pre>
   */
  synchronized void log(String eventType, Object... params) {
    try {
      if (logDisabled)
        return;
      StringBuffer buffer = new StringBuffer();
      buffer.append(eventType);
      for (Object param: params) {
        buffer.append("\t");
        buffer.append(param);
      }
      String message = buffer.toString();
      Logger logger = Logger.getLogger(getClass());
      appender.append(new LoggingEvent("", logger, Level.INFO, message, null));
    } catch (Exception e) {
      LOG.error("Failed to append to fair scheduler event log", e);
      logDisabled = true;
    }
  }
  
  /**
   * Flush and close the log.
   */
  void shutdown() {
    try {
      if (appender != null)
        appender.close();
    } catch (Exception e) {}
    logDisabled = true;
  }

  boolean isEnabled() {
    return !logDisabled;
  }
}
