/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.utils

import java.io.PrintStream

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.session.SessionState
import org.slf4j.Logger


/**
 * This class provides helper routines to emit informational and error
 * messages to the user and log4j files while obeying the current session's
 * verbosity levels.
 *
 * NEVER write directly to the SessionStates standard output other than to
 * emit result data DO use printInfo and printError provided by LogHelper to
 * emit non result data strings.
 *
 * It is perfectly acceptable to have global static LogHelper objects (for
 * example - once per module) LogHelper always emits info/error to current
 * session as required.
 */
object LogHelper {
  def getInfoStream: PrintStream = {
    val ss = SessionState.get
    if ((ss != null) && (ss.info != null)) ss.info
    else getErrStream
  }

  def getErrStream: PrintStream = {
    val ss = SessionState.get
    if ((ss != null) && (ss.err != null)) ss.err
    else System.err
  }
}

class LogHelper(var LOG: Logger, var isSilent: Boolean) {
  def this(LOG: Nothing) {
    this(LOG, false)
  }

  def getOutStream: PrintStream = {
    val ss = SessionState.get
    if ((ss != null) && (ss.out != null)) ss.out
    else System.out
  }

  def getChildOutStream: PrintStream = {
    val ss = SessionState.get
    if ((ss != null) && (ss.childOut != null)) ss.childOut
    else System.out
  }

  def getChildErrStream: PrintStream = {
    val ss = SessionState.get
    if ((ss != null) && (ss.childErr != null)) ss.childErr
    else System.err
  }

  def getIsSilent: Boolean = {
    val ss = SessionState.get
    // use the session or the one supplied in constructor
    if (ss != null) ss.getIsSilent
    else isSilent
  }

  def logInfo(info: String): Unit = {
    logInfo(info, null)
  }

  def logInfo(info: String, detail: String): Unit = {
    LOG.info(info + StringUtils.defaultString(detail))
  }

  def printInfo(info: String): Unit = {
    printInfo(info, null)
  }

  def printInfo(info: String, isSilent: Boolean): Unit = {
    printInfo(info, null, isSilent)
  }

  def printInfo(info: String, detail: String): Unit = {
    printInfo(info, detail, getIsSilent)
  }

  def printInfo(info: String, detail: String, isSilent: Boolean): Unit = {
    if (!isSilent) {
      // scalastyle:off
      LogHelper.getInfoStream.println(info)
      // scalastyle:on
    }
    LOG.info(info + StringUtils.defaultString(detail))
  }

  def printInfoNoLog(info: String): Unit = {
    if (!getIsSilent) {
      // scalastyle:off
      LogHelper.getInfoStream.println(info)
      // scalastyle:on
    }
  }

  def printError(error: String): Unit = {
    printError(error, null)
  }

  def printError(error: String, detail: String): Unit = {
    // scalastyle:off
    LogHelper.getErrStream.println(error)
    // scalastyle:on
    LOG.error(error + StringUtils.defaultString(detail))
  }
}