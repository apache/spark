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

package org.apache.spark.internal

import com.palantir.logsafe.Arg
import org.slf4j.{Logger, LoggerFactory}

trait SafeLogging {
  // Taken from Logging.scala
  @transient private[this] var safeLog_ : Logger = null

  // Method to get the logger name for this object
  protected def getLogName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def safeLog: Logger = {
    if (safeLog_ == null) {
      safeLog_ = LoggerFactory.getLogger(getLogName)
    }
    safeLog_
  }

  def safeLogIsInfoEnabled: Boolean = safeLog.isInfoEnabled

  def safeLogInfo(message: String, args: Arg[_]*): Unit = {
    if (safeLog.isInfoEnabled) safeLog.info(message, args: _*)
  }

  def safeLogInfo(message: String, error: Throwable, args: Arg[_]*): Unit = {
    if (safeLog.isInfoEnabled) safeLog.info(message, args :+ error: _*)
  }

  def safeLogDebug(message: String, args: Arg[_]*): Unit = {
    if (safeLog.isDebugEnabled) safeLog.debug(message, args: _*)
  }

  def safeLogDebug(message: String, error: Throwable, args: Arg[_]*): Unit = {
    if (safeLog.isDebugEnabled) safeLog.debug(message, args :+ error: _*)
  }

  def safeLogTrace(message: String, args: Arg[_]*): Unit = {
    if (safeLog.isTraceEnabled) safeLog.trace(message, args: _*)
  }

  def safeLogTrace(message: String, error: Throwable, args: Arg[_]*): Unit = {
    if (safeLog.isTraceEnabled) safeLog.trace(message, args :+ error: _*)
  }

  def safeLogWarning(message: String, args: Arg[_]*): Unit = {
    if (safeLog.isWarnEnabled) safeLog.warn(message, args: _*)
  }

  def safeLogWarning(message: String, error: Throwable, args: Arg[_]*): Unit = {
    if (safeLog.isWarnEnabled) safeLog.warn(message, args :+ error: _*)
  }

  def safeLogError(message: String, args: Arg[_]*): Unit = {
    if (safeLog.isErrorEnabled) safeLog.error(message, args: _*)
  }

  def safeLogError(message: String, error: Throwable, args: Arg[_]*): Unit = {
    if (safeLog.isErrorEnabled) safeLog.error(message, args :+ error: _*)
  }
}

