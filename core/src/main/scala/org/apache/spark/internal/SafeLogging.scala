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

trait SafeLogging extends Logging {
  // Method to get the logger name for this object
  override protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    s"com.palantir.${this.getClass.getName.stripSuffix("$")}"
  }

  protected def safeLogInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }
  protected def safeLogDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def safeLogTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def safeLogWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def safeLogError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }


  // You can only safelog to SafeLogging logs, everything else is unsupported
  override protected def logInfo(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }
  override protected def logDebug(msg: => String) {
    throw new UnsupportedOperationException
  }
  override protected def logTrace(msg: => String) {
    throw new UnsupportedOperationException
  }
  override protected def logWarning(msg: => String) {
    throw new UnsupportedOperationException
  }
  override protected def logError(msg: => String) {
    throw new UnsupportedOperationException
  }

  override protected def logInfo(msg: => String, throwable: Throwable) {
    throw new UnsupportedOperationException
  }
  override protected def logDebug(msg: => String, throwable: Throwable) {
    throw new UnsupportedOperationException
  }
  override protected def logTrace(msg: => String, throwable: Throwable) {
    throw new UnsupportedOperationException
  }
  override protected def logWarning(msg: => String, throwable: Throwable) {
    throw new UnsupportedOperationException
  }
  override protected def logError(msg: => String, throwable: Throwable) {
    throw new UnsupportedOperationException
  }
}
