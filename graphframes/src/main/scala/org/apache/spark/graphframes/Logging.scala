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

package org.apache.spark.graphframes

import org.slf4j.Logger
import org.slf4j.LoggerFactory

// This needs to be accessible to org.apache.spark.graphx.lib.backport
private[org] trait Logging {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  protected def logDebug(s: => String): Unit = {
    if (logger.isDebugEnabled) logger.debug(s)
  }

  protected def logWarn(s: => String): Unit = {
    if (logger.isWarnEnabled) logger.warn(s)
  }

  protected def logInfo(s: => String): Unit = {
    if (logger.isInfoEnabled) logger.info(s)
  }

  protected def logTrace(s: => String): Unit = {
    if (logger.isTraceEnabled) logger.trace(s)
  }

  protected def resultIsPersistent(): Unit = {
    logWarn("Returned DataFrame is persistent and materialized!")
  }
}
