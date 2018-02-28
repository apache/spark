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
package org.apache.spark.deploy.k8s.integrationtest

import org.apache.log4j.{Logger, LogManager, Priority}

trait Logging {

  private val log: Logger = LogManager.getLogger(this.getClass)

  protected def logDebug(msg: => String) = if (log.isDebugEnabled) log.debug(msg)

  protected def logInfo(msg: => String) = if (log.isInfoEnabled) log.info(msg)

  protected def logWarning(msg: => String) = if (log.isEnabledFor(Priority.WARN)) log.warn(msg)

  protected def logWarning(msg: => String, throwable: Throwable) =
    if (log.isEnabledFor(Priority.WARN)) log.warn(msg, throwable)

  protected def logError(msg: => String) = if (log.isEnabledFor(Priority.ERROR)) log.error(msg)
}
