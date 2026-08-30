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
package org.apache.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.udf.worker.core.WorkerLogger

/**
 * Adapts the UDF worker framework's [[WorkerLogger]] to Spark's
 * [[Logging]] trait so that worker log messages go through the
 * standard Spark logging pipeline.
 */
private[spark] class SparkUDFWorkerLogger
    extends WorkerLogger with Logging {

  override def info(msg: => String): Unit = {
    logInfo(msg)
  }
  override def info(msg: => String, t: Throwable): Unit =
    logInfo(msg, t)
  override def warn(msg: => String): Unit = logWarning(msg)
  override def warn(msg: => String, t: Throwable): Unit =
    logWarning(msg, t)
  override def debug(msg: => String): Unit = logDebug(msg)
  override def debug(msg: => String, t: Throwable): Unit =
    logDebug(msg, t)
}
