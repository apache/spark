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

package org.apache.spark.executor.ui

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.WebUI

/**
 * Web UI server for the executor.
 * Without this, executor MetricsServlet sink can not be enabled in executor.
 */
private[executor] class ExecutorWebUI(
    conf: SparkConf,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager,
    securityManager.getSSLOptions("executor"),
    port, conf, name = "ExecutorUI") with Logging {

  initialize()

  /**
   * In furture, maybe we need more information from this UI.
   * Now we just leave everything empty.
   */
  def initialize(): Unit = {}
}
