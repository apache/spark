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

package org.apache.spark.sql.hive.thriftserver.ui

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * Spark Web UI tab that shows statistics of jobs running in the thrift server.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
private[thriftserver] class ThriftServerTab(
   val store: HiveThriftServer2AppStatusStore,
   sparkUI: SparkUI) extends SparkUITab(sparkUI, "sqlserver") with Logging {
  override val name = "JDBC/ODBC Server"

  val parent = sparkUI
  val startTime = sparkUI.store.applicationInfo().attempts.head.startTime

  attachPage(new ThriftServerPage(this))
  attachPage(new ThriftServerSessionPage(this))
  parent.attachTab(this)
  def detach(): Unit = {
    sparkUI.detachTab(this)
  }

  override def displayOrder: Int = 1
}

private[thriftserver] object ThriftServerTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw QueryExecutionErrors.parentSparkUIToAttachTabNotFoundError()
    }
  }
}
