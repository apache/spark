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

import org.apache.spark.sql.hive.thriftserver.{HiveThriftServer2, SparkSQLEnv}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{SparkContext, Logging, SparkException}

/**
 * Spark Web UI tab that shows statistics of a streaming job.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
private[thriftserver] class ThriftServerTab(sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), "sqlserver") with Logging {

  override val name = "JDBC/ODBC Server"

  val parent = getSparkUI(sparkContext)
  val listener = HiveThriftServer2.listener

  attachPage(new ThriftServerPage(this))
  attachPage(new ThriftServerSessionPage(this))
  parent.attachTab(this)

  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

private[thriftserver] object ThriftServerTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
