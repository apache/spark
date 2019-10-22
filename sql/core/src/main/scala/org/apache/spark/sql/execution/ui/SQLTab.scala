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

package org.apache.spark.sql.execution.ui

import scala.collection.mutable.HashSet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.ui.{StreamingQueryPage, StreamingQueryStatisticsPage}
import org.apache.spark.ui.{SparkUI, SparkUITab}

class SQLTab(
    val sqlStore: SQLAppStatusStore,
    val streamQueryCache: Option[HashSet[(StreamingQuery, Long)]],
    sparkUI: SparkUI)
  extends SparkUITab(sparkUI, "SQL") with Logging {

  val parent = sparkUI

  attachPage(new AllExecutionsPage(this))
  attachPage(new ExecutionPage(this))
  attachPage(new StreamingQueryPage(this, streamQueryCache))
  attachPage(new StreamingQueryStatisticsPage(this, streamQueryCache))
  parent.attachTab(this)

  parent.addStaticHandler(SQLTab.STATIC_RESOURCE_DIR, "/static/sql")
}

object SQLTab {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/sql/execution/ui/static"
}
