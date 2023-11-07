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

package org.apache.spark.sql.connect.ui

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.ui.{SparkUI, SparkUITab}

private[connect] class SparkConnectServerTab(
    val store: SparkConnectServerAppStatusStore,
    sparkUI: SparkUI)
    extends SparkUITab(sparkUI, "connect")
    with Logging {

  override val name = "Connect"

  val parent = sparkUI
  val startTime =
    try {
      sparkUI.store.applicationInfo().attempts.head.startTime
    } catch {
      case _: NoSuchElementException => new Date(System.currentTimeMillis())
    }

  attachPage(new SparkConnectServerPage(this))
  attachPage(new SparkConnectServerSessionPage(this))
  parent.attachTab(this)
  def detach(): Unit = {
    parent.detachTab(this)
  }

  override def displayOrder: Int = 3
}

private[connect] object SparkConnectServerTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw QueryExecutionErrors.parentSparkUIToAttachTabNotFoundError()
    }
  }
}
