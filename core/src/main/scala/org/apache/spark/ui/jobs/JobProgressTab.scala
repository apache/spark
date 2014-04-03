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

package org.apache.spark.ui.jobs

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.{SparkUI, UITab}

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[ui] class JobProgressTab(parent: SparkUI) extends UITab("stages") {
  val appName = parent.appName
  val basePath = parent.basePath
  val live = parent.live
  val sc = parent.sc

  def start() {
    val conf = if (live) sc.conf else new SparkConf
    listener = Some(new JobProgressListener(conf))
    attachPage(new IndexPage(this))
    attachPage(new StagePage(this))
    attachPage(new PoolPage(this))
  }

  def jobProgressListener: JobProgressListener = {
    assert(listener.isDefined, "JobProgressTab has not started yet!")
    listener.get.asInstanceOf[JobProgressListener]
  }

  def isFairScheduler = jobProgressListener.schedulingMode.exists(_ == SchedulingMode.FAIR)

  def headerTabs: Seq[UITab] = parent.getTabs
}
