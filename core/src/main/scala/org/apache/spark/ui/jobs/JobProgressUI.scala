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

import scala.concurrent.duration._

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.Seq
import scala.collection.mutable.{HashSet, ListBuffer, HashMap, ArrayBuffer}

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.{ExceptionFailure, SparkContext, Success}
import org.apache.spark.scheduler._
import collection.mutable
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(val sc: SparkContext) {
  private var _listener: Option[JobProgressListener] = None
  def listener = _listener.get
  val dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  private val indexPage = new IndexPage(this)
  private val stagePage = new StagePage(this)
  private val poolPage = new PoolPage(this)

  def start() {
    _listener = Some(new JobProgressListener(sc))
    sc.addSparkListener(listener)
  }

  def formatDuration(ms: Long) = Utils.msDurationToString(ms)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages/pool", (request: HttpServletRequest) => poolPage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}
