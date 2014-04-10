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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.Node

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}

/*
 * This is an example of how to extend the SparkUI by adding new tabs to it. It is intended
 * only as a demonstration and should be removed before merging into master!
 *
 *   bin/spark-class org.apache.spark.ui.FooTab
 */

/** A tab that displays basic information about jobs seen so far. */
private[spark] class FooTab(parent: SparkUI) extends UITab("foo") {
  val appName = parent.appName
  val basePath = parent.basePath

  def start() {
    listener = Some(new FooListener)
    attachPage(new IndexPage(this))
  }

  def fooListener: FooListener = {
    assert(listener.isDefined, "ExecutorsTab has not started yet!")
    listener.get.asInstanceOf[FooListener]
  }

  def headerTabs: Seq[UITab] = parent.getTabs
}

/** A foo page. Enough said. */
private[spark] class IndexPage(parent: FooTab) extends UIPage("") {
  private val appName = parent.appName
  private val basePath = parent.basePath
  private val listener = parent.fooListener

  override def render(request: HttpServletRequest): Seq[Node] = {
    val results = listener.jobResultMap.toSeq.sortBy { case (k, _) => k }
    val content =
      <div class="row-fluid">
        <div class="span12">
          <strong>Foo Jobs: </strong>
          <ul>
            {results.map { case (k, v) => <li>Job {k}: <strong>{v}</strong></li> }}
          </ul>
        </div>
      </div>
    UIUtils.headerSparkPage(content, basePath, appName, "Foo", parent.headerTabs, parent)
  }
}

/** A listener that maintains a mapping between job IDs and job results. */
private[spark] class FooListener extends SparkListener {
  val jobResultMap = mutable.Map[Int, String]()

  override def onJobEnd(end: SparkListenerJobEnd) {
    jobResultMap(end.jobId) = end.jobResult.toString
  }
}


/**
 * Start a SparkContext and a SparkUI with a FooTab attached.
 */
private[spark] object FooTab {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Foo Tab", new SparkConf)
    val fooTab = new FooTab(sc.ui)
    sc.ui.attachTab(fooTab)

    // Run a few jobs
    sc.parallelize(1 to 1000).count()
    sc.parallelize(1 to 2000).persist().count()
    sc.parallelize(1 to 3000).map(i => (i/2, i)).groupByKey().count()
    sc.parallelize(1 to 4000).map(i => (i/2, i)).groupByKey().persist().count()
    sc.parallelize(1 to 5000).map(i => (i/2, i)).groupByKey().persist().count()
    sc.parallelize(1 to 6000).map(i => (i/2, i)).groupByKey().persist().count()
    sc.parallelize(1 to 7000).map(i => (i/2, i)).groupByKey().persist().count()

    readLine("\n> Started SparkUI with a Foo tab...")
  }
}
