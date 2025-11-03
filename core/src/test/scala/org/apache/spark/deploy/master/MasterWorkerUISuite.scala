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

package org.apache.spark.deploy.master

import scala.concurrent.duration._
import scala.io.Source

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkConf
import org.apache.spark.deploy._
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.Utils

class MasterWorkerUISuite extends MasterSuiteBase {
  test("master/worker web ui available") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    val localCluster = LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://${Utils.localHostNameForURI()}:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(50.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          val JString(workerWebUi) = workerSummaryJson \ "webuiaddress"
          val workerResponse = parse(Utils
            .tryWithResource(Source.fromURL(s"$workerWebUi/json"))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
        }

        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        val workerLinks = (WORKER_LINK_RE findAllMatchIn html).toList
        workerLinks.size should be (2)
        workerLinks foreach { case WORKER_LINK_RE(workerUrl, workerId) =>
          val workerHtml = Utils
            .tryWithResource(Source.fromURL(workerUrl))(_.getLines().mkString("\n"))
          workerHtml should include ("Spark Worker at")
          workerHtml should include ("Running Executors (0)")
        }
      }
    } finally {
      localCluster.stop()
    }
  }

  test("master/worker web ui available with reverseProxy") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    conf.set(UI_REVERSE_PROXY, true)
    val localCluster = LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://${Utils.localHostNameForURI()}:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(50.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          // the webuiaddress intentionally points to the local web ui.
          // explicitly construct reverse proxy url targeting the master
          val JString(workerId) = workerSummaryJson \ "id"
          val url = s"$masterUrl/proxy/${workerId}/json"
          val workerResponse = parse(
            Utils.tryWithResource(Source.fromURL(url))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
        }

        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        html should include ("""href="/static""")
        html should include ("""src="/static""")
        verifyWorkerUI(html, masterUrl)
      }
    } finally {
      localCluster.stop()
      System.getProperties().remove("spark.ui.proxyBase")
    }
  }

  test("master/worker web ui available behind front-end reverseProxy") {
    implicit val formats = org.json4s.DefaultFormats
    val reverseProxyUrl = "http://proxyhost:8080/path/to/spark"
    val conf = new SparkConf()
    conf.set(UI_REVERSE_PROXY, true)
    conf.set(UI_REVERSE_PROXY_URL, reverseProxyUrl)
    val localCluster = LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://${Utils.localHostNameForURI()}:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(50.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          // the webuiaddress intentionally points to the local web ui.
          // explicitly construct reverse proxy url targeting the master
          val JString(workerId) = workerSummaryJson \ "id"
          val url = s"$masterUrl/proxy/${workerId}/json"
          val workerResponse = parse(Utils
            .tryWithResource(Source.fromURL(url))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
          (workerResponse \ "masterwebuiurl").extract[String] should be (reverseProxyUrl + "/")
        }

        System.getProperty("spark.ui.proxyBase") should be (reverseProxyUrl)
        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        verifyStaticResourcesServedByProxy(html, reverseProxyUrl)
        verifyWorkerUI(html, masterUrl, reverseProxyUrl)
      }
    } finally {
      localCluster.stop()
      System.getProperties().remove("spark.ui.proxyBase")
    }
  }

  test("SPARK-49007: Support custom master web ui title") {
    implicit val formats = org.json4s.DefaultFormats
    val title = "Spark Custom Title"
    val conf = new SparkConf().set(MASTER_UI_TITLE, title)
    val localCluster = LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://${Utils.localHostNameForURI()}:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(50.seconds), interval(100.milliseconds)) {
        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include (title)
      }
    } finally {
      localCluster.stop()
    }
  }
}
