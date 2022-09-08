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

package org.apache.spark.deploy.history

import java.net.URL
import javax.servlet.http.HttpServletResponse

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.status.api.v1.ApplicationStatus
import org.apache.spark.util.Utils

class HistoryServerPageSuite extends SparkFunSuite with BeforeAndAfter {
  private implicit val format: DefaultFormats.type = DefaultFormats

  private val logDirs = Seq(
    getTestResourcePath("spark-events-broken/previous-attempt-incomplete"),
    getTestResourcePath("spark-events-broken/last-attempt-incomplete")
  )

  private var server: Option[HistoryServer] = None
  private val localhost: String = Utils.localHostNameForURI()
  private var port: Int = -1

  private def startHistoryServer(logDir: String): Unit = {
    assert(server.isEmpty)
    val conf = new SparkConf()
      .set(HISTORY_LOG_DIR, logDir)
      .set(UPDATE_INTERVAL_S.key, "0")
      .set(IS_TESTING, true)
    val provider = new FsHistoryProvider(conf)
    provider.checkForLogs()
    val securityManager = HistoryServer.createSecurityManager(conf)
    val _server = new HistoryServer(conf, provider, securityManager, 18080)
    _server.bind()
    provider.start()
    server = Some(_server)
    port = _server.boundPort
  }

  private def stopHistoryServer(): Unit = {
    server.foreach(_.stop())
    server = None
  }

  private def callApplicationsAPI(requestedIncomplete: Boolean): Seq[JObject] = {
    val param = if (requestedIncomplete) {
      ApplicationStatus.RUNNING.toString.toLowerCase()
    } else {
      ApplicationStatus.COMPLETED.toString.toLowerCase()
    }
    val (code, jsonOpt, errOpt) = HistoryServerSuite.getContentAndCode(
      new URL(s"http://$localhost:$port/api/v1/applications?status=$param")
    )
    assert(code == HttpServletResponse.SC_OK)
    assert(jsonOpt.isDefined)
    assert(errOpt.isEmpty)
    val json = parse(jsonOpt.get).extract[List[JObject]]
    json
  }

  override def afterEach(): Unit = {
    super.afterEach()
    stopHistoryServer()
  }

  test("SPARK-39620: should behaves the same as REST API when filtering applications") {
    logDirs.foreach { logDir =>
      startHistoryServer(logDir)
      val page = new HistoryPage(server.get)
      Seq(true, false).foreach { requestedIncomplete =>
        val apiResponse = callApplicationsAPI(requestedIncomplete)
        if (page.shouldDisplayApplications(requestedIncomplete)) {
          assert(apiResponse.nonEmpty)
        } else {
          assert(apiResponse.isEmpty)
        }
      }
      stopHistoryServer()
    }
  }
}
