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

import java.io.{FileInputStream, FileWriter, File}
import java.net.URL

import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.{SecurityManager, SparkConf}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

class HistoryServerSuite extends FunSuite with BeforeAndAfter with Matchers {

  private val logDir = new File("src/test/resources/spark-events")
  private val expRoot = new File("src/test/resources/HistoryServerExpectations/")
  private val port = 18080

  private var provider: FsHistoryProvider = null
  private var server: HistoryServer = null

  def init(): Unit = {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
      .set("spark.history.fs.updateInterval", "0")
    provider = new FsHistoryProvider(conf)
    val securityManager = new SecurityManager(conf)

    server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
  }
  def stop(): Unit = {
    server.stop()
  }

  before {
    init()
  }

  after{
    stop()
  }

  val cases = Seq(
    "application list json" -> "applications",
    "one app json" -> "applications/local-1422981780767",
    "job list json" -> "applications/local-1422981780767/jobs",
    "executor list json" -> "applications/local-1422981780767/executors",
    "stage list json" -> "applications/local-1422981780767/stages",
    "complete stage list json" -> "applications/local-1422981780767/stages?status=complete",
    "failed stage list json" -> "applications/local-1422981780767/stages?status=failed",
    "one stage json" -> "applications/local-1422981780767/stages/1",
    "rdd list storage json" -> "applications/local-1422981780767/storage/rdd",
    "one rdd storage json" -> "applications/local-1422981780767/storage/rdd/0"
  )

  //run a bunch of characterization tests -- just verify the behavior is the same as what is saved in the test
  // resource folder
  cases.foreach{case(name, path) =>
      test(name){
        val json = getUrl(path)
        val exp = IOUtils.toString(new FileInputStream(new File(expRoot, path + "/json_expectation")))
        json should be (exp)
      }
  }

  test("fields w/ None are skipped (not written as null)") {
    pending
  }

  test("response codes on bad paths") {
    pending
  }


  def getUrl(path: String): String = {
    val u = new URL(s"http://localhost:$port/json/v1/$path")
    val in = u.openStream()
    try {
      val json = IOUtils.toString(in)
      json
    } finally {
      in.close()
    }
  }

  def generateExpectation(path: String): Unit = {
    val json = getUrl(path)
    val dir = new File(expRoot, path)
    dir.mkdirs()
    val out = new FileWriter(new File(dir, "json_expectation"))
    out.write(json)
    out.close()
  }

}

object HistoryServerSuite {
  def main(args: Array[String]): Unit = {
    /* generate the "expected" results for the characterization tests.  Just blindly assume the current behavior
     * is correct, and write out the returned json to the test/resource files
     */

    //TODO this should probably also run jobs to create the logs, so its totally self-contained

    val suite = new HistoryServerSuite
    FileUtils.deleteDirectory(suite.expRoot)
    suite.expRoot.mkdirs()
    try {
      suite.init()
      suite.cases.foreach { case (_, path) =>
        suite.generateExpectation(path)
      }
    } finally {
      suite.stop()
    }
  }
}