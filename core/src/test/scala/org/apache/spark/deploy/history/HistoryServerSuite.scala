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

import java.io.{IOException, FileInputStream, FileWriter, File}
import java.net.{HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

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
    server.initialize()
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
    "completed app list json" -> "applications?status=completed",
    "running app list json" -> "applications?status=running",
    "minDate app list json" -> "applications?minDate=2015-02-10",
    "maxDate app list json" -> "applications?maxDate=2015-02-10",
    "maxDate2 app list json" -> "applications?maxDate=2015-02-03T10:42:40CST",
    "one app json" -> "applications/local-1422981780767",
    "job list json" -> "applications/local-1422981780767/jobs",
    "succeeded job list json" -> "applications/local-1422981780767/jobs?status=succeeded",
    "succeeded&failed job list json" -> "applications/local-1422981780767/jobs?status=succeeded&status=failed",
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
        val (code, jsonOpt, errOpt) = getContentAndCode(path)
        code should be (HttpServletResponse.SC_OK)
        jsonOpt should be ('defined)
        errOpt should be (None)
        val json = jsonOpt.get
        val exp = IOUtils.toString(new FileInputStream(new File(expRoot, path + "/json_expectation")))
        json should be (exp)
      }
  }

  test("fields w/ None are skipped (not written as null)") {
    pending
  }

  test("security") {
    pending
  }

  test("response codes on bad paths") {
    val badAppId = getContentAndCode("applications/foobar")
    badAppId._1 should be (HttpServletResponse.SC_NOT_FOUND)
    badAppId._3 should be (Some("unknown app: foobar"))

    val badStageId = getContentAndCode("applications/local-1422981780767/stages/12345")
    badStageId._1 should be (HttpServletResponse.SC_NOT_FOUND)
    badStageId._3 should be (Some("unknown stage: 12345"))

    val badStageId2 = getContentAndCode("applications/local-1422981780767/stages/flimflam")
    badStageId2._1 should be (HttpServletResponse.SC_NOT_FOUND)
    //will take some mucking w/ jersey to get a better error msg here ...


    getContentAndCode("foobar")._1 should be (HttpServletResponse.SC_NOT_FOUND)

  }

  def getContentAndCode(path: String): (Int, Option[String], Option[String]) = {
    val url = new URL(s"http://localhost:$port/json/v1/$path")
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()
    val code = connection.getResponseCode()
    val inString = try {
      val in = Option(connection.getInputStream())
      in.map{IOUtils.toString}
    } catch {
      case io: IOException => None
    }
    val errString = try {
      val err = Option(connection.getErrorStream())
      err.map{IOUtils.toString}
    } catch {
      case io: IOException => None
    }
    (code, inString, errString)
  }


  def getUrl(path: String): String = {
    val (code, resultOpt, error) = getContentAndCode(path)
    if (code == 200)
      resultOpt.get
    else throw new RuntimeException("got code: " + code + " when getting " + path + " w/ error: " + error)
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