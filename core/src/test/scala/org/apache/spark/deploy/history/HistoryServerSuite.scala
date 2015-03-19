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

import java.io.{File, FileInputStream, FileWriter, IOException}
import java.net.{HttpURLConnection, URL}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.ui.SparkUI

class HistoryServerSuite extends FunSuite with BeforeAndAfter with Matchers with MockitoSugar {

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
    "maxDate2 app list json" -> "applications?maxDate=2015-02-03T10:42:40.000CST",
    "one app json" -> "applications/local-1422981780767",
    "job list json" -> "applications/local-1422981780767/jobs",
    "one job json" -> "applications/local-1422981780767/jobs/0",
    "succeeded job list json" -> "applications/local-1422981780767/jobs?status=succeeded",
    "succeeded&failed job list json" -> "applications/local-1422981780767/jobs?status=succeeded&status=failed",
    "executor list json" -> "applications/local-1422981780767/executors",
    "stage list json" -> "applications/local-1422981780767/stages",
    "complete stage list json" -> "applications/local-1422981780767/stages?status=complete",
    "failed stage list json" -> "applications/local-1422981780767/stages?status=failed",
    "one stage json" -> "applications/local-1422981780767/stages/1",
    "one stage attempt json" -> "applications/local-1422981780767/stages/1/0",
    "stage list with accumulable json" -> "applications/local-1426533911241/stages",
    "stage with accumulable json" -> "applications/local-1426533911241/stages/0",
    "rdd list storage json" -> "applications/local-1422981780767/storage/rdd",
    "one rdd storage json" -> "applications/local-1422981780767/storage/rdd/0"
    //TODO multi-attempt stages
  )

  //run a bunch of characterization tests -- just verify the behavior is the same as what is saved in the test
  // resource folder
  cases.foreach { case (name, path) =>
      test(name) {
        val (code, jsonOpt, errOpt) = getContentAndCode(path)
        code should be (HttpServletResponse.SC_OK)
        jsonOpt should be ('defined)
        errOpt should be (None)
        val json = jsonOpt.get
        val exp = IOUtils.toString(new FileInputStream(new File(expRoot, path + "/json_expectation")))
        //compare the ASTs so formatting differences don't cause failures
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        val jsonAst = parse(json)
        val expAst = parse(exp)
        HistoryServerSuite.assertValidDataInJson(jsonAst, expAst)
      }
  }

  test("security") {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
      .set("spark.history.fs.updateInterval", "0")
      .set("spark.acls.enable", "true")
      .set("spark.ui.view.acls", "user1")
    val securityManager = new SecurityManager(conf)

    val securePort = port + 1
    val secureServer = new HistoryServer(conf, provider, securityManager, securePort)
    secureServer.initialize()
    secureServer.bind()

    securityManager.checkUIViewPermissions("user1") should be (true)
    securityManager.checkUIViewPermissions("user2") should be (false)

    try {

      //TODO figure out a way to authenticate as the users in the requests
//      getContentAndCode("applications", securePort)._1 should be (200)
      pending

    } finally {
      secureServer.stop()
    }

  }

  test("response codes on bad paths") {
    val badAppId = getContentAndCode("applications/foobar")
    badAppId._1 should be (HttpServletResponse.SC_NOT_FOUND)
    badAppId._3 should be (Some("unknown app: foobar"))

    val badStageId = getContentAndCode("applications/local-1422981780767/stages/12345")
    badStageId._1 should be (HttpServletResponse.SC_NOT_FOUND)
    badStageId._3 should be (Some("unknown stage: 12345"))

    val badStageAttemptId = getContentAndCode("applications/local-1422981780767/stages/1/1")
    badStageAttemptId._1 should be (HttpServletResponse.SC_NOT_FOUND)
    badStageAttemptId._3 should be (Some("unknown attempt for stage 1.  Found attempts: [0]"))

    val badStageId2 = getContentAndCode("applications/local-1422981780767/stages/flimflam")
    badStageId2._1 should be (HttpServletResponse.SC_NOT_FOUND)
    // will take some mucking w/ jersey to get a better error msg in this case

    getContentAndCode("foobar")._1 should be (HttpServletResponse.SC_NOT_FOUND)
  }

  test("generate history page with relative links") {
    val historyServer = mock[HistoryServer]
    val request = mock[HttpServletRequest]
    val ui = mock[SparkUI]
    val link = "/history/app1"
    val info = new ApplicationHistoryInfo("app1", "app1", 0, 2, 1, "xxx", true)
    when(historyServer.getApplicationList(true)).thenReturn(Seq(info))
    when(ui.basePath).thenReturn(link)
    when(historyServer.getProviderConfig()).thenReturn(Map[String, String]())
    val page = new HistoryPage(historyServer)

    //when
    val response = page.render(request)

    //then
    val links = response \\ "a"
    val justHrefs = for {
      l <- links
      attrs <- l.attribute("href")
    } yield (attrs.toString)
    justHrefs should contain(link)
  }

  def getContentAndCode(path: String): (Int, Option[String], Option[String]) = {
    HistoryServerSuite.getContentAndCode(new URL(s"http://localhost:$port/json/v1/$path"))
  }

  def getUrl(path: String): String = {
    HistoryServerSuite.getUrl(new URL(s"http://localhost:$port/json/v1/$path"))
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

  def getContentAndCode(url: URL): (Int, Option[String], Option[String]) = {
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

  def getUrl(path: URL): String = {
    val (code, resultOpt, error) = getContentAndCode(path)
    if (code == 200)
      resultOpt.get
    else throw new RuntimeException("got code: " + code + " when getting " + path + " w/ error: " + error)
  }

  def assertValidDataInJson(validateJson: JValue, expectedJson: JValue) {
    val Diff(c, a, d) = validateJson diff expectedJson
    val validatePretty = JsonMethods.pretty(validateJson)
    val expectedPretty = JsonMethods.pretty(expectedJson)
    val errorMessage = s"Expected:\n$expectedPretty\nFound:\n$validatePretty"
    import org.scalactic.TripleEquals._
    assert(c === JNothing, s"$errorMessage\nChanged:\n${JsonMethods.pretty(c)}")
    assert(a === JNothing, s"$errorMessage\nAdded:\n${JsonMethods.pretty(a)}")
    assert(d === JNothing, s"$errorMessage\nDeleted:\n${JsonMethods.pretty(d)}")
  }
}

