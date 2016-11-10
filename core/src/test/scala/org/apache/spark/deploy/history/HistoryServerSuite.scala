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

import java.io.{File, FileInputStream, FileWriter, InputStream, IOException}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipInputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.google.common.base.Charsets
import com.google.common.io.{ByteStreams, Files}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{JsonTestUtils, SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.ui.{SparkUI, UIUtils}

/**
 * A collection of tests against the historyserver, including comparing responses from the json
 * metrics api to a set of known "golden files".  If new endpoints / parameters are added,
 * cases should be added to this test suite.  The expected outcomes can be genered by running
 * the HistoryServerSuite.main.  Note that this will blindly generate new expectation files matching
 * the current behavior -- the developer must verify that behavior is correct.
 *
 * Similarly, if the behavior is changed, HistoryServerSuite.main can be run to update the
 * expectations.  However, in general this should be done with extreme caution, as the metrics
 * are considered part of Spark's public api.
 */
class HistoryServerSuite extends SparkFunSuite with BeforeAndAfter with Matchers with MockitoSugar
  with JsonTestUtils {

  private val logDir = new File("src/test/resources/spark-events")
  private val expRoot = new File("src/test/resources/HistoryServerExpectations/")

  private var provider: FsHistoryProvider = null
  private var server: HistoryServer = null
  private var port: Int = -1

  def init(): Unit = {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
      .set("spark.history.fs.updateInterval", "0")
      .set("spark.testing", "true")
    provider = new FsHistoryProvider(conf)
    provider.checkForLogs()
    val securityManager = new SecurityManager(conf)

    server = new HistoryServer(conf, provider, securityManager, 18080)
    server.initialize()
    server.bind()
    port = server.boundPort
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
    "maxDate2 app list json" -> "applications?maxDate=2015-02-03T16:42:40.000GMT",
    "one app json" -> "applications/local-1422981780767",
    "one app multi-attempt json" -> "applications/local-1426533911241",
    "job list json" -> "applications/local-1422981780767/jobs",
    "job list from multi-attempt app json(1)" -> "applications/local-1426533911241/1/jobs",
    "job list from multi-attempt app json(2)" -> "applications/local-1426533911241/2/jobs",
    "one job json" -> "applications/local-1422981780767/jobs/0",
    "succeeded job list json" -> "applications/local-1422981780767/jobs?status=succeeded",
    "succeeded&failed job list json" ->
      "applications/local-1422981780767/jobs?status=succeeded&status=failed",
    "executor list json" -> "applications/local-1422981780767/executors",
    "stage list json" -> "applications/local-1422981780767/stages",
    "complete stage list json" -> "applications/local-1422981780767/stages?status=complete",
    "failed stage list json" -> "applications/local-1422981780767/stages?status=failed",
    "one stage json" -> "applications/local-1422981780767/stages/1",
    "one stage attempt json" -> "applications/local-1422981780767/stages/1/0",

    "stage task summary w shuffle write"
      -> "applications/local-1430917381534/stages/0/0/taskSummary",
    "stage task summary w shuffle read"
      -> "applications/local-1430917381534/stages/1/0/taskSummary",
    "stage task summary w/ custom quantiles" ->
      "applications/local-1430917381534/stages/0/0/taskSummary?quantiles=0.01,0.5,0.99",

    "stage task list" -> "applications/local-1430917381534/stages/0/0/taskList",
    "stage task list w/ offset & length" ->
      "applications/local-1430917381534/stages/0/0/taskList?offset=10&length=50",
    "stage task list w/ sortBy" ->
      "applications/local-1430917381534/stages/0/0/taskList?sortBy=DECREASING_RUNTIME",
    "stage task list w/ sortBy short names: -runtime" ->
      "applications/local-1430917381534/stages/0/0/taskList?sortBy=-runtime",
    "stage task list w/ sortBy short names: runtime" ->
      "applications/local-1430917381534/stages/0/0/taskList?sortBy=runtime",

    "stage list with accumulable json" -> "applications/local-1426533911241/1/stages",
    "stage with accumulable json" -> "applications/local-1426533911241/1/stages/0/0",
    "stage task list from multi-attempt app json(1)" ->
      "applications/local-1426533911241/1/stages/0/0/taskList",
    "stage task list from multi-attempt app json(2)" ->
      "applications/local-1426533911241/2/stages/0/0/taskList",

    "rdd list storage json" -> "applications/local-1422981780767/storage/rdd"
    // Todo: enable this test when logging the even of onBlockUpdated. See: SPARK-13845
    // "one rdd storage json" -> "applications/local-1422981780767/storage/rdd/0"
  )

  // run a bunch of characterization tests -- just verify the behavior is the same as what is saved
  // in the test resource folder
  cases.foreach { case (name, path) =>
    test(name) {
      val (code, jsonOpt, errOpt) = getContentAndCode(path)
      code should be (HttpServletResponse.SC_OK)
      jsonOpt should be ('defined)
      errOpt should be (None)
      val json = jsonOpt.get
      val exp = IOUtils.toString(new FileInputStream(
        new File(expRoot, HistoryServerSuite.sanitizePath(name) + "_expectation.json")))
      // compare the ASTs so formatting differences don't cause failures
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      val jsonAst = parse(json)
      val expAst = parse(exp)
      assertValidDataInJson(jsonAst, expAst)
    }
  }

  test("download all logs for app with multiple attempts") {
    doDownloadTest("local-1430917381535", None)
  }

  test("download one log for app with multiple attempts") {
    (1 to 2).foreach { attemptId => doDownloadTest("local-1430917381535", Some(attemptId)) }
  }

  test("download legacy logs - all attempts") {
    doDownloadTest("local-1426533911241", None, legacy = true)
  }

  test("download legacy logs - single  attempts") {
    (1 to 2). foreach {
      attemptId => doDownloadTest("local-1426533911241", Some(attemptId), legacy = true)
    }
  }

  // Test that the files are downloaded correctly, and validate them.
  def doDownloadTest(appId: String, attemptId: Option[Int], legacy: Boolean = false): Unit = {

    val url = attemptId match {
      case Some(id) =>
        new URL(s"${generateURL(s"applications/$appId")}/$id/logs")
      case None =>
        new URL(s"${generateURL(s"applications/$appId")}/logs")
    }

    val (code, inputStream, error) = HistoryServerSuite.connectAndGetInputStream(url)
    code should be (HttpServletResponse.SC_OK)
    inputStream should not be None
    error should be (None)

    val zipStream = new ZipInputStream(inputStream.get)
    var entry = zipStream.getNextEntry
    entry should not be null
    val totalFiles = {
      if (legacy) {
        attemptId.map { x => 3 }.getOrElse(6)
      } else {
        attemptId.map { x => 1 }.getOrElse(2)
      }
    }
    var filesCompared = 0
    while (entry != null) {
      if (!entry.isDirectory) {
        val expectedFile = {
          if (legacy) {
            val splits = entry.getName.split("/")
            new File(new File(logDir, splits(0)), splits(1))
          } else {
            new File(logDir, entry.getName)
          }
        }
        val expected = Files.toString(expectedFile, Charsets.UTF_8)
        val actual = new String(ByteStreams.toByteArray(zipStream), Charsets.UTF_8)
        actual should be (expected)
        filesCompared += 1
      }
      entry = zipStream.getNextEntry
    }
    filesCompared should be (totalFiles)
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

    val badQuantiles = getContentAndCode(
      "applications/local-1430917381534/stages/0/0/taskSummary?quantiles=foo,0.1")
    badQuantiles._1 should be (HttpServletResponse.SC_BAD_REQUEST)
    badQuantiles._3 should be (Some("Bad value for parameter \"quantiles\".  Expected a double, " +
      "got \"foo\""))

    getContentAndCode("foobar")._1 should be (HttpServletResponse.SC_NOT_FOUND)
  }

  test("generate history page with relative links") {
    val historyServer = mock[HistoryServer]
    val request = mock[HttpServletRequest]
    val ui = mock[SparkUI]
    val link = "/history/app1"
    val info = new ApplicationHistoryInfo("app1", "app1",
      List(ApplicationAttemptInfo(None, 0, 2, 1, "xxx", true)))
    when(historyServer.getApplicationList()).thenReturn(Seq(info))
    when(ui.basePath).thenReturn(link)
    when(historyServer.getProviderConfig()).thenReturn(Map[String, String]())
    val page = new HistoryPage(historyServer)

    // when
    val response = page.render(request)

    // then
    val links = response \\ "a"
    val justHrefs = for {
      l <- links
      attrs <- l.attribute("href")
    } yield (attrs.toString)
    justHrefs should contain (UIUtils.prependBaseUri(resource = link))
  }

  test("relative links are prefixed with uiRoot (spark.ui.proxyBase)") {
    val proxyBaseBeforeTest = System.getProperty("spark.ui.proxyBase")
    val uiRoot = Option(System.getenv("APPLICATION_WEB_PROXY_BASE")).getOrElse("/testwebproxybase")
    val page = new HistoryPage(server)
    val request = mock[HttpServletRequest]

    // when
    System.setProperty("spark.ui.proxyBase", uiRoot)
    val response = page.render(request)
    System.setProperty("spark.ui.proxyBase", Option(proxyBaseBeforeTest).getOrElse(""))

    // then
    val urls = response \\ "@href" map (_.toString)
    val siteRelativeLinks = urls filter (_.startsWith("/"))
    all (siteRelativeLinks) should startWith (uiRoot)
  }

  def getContentAndCode(path: String, port: Int = port): (Int, Option[String], Option[String]) = {
    HistoryServerSuite.getContentAndCode(new URL(s"http://localhost:$port/api/v1/$path"))
  }

  def getUrl(path: String): String = {
    HistoryServerSuite.getUrl(generateURL(path))
  }

  def generateURL(path: String): URL = {
    new URL(s"http://localhost:$port/api/v1/$path")
  }

  def generateExpectation(name: String, path: String): Unit = {
    val json = getUrl(path)
    val file = new File(expRoot, HistoryServerSuite.sanitizePath(name) + "_expectation.json")
    val out = new FileWriter(file)
    out.write(json)
    out.close()
  }
}

object HistoryServerSuite {
  def main(args: Array[String]): Unit = {
    // generate the "expected" results for the characterization tests.  Just blindly assume the
    // current behavior is correct, and write out the returned json to the test/resource files

    val suite = new HistoryServerSuite
    FileUtils.deleteDirectory(suite.expRoot)
    suite.expRoot.mkdirs()
    try {
      suite.init()
      suite.cases.foreach { case (name, path) =>
        suite.generateExpectation(name, path)
      }
    } finally {
      suite.stop()
    }
  }

  def getContentAndCode(url: URL): (Int, Option[String], Option[String]) = {
    val (code, in, errString) = connectAndGetInputStream(url)
    val inString = in.map(IOUtils.toString)
    (code, inString, errString)
  }

  def connectAndGetInputStream(url: URL): (Int, Option[InputStream], Option[String]) = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()
    val code = connection.getResponseCode()
    val inStream = try {
      Option(connection.getInputStream())
    } catch {
      case io: IOException => None
    }
    val errString = try {
      val err = Option(connection.getErrorStream())
      err.map(IOUtils.toString)
    } catch {
      case io: IOException => None
    }
    (code, inStream, errString)
  }


  def sanitizePath(path: String): String = {
    // this doesn't need to be perfect, just good enough to avoid collisions
    path.replaceAll("\\W", "_")
  }

  def getUrl(path: URL): String = {
    val (code, resultOpt, error) = getContentAndCode(path)
    if (code == 200) {
      resultOpt.get
    } else {
      throw new RuntimeException(
        "got code: " + code + " when getting " + path + " w/ error: " + error)
    }
  }
}
