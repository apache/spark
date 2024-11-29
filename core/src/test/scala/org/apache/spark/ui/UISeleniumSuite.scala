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

import java.net.{URI, URL}
import java.util.Locale

import scala.io.Source
import scala.xml.Node

import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap
import org.htmlunit.DefaultCssErrorHandler
import org.htmlunit.cssparser.parser.CSSParseException
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.openqa.selenium.{By, WebDriver}
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.deploy.history.HistoryServerSuite
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Status._
import org.apache.spark.internal.config.UI._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.status.api.v1.{JacksonMessageWriter, RDDDataDistribution, StageStatus}
import org.apache.spark.tags.WebBrowserTest
import org.apache.spark.util.Utils

private[spark] class SparkUICssErrorHandler extends DefaultCssErrorHandler {

  /**
   * Some libraries have warn/error messages that are too noisy for the tests; exclude them from
   * normal error handling to avoid logging these.
   */
  private val cssExcludeList = List("bootstrap.min.css", "vis-timeline-graph2d.min.css")

  private def isInExcludeList(uri: String): Boolean = cssExcludeList.exists(uri.endsWith)

  override def warning(e: CSSParseException): Unit = {
    if (!isInExcludeList(e.getURI)) {
      super.warning(e)
    }
  }

  override def fatalError(e: CSSParseException): Unit = {
    if (!isInExcludeList(e.getURI)) {
      super.fatalError(e)
    }
  }

  override def error(e: CSSParseException): Unit = {
    if (!isInExcludeList(e.getURI)) {
      super.error(e)
    }
  }
}

/**
 * Selenium tests for the Spark Web UI.
 */
@WebBrowserTest
class UISeleniumSuite extends SparkFunSuite with WebBrowser with Matchers {

  implicit var webDriver: WebDriver = _
  implicit val formats: Formats = DefaultFormats


  override def beforeAll(): Unit = {
    super.beforeAll()
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (webDriver != null) {
        webDriver.quit()
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
   */
  private def newSparkContext(
      killEnabled: Boolean = true,
      timelineEnabled: Boolean = true,
      master: String = "local",
      additionalConfs: Map[String, String] = Map.empty): SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("test")
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
      .set(UI_KILL_ENABLED, killEnabled)
      .set(UI_TIMELINE_ENABLED, timelineEnabled)
      .set(MEMORY_OFFHEAP_SIZE.key, "64m")
    additionalConfs.foreach { case (k, v) => conf.set(k, v) }
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  test("all jobs page should be rendered even though we configure the scheduling mode to fair") {
    // Regression test for SPARK-33991
    val conf = Map("spark.scheduler.mode" -> "fair")
    withSpark(newSparkContext(additionalConfs = conf)) { sc =>
      val rdd = sc.parallelize(0 to 100, 100).repartition(10).cache()
      rdd.count()

      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        // The completed jobs table should have one row. The first row will be the most recent job:
        val firstRow = find(cssSelector("tbody tr")).get.underlying
        val firstRowColumns = firstRow.findElements(By.tagName("td"))
        // if first row can get the id 0, then the page is rendered and the scheduling mode is
        // displayed with no error when we visit http://localhost:4040/jobs/ even though
        // we configure the scheduling mode like spark.scheduler.mode=fair
        // instead of spark.scheculer.mode=FAIR
        firstRowColumns.get(0).getText should be ("0")
      }
    }
  }

  test("effects of unpersist() / persist() should be reflected") {
    // Regression test for SPARK-2527
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val rdd = sc.parallelize(Seq(1, 2, 3))
      rdd.persist(StorageLevels.DISK_ONLY).count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(ui, "/storage")
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(ui, "/storage/rdd/?id=0")
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }

      val storageJson = getJson(ui, "storage/rdd")
      storageJson.children.length should be (1)
      (storageJson.children.head \ "storageLevel").extract[String] should be (
        StorageLevels.DISK_ONLY.description)
      val rddJson = getJson(ui, "storage/rdd/0")
      (rddJson  \ "storageLevel").extract[String] should be (StorageLevels.DISK_ONLY.description)

      rdd.unpersist(blocking = true)
      rdd.persist(StorageLevels.MEMORY_ONLY).count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(ui, "/storage")
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(ui, "/storage/rdd/?id=0")
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }

      val updatedStorageJson = getJson(ui, "storage/rdd")
      updatedStorageJson.children.length should be (1)
      (updatedStorageJson.children.head \ "storageLevel").extract[String] should be (
        StorageLevels.MEMORY_ONLY.description)
      val updatedRddJson = getJson(ui, "storage/rdd/0")
      (updatedRddJson  \ "storageLevel").extract[String] should be (
        StorageLevels.MEMORY_ONLY.description)

      val dataDistributions0 =
        (updatedRddJson \ "dataDistribution").extract[Seq[RDDDataDistribution]]
      dataDistributions0.length should be (1)
      val dist0 = dataDistributions0.head

      dist0.onHeapMemoryUsed should not be (None)
      dist0.memoryUsed should be (dist0.onHeapMemoryUsed.get)
      dist0.onHeapMemoryRemaining should not be (None)
      dist0.offHeapMemoryRemaining should not be (None)
      dist0.memoryRemaining should be (
        dist0.onHeapMemoryRemaining.get + dist0.offHeapMemoryRemaining.get)
      dist0.onHeapMemoryUsed should not be (Some(0L))
      dist0.offHeapMemoryUsed should be (Some(0L))

      rdd.unpersist(blocking = true)
      rdd.persist(StorageLevels.OFF_HEAP).count()
      val updatedStorageJson1 = getJson(ui, "storage/rdd")
      updatedStorageJson1.children.length should be (1)
      val updatedRddJson1 = getJson(ui, "storage/rdd/0")
      val dataDistributions1 =
        (updatedRddJson1 \ "dataDistribution").extract[Seq[RDDDataDistribution]]
      dataDistributions1.length should be (1)
      val dist1 = dataDistributions1.head

      dist1.offHeapMemoryUsed should not be (None)
      dist1.memoryUsed should be (dist1.offHeapMemoryUsed.get)
      dist1.onHeapMemoryRemaining should not be (None)
      dist1.offHeapMemoryRemaining should not be (None)
      dist1.memoryRemaining should be (
        dist1.onHeapMemoryRemaining.get + dist1.offHeapMemoryRemaining.get)
      dist1.onHeapMemoryUsed should be (Some(0L))
      dist1.offHeapMemoryUsed should not be (Some(0L))
    }
  }

  test("failed stages should not appear to be active") {
    withSpark(newSparkContext()) { sc =>
      // Regression test for SPARK-3021
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => throw new Exception()}.collect()
      }
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        find(id("active")) should be(None)  // Since we hide empty tables
        find(id("failed")).get.text should be("Failed Stages (1)")
      }
      val stageJson = getJson(sc.ui.get, "stages")
      stageJson.children.length should be (1)
      (stageJson.children.head \ "status").extract[String] should be (StageStatus.FAILED.name())

      // Regression test for SPARK-2105
      class NotSerializable
      val unserializableObject = new NotSerializable
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => unserializableObject}.collect()
      }
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        find(id("active")) should be(None)  // Since we hide empty tables
        // The failure occurs before the stage becomes active, hence we should still show only one
        // failed stage, not two:
        find(id("failed")).get.text should be("Failed Stages (1)")
      }

      val updatedStageJson = getJson(sc.ui.get, "stages")
      updatedStageJson should be (stageJson)
    }
  }

  test("spark.ui.killEnabled should properly control kill button display") {
    def hasKillLink: Boolean = find(className("kill-link")).isDefined
    def runSlowJob(sc: SparkContext): Unit = {
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
    }

    withSpark(newSparkContext(killEnabled = true)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        assert(hasKillLink)
      }
    }

    withSpark(newSparkContext(killEnabled = false)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        assert(!hasKillLink)
      }
    }

    withSpark(newSparkContext(killEnabled = true)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        assert(hasKillLink)
      }
    }

    withSpark(newSparkContext(killEnabled = false)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        assert(!hasKillLink)
      }
    }
  }

  test("jobs page should not display job group name unless some job was submitted in a job group") {
    withSpark(newSparkContext()) { sc =>
      // If no job has been run in a job group, then "(Job Group)" should not appear in the header
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        tableHeaders(0) should not startWith "Job Id (Job Group)"
      }
      // Once at least one job has been run in a job group, then we should display the group name:
      sc.setJobGroup("my-job-group", "my-job-group-description")
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        // Can suffix up/down arrow in the header
        tableHeaders(0) should startWith ("Job Id (Job Group)")
      }

      val jobJson = getJson(sc.ui.get, "jobs")
      for {
        job @ JObject(_) <- jobJson
        JInt(jobId) <- job \ "jobId"
        jobGroup = job \ "jobGroup"
      } {
        jobId.toInt match {
          case 0 => jobGroup should be (JNothing)
          case 1 => jobGroup should be (JString("my-job-group"))
        }
      }
    }
  }

  test("job progress bars should handle stage / task failures") {
    withSpark(newSparkContext()) { sc =>
      val data = sc.parallelize(Seq(1, 2, 3), 1).map(identity).groupBy(identity)
      val shuffleHandle =
        data.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      // Simulate fetch failures:
      val mappedData = data.map { x =>
        val taskContext = TaskContext.get()
        if (taskContext.taskAttemptId() == 1) {
          // Cause the post-shuffle stage to fail on its first attempt with a single task failure
          val env = SparkEnv.get
          val bmAddress = env.blockManager.blockManagerId
          val shuffleId = shuffleHandle.shuffleId
          val mapId = 0L
          val mapIndex = 0
          val reduceId = taskContext.partitionId()
          val message = "Simulated fetch failure"
          throw new FetchFailedException(
            bmAddress, shuffleId, mapId, mapIndex, reduceId, message)
        } else {
          x
        }
      }
      mappedData.count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        find(cssSelector(".stage-progress-cell")).get.text should be ("2/2 (1 failed)")
        find(cssSelector(".progress-cell .progress")).get.text should be ("2/2 (1 failed)")
      }
      val jobJson = getJson(sc.ui.get, "jobs")
      (jobJson \\ "numTasks").extract[Int]should be (2)
      (jobJson \\ "numCompletedTasks").extract[Int] should be (3)
      (jobJson \\ "numFailedTasks").extract[Int] should be (1)
      (jobJson \\ "numCompletedStages").extract[Int] should be (2)
      (jobJson \\ "numFailedStages").extract[Int] should be (1)
      val stageJson = getJson(sc.ui.get, "stages")

      for {
        stage @ JObject(_) <- stageJson
        JString(status) <- stage \ "status"
        JInt(stageId) <- stage \ "stageId"
        JInt(attemptId) <- stage \ "attemptId"
      } {
        val exp = if (attemptId.toInt == 0 && stageId.toInt == 1) {
          StageStatus.FAILED
        } else {
          StageStatus.COMPLETE
        }
        status should be (exp.name())
      }

      for {
        stageId <- 0 to 1
        attemptId <- 0 to 1
      } {
        val exp = if (attemptId == 0 && stageId == 1) StageStatus.FAILED else StageStatus.COMPLETE
        val stageJson = getJson(sc.ui.get, s"stages/$stageId/$attemptId")
        (stageJson \ "status").extract[String] should be (exp.name())
      }
    }
  }

  test("job details page should display useful information for stages that haven't started") {
    withSpark(newSparkContext()) { sc =>
      // Create a multi-stage job with a long delay in the first stage:
      val rdd = sc.parallelize(Seq(1, 2, 3)).map { x =>
        // This long sleep call won't slow down the tests because we don't actually need to wait
        // for the job to finish.
        Thread.sleep(20000)
      }.groupBy(identity).map(identity).groupBy(identity).map(identity)
      // Start the job:
      rdd.countAsync()
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs/job/?id=0")
        find(id("active")).get.text should be ("Active Stages (1)")
        find(id("pending")).get.text should be ("Pending Stages (2)")
        // Essentially, we want to check that none of the stage rows show
        // "No data available for this stage". Checking for the absence of that string is brittle
        // because someone could change the error message and cause this test to pass by accident.
        // Instead, it's safer to check that each row contains a link to a stage details page.
        findAll(cssSelector("tbody tr")).foreach { row =>
          val link = row.underlying.findElement(By.xpath("./td/div/a"))
          link.getAttribute("href") should include ("stage")
        }
      }
    }
  }

  test("job progress bars / cells reflect skipped stages / tasks") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      val rdd = sc.parallelize(1 to 8, 8)
        .map(x => x).groupBy((x: Int) => x, numPartitions = 8)
        .flatMap(x => x._2).groupBy((x: Int) => x, numPartitions = 8)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      rdd.count()
      rdd.count()
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        // The completed jobs table should have two rows. The first row will be the most recent job:
        val firstRow = find(cssSelector("tbody tr")).get.underlying
        val firstRowColumns = firstRow.findElements(By.tagName("td"))
        firstRowColumns.get(0).getText should be ("1")
        firstRowColumns.get(4).getText should be ("1/1 (2 skipped)")
        firstRowColumns.get(5).getText should be ("8/8 (16 skipped)")
        // The second row is the first run of the job, where nothing was skipped:
        val secondRow = findAll(cssSelector("tbody tr")).toSeq(1).underlying
        val secondRowColumns = secondRow.findElements(By.tagName("td"))
        secondRowColumns.get(0).getText should be ("0")
        secondRowColumns.get(4).getText should be ("3/3")
        secondRowColumns.get(5).getText should be ("24/24")
      }
    }
  }

  test("stages that aren't run appear as 'skipped stages' after a job finishes") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      val rdd =
        sc.parallelize(Seq(1, 2, 3)).map(identity).groupBy(identity).map(identity).groupBy(identity)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      rdd.count()
      rdd.count()
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs/job/?id=1")
        find(id("pending")) should be (None)
        find(id("active")) should be (None)
        find(id("failed")) should be (None)
        find(id("completed")).get.text should be ("Completed Stages (1)")
        find(id("skipped")).get.text should be ("Skipped Stages (2)")
        // Essentially, we want to check that none of the stage rows show
        // "No data available for this stage". Checking for the absence of that string is brittle
        // because someone could change the error message and cause this test to pass by accident.
        // Instead, it's safer to check that each row contains a link to a stage details page.
        findAll(cssSelector("tbody tr")).foreach { row =>
          val link = row.underlying.findElement(By.xpath(".//a"))
          link.getAttribute("href") should include ("stage")
        }
      }
    }
  }

  test("jobs with stages that are skipped should show correct link descriptions on all jobs page") {
    withSpark(newSparkContext()) { sc =>
      // Create an RDD that involves multiple stages:
      val rdd =
        sc.parallelize(Seq(1, 2, 3)).map(identity).groupBy(identity).map(identity).groupBy(identity)
      // Run it twice; this will cause the second job to have two "phantom" stages that were
      // mentioned in its job start event but which were never actually executed:
      rdd.count()
      rdd.count()
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        findAll(cssSelector("tbody tr a")).foreach { link =>
          link.text.toLowerCase(Locale.ROOT) should include ("count")
          link.text.toLowerCase(Locale.ROOT) should not include "unknown"
        }
      }
    }
  }

  test("attaching and detaching a new tab") {
    withSpark(newSparkContext()) { sc =>
      val sparkUI = sc.ui.get

      val newTab = new WebUITab(sparkUI, "foo") {
        attachPage(new WebUIPage("") {
          def render(request: HttpServletRequest): Seq[Node] = {
            <b>"html magic"</b>
          }
        })
      }
      sparkUI.attachTab(newTab)
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "")
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should not be(None)
      }
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        // check whether new page exists
        goToUi(sc, "/foo")
        find(cssSelector("b")).get.text should include ("html magic")
      }
      sparkUI.detachTab(newTab)
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "")
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should be(None)
      }
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        // check new page not exist
        goToUi(sc, "/foo")
        find(cssSelector("b")) should be(None)
      }
    }
  }

  test("kill stage POST/GET response is correct") {
    withSpark(newSparkContext(killEnabled = true)) { sc =>
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        val url = new URI(
          sc.ui.get.webUrl.stripSuffix("/") + "/stages/stage/kill/?id=0").toURL
        // SPARK-6846: should be POST only but YARN AM doesn't proxy POST
        TestUtils.httpResponseCode(url, "GET") should be (200)
        TestUtils.httpResponseCode(url, "POST") should be (200)
      }
    }
  }

  test("kill job POST/GET response is correct") {
    withSpark(newSparkContext(killEnabled = true)) { sc =>
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        val url = new URI(
          sc.ui.get.webUrl.stripSuffix("/") + "/jobs/job/kill/?id=0").toURL
        // SPARK-6846: should be POST only but YARN AM doesn't proxy POST
        TestUtils.httpResponseCode(url, "GET") should be (200)
        TestUtils.httpResponseCode(url, "POST") should be (200)
      }
    }
  }

  test("stage & job retention") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
      .set(MAX_RETAINED_STAGES, 3)
      .set(MAX_RETAINED_JOBS, 2)
      .set(ASYNC_TRACKING_ENABLED, false)
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)

    withSpark(sc) { sc =>
      // run a few jobs & stages ...
      (0 until 5).foreach { idx =>
        // NOTE: if we reverse the order, things don't really behave nicely
        // we lose the stage for a job we keep, and then the job doesn't know
        // about its last stage
        sc.parallelize(idx to (idx + 3)).map(identity).groupBy(identity).map(identity)
          .groupBy(identity).count()
        sc.parallelize(idx to (idx + 3)).collect()
      }

      val expJobInfo = Seq(
        ("9", "collect"),
        ("8", "count")
      )

      eventually(timeout(1.second), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        // The completed jobs table should have two rows. The first row will be the most recent job:
        find("completed-summary").get.text should be ("Completed Jobs: 10, only showing 2")
        find("completed").get.text should be ("Completed Jobs (10, only showing 2)")
        val rows = findAll(cssSelector("tbody tr")).toIndexedSeq.map{_.underlying}
        rows.size should be (expJobInfo.size)
        for {
          (row, idx) <- rows.zipWithIndex
          columns = row.findElements(By.tagName("td"))
          id = columns.get(0).getText()
          description = columns.get(1).getText()
        } {
          id should be (expJobInfo(idx)._1)
          description should include (expJobInfo(idx)._2)
        }
      }

      val jobsJson = getJson(sc.ui.get, "jobs")
      jobsJson.children.size should be (expJobInfo.size)
      for {
        (job @ JObject(_), idx) <- jobsJson.children.zipWithIndex
        id = (job \ "jobId").extract[String]
        name = (job \ "name").extract[String]
      } {
        withClue(s"idx = $idx; id = $id; name = ${name.substring(0, 20)}") {
          id should be (expJobInfo(idx)._1)
          name should include (expJobInfo(idx)._2)
        }
      }

      // what about when we query for a job that did exist, but has been cleared?
      goToUi(sc, "/jobs/job/?id=7")
      find("no-info").get.text should be ("No information to display for job 7")

      val badJob = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "jobs/7"))
      badJob._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badJob._2 should be (None)
      badJob._3 should be (Some("unknown job: 7"))

      val expStageInfo = Seq(
        ("19", "collect"),
        ("18", "count"),
        ("17", "groupBy")
      )

      eventually(timeout(1.second), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        find("completed-summary").get.text should be ("Completed Stages: 20, only showing 3")
        find("completed").get.text should be ("Completed Stages (20, only showing 3)")
        val rows = findAll(cssSelector("tbody tr")).toIndexedSeq.map{_.underlying}
        rows.size should be (3)
        for {
          (row, idx) <- rows.zipWithIndex
          columns = row.findElements(By.tagName("td"))
          id = columns.get(0).getText()
          description = columns.get(1).getText()
        } {
          id should be (expStageInfo(idx)._1)
          description should include (expStageInfo(idx)._2)
        }
      }

      val stagesJson = getJson(sc.ui.get, "stages")
      stagesJson.children.size should be (3)
      for {
        (stage @ JObject(_), idx) <- stagesJson.children.zipWithIndex
        id = (stage \ "stageId").extract[String]
        name = (stage \ "name").extract[String]
      } {
        id should be (expStageInfo(idx)._1)
        name should include (expStageInfo(idx)._2)
      }

      // nonexistent stage

      goToUi(sc, "/stages/stage/?id=12&attempt=0")
      find("no-info").get.text should be ("No information to display for Stage 12 (Attempt 0)")
      val badStage = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "stages/12/0"))
      badStage._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badStage._2 should be (None)
      badStage._3 should be (Some("unknown stage: 12"))

      val badAttempt = HistoryServerSuite.getContentAndCode(apiUrl(sc.ui.get, "stages/19/15"))
      badAttempt._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badAttempt._2 should be (None)
      badAttempt._3 should be (Some("unknown attempt for stage 19.  Found attempts: [0]"))

      val badStageAttemptList = HistoryServerSuite.getContentAndCode(
        apiUrl(sc.ui.get, "stages/12"))
      badStageAttemptList._1 should be (HttpServletResponse.SC_NOT_FOUND)
      badStageAttemptList._2 should be (None)
      badStageAttemptList._3 should be (Some("unknown stage: 12"))
    }
  }

  test("live UI json application list") {
    withSpark(newSparkContext()) { sc =>
      val appListRawJson = HistoryServerSuite.getUrl(new URI(
        sc.ui.get.webUrl + "/api/v1/applications").toURL)
      val appListJsonAst = JsonMethods.parse(appListRawJson)
      appListJsonAst.children.length should be (1)
      val attempts = (appListJsonAst.children.head \ "attempts").children
      attempts.size should be (1)
      (attempts(0) \ "completed").extract[Boolean] should be (false)
      parseDate(attempts(0) \ "startTime") should be (sc.startTime)
      parseDate(attempts(0) \ "endTime") should be (-1)
      val oneAppJsonAst = getJson(sc.ui.get, "")
      val duration = attempts(0) \ "duration"
      oneAppJsonAst \\ "duration" should not be duration
      // SPARK-42697: duration will increase as the app is running
      // Replace the duration before we compare the full JObjects
      val durationAdjusted = oneAppJsonAst.transformField {
        case ("duration", _) => ("duration", duration)
      }
      durationAdjusted should be (appListJsonAst.children(0))
    }
  }

  test("job stages should have expected dotfile under DAG visualization") {
    withSpark(newSparkContext()) { sc =>
      // Create a multi-stage job
      val rdd =
        sc.parallelize(Seq(1, 2, 3)).map(identity).groupBy(identity).map(identity).groupBy(identity)
      rdd.count()

      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        val stage0 = Utils.tryWithResource(Source.fromURL(sc.ui.get.webUrl +
          "/stages/stage/?id=0&attempt=0&expandDagViz=true"))(_.mkString)
        assert(stage0.contains("""digraph G {
                                 |  id=&quot;graph_0&quot;;
                                 |  subgraph graph_stage_0 {
                                 |    id=&quot;graph_stage_0&quot;;
                                 |    isCluster=&quot;true&quot;;
                                 |    label=&quot;Stage 0&quot;;""".stripMargin))
        assert(stage0.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;parallelize&quot;;
                                 |      0 [id=&quot;node_0&quot;""".stripMargin))
        assert(stage0.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;map&quot;;
                                 |      1 [id=&quot;node_1&quot;""".stripMargin))
        assert(stage0.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;groupBy&quot;;
                                 |      2 [id=&quot;node_2&quot;""".stripMargin))

        val stage1 = Utils.tryWithResource(Source.fromURL(sc.ui.get.webUrl +
          "/stages/stage/?id=1&attempt=0&expandDagViz=true"))(_.mkString)
        assert(stage1.contains("""digraph G {
                                 |  id=&quot;graph_1&quot;;
                                 |  subgraph graph_stage_1 {
                                 |    id=&quot;graph_stage_1&quot;;
                                 |    isCluster=&quot;true&quot;;
                                 |    label=&quot;Stage 1&quot;;""".stripMargin))
        assert(stage1.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;groupBy&quot;;""".stripMargin))
        assert(stage1.contains(
          "3 [id=&quot;node_3&quot; labelType=&quot;html&quot; label=&quot;ShuffledRDD"))
        assert(stage1.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;map&quot;;""".stripMargin))
        assert(stage1.contains(
          "4 [id=&quot;node_4&quot; labelType=&quot;html&quot; label=&quot;MapPartitionsRDD [4]"))

        val stage2 = Utils.tryWithResource(Source.fromURL(sc.ui.get.webUrl +
          "/stages/stage/?id=2&attempt=0&expandDagViz=true"))(_.mkString)
        assert(stage2.contains("""digraph G {
                                 |  id=&quot;graph_2&quot;;
                                 |  subgraph graph_stage_2 {
                                 |    id=&quot;graph_stage_2&quot;;
                                 |    isCluster=&quot;true&quot;;
                                 |    label=&quot;Stage 2&quot;;""".stripMargin))
        assert(stage2.contains("""
                                 |      isCluster=&quot;true&quot;;
                                 |      label=&quot;groupBy&quot;;""".stripMargin))
        assert(stage2.contains(
          "6 [id=&quot;node_6&quot; labelType=&quot;html&quot; label=&quot;ShuffledRDD [6]"))
      }
    }
  }

  test("stages page should show skipped stages") {
    withSpark(newSparkContext()) { sc =>
      val rdd = sc.parallelize(0 to 100, 100).repartition(10).cache()
      rdd.count()
      rdd.count()

      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/stages")
        find(id("skipped")).get.text should be("Skipped Stages (1)")
      }
      val stagesJson = getJson(sc.ui.get, "stages")
      stagesJson.children.size should be (4)
      val stagesStatus = stagesJson.children.map(_ \ "status")
      stagesStatus.count(_ == JString(StageStatus.SKIPPED.name())) should be (1)
    }
  }

  test("Staleness of Spark UI should not last minutes or hours") {
    withSpark(newSparkContext(
      master = "local[2]",
      // Set a small heart beat interval to make the test fast
      additionalConfs = Map(
        EXECUTOR_HEARTBEAT_INTERVAL.key -> "10ms",
        LIVE_ENTITY_UPDATE_MIN_FLUSH_PERIOD.key -> "10ms"))) { sc =>
      sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "true")
      val f = sc.parallelize(1 to 1000, 1000).foreachAsync { _ =>
        // Make the task never finish so there won't be any task start/end events after the first 2
        // tasks start.
        Thread.sleep(300000)
      }
      try {
        eventually(timeout(10.seconds)) {
          val jobsJson = getJson(sc.ui.get, "jobs")
          jobsJson.children.length should be (1)
          (jobsJson.children.head \ "numActiveTasks").extract[Int] should be (2)
        }
      } finally {
        f.cancel()
      }
    }
  }

  test("description for empty jobs") {
    withSpark(newSparkContext()) { sc =>
      sc.emptyRDD[Int].collect()
      val description = "This is my job"
      sc.setJobDescription(description)
      sc.emptyRDD[Int].collect()

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")
        val descriptions = findAll(className("description-input")).toArray
        descriptions(0).text should be (description)
        descriptions(1).text should include ("collect")
      }
    }
  }

  test("Support disable event timeline") {
    Seq(true, false).foreach { timelineEnabled =>
      withSpark(newSparkContext(timelineEnabled = timelineEnabled)) { sc =>
        sc.range(1, 3).collect()
        eventually(timeout(10.seconds), interval(50.milliseconds)) {
          goToUi(sc, "/jobs")
          assert(findAll(className("expand-application-timeline")).nonEmpty === timelineEnabled)

          goToUi(sc, "/jobs/job/?id=0")
          assert(findAll(className("expand-job-timeline")).nonEmpty === timelineEnabled)

          goToUi(sc, "/stages/stage/?id=0&attempt=0")
          assert(findAll(className("expand-task-assignment-timeline")).nonEmpty === timelineEnabled)
        }
      }
    }
  }

  test("SPARK-41365: Stage page can be accessed if URI was encoded twice") {
    withSpark(newSparkContext()) { sc =>
      val rdd = sc.parallelize(0 to 10, 10).repartition(10)
      rdd.count()
      eventually(timeout(5.seconds), interval(50.milliseconds)) {
        val encodeParams = new MultivaluedStringMap
        encodeParams.add("order%255B0%255D%255Bcolumn%255D", "Locality%2520Level")
        encodeParams.add("order%255B0%255D%255Bcolumn%255D", "Executor%2520ID")
        encodeParams.add("search%255Bvalue%255D", null)
        val decodeParams = UIUtils.decodeURLParameter(encodeParams)
        // assert no change in order
        assert(decodeParams.getFirst("order[0][column]").equals("Locality Level"))
        assert(decodeParams.get("order[0][column]").size() == 2)
        assert(decodeParams.getFirst("search[value]").equals(""))

        val decodeQuery = "draw=2&order[0][column]=4&order[0][dir]=asc&start=0&length=20" +
          "&search[value]=&search[regex]=false&numTasks=10&columnIndexToSort=4" +
          "&columnNameToSort=Locality Level"
        val encodeOnceQuery = "draw=2&order%5B0%5D%5Bcolumn%5D=4&start=0&length=20" +
          "&search%5Bvalue%5D=&search%5Bregex%5D=false&numTasks=10&columnIndexToSort=4" +
          "&columnNameToSort=Locality%20Level"
        val encodeTwiceQuery = "draw=2&order%255B0%255D%255Bcolumn%255D=4&start=0&length=20" +
          "&search%255Bvalue%255D=&search%255Bregex%255D=false&numTasks=10&columnIndexToSort=4" +
          "&columnNameToSort=Locality%2520Level"
        val encodeOnceRes = Utils.tryWithResource(Source.fromURL(
          apiUrl(sc.ui.get, "stages/0/0/taskTable?" + encodeOnceQuery)))(_.mkString)
        val encodeTwiceRes = Utils.tryWithResource(Source.fromURL(
          apiUrl(sc.ui.get, "stages/0/0/taskTable?" + encodeTwiceQuery)))(_.mkString)
        assert(encodeOnceRes.equals(encodeTwiceRes))
      }
    }
  }

  test("SPARK-44895: Add 'daemon', 'priority' for ThreadStackTrace") {
    withSpark(newSparkContext()) { sc =>
      val uiThreads = getJson(sc.ui.get, "executors/driver/threads")
        .children
        .filter(v => (v \ "threadName").extract[String].matches("SparkUI-\\d+"))
      val priority = Thread.currentThread().getPriority

      uiThreads.foreach { v =>
        assert((v \ "isDaemon").extract[Boolean])
        assert((v \ "priority").extract[Int] === priority)
      }
    }
  }

  def goToUi(sc: SparkContext, path: String): Unit = {
    goToUi(sc.ui.get, path)
  }

  def goToUi(ui: SparkUI, path: String): Unit = {
    go to (ui.webUrl.stripSuffix("/") + path)
  }

  def parseDate(json: JValue): Long = {
    JacksonMessageWriter.makeISODateFormat.parse(json.extract[String]).getTime
  }

  def getJson(ui: SparkUI, path: String): JValue = {
    JsonMethods.parse(HistoryServerSuite.getUrl(apiUrl(ui, path)))
  }

  def apiUrl(ui: SparkUI, path: String): URL = {
    new URI(ui.webUrl + "/api/v1/applications/" + ui.sc.get.applicationId + "/" + path).toURL
  }
}
