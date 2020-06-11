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

import java.util.{List => JList}
import java.util.regex._

import scala.collection.JavaConverters._
import scala.util.Try

import org.openqa.selenium.{By, JavascriptExecutor, WebDriver}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.MEMORY_OFFHEAP_SIZE
import org.apache.spark.internal.config.UI.{UI_ENABLED, UI_KILL_ENABLED, UI_PORT}
import org.apache.spark.util.CallSite

/**
 * Selenium tests for the Spark Web UI with real web browsers.
 */
abstract class RealBrowserUISeleniumSuite(val driverProp: String)
  extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver with JavascriptExecutor
  private val driverPropPrefix = "spark.test."

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      sys.props(driverPropPrefix + driverProp) !== null,
      "System property " + driverPropPrefix + driverProp +
        " should be set to the corresponding driver path.")
    sys.props(driverProp) = sys.props(driverPropPrefix + driverProp)
  }

  override def afterAll(): Unit = {
    sys.props.remove(driverProp)
    super.afterAll()
  }

  test("SPARK-31534: text for tooltip should be escaped") {
    withSpark(newSparkContext()) { sc =>
      sc.setLocalProperty(CallSite.LONG_FORM, "collect at <console>:25")
      sc.setLocalProperty(CallSite.SHORT_FORM, "collect at <console>:25")
      sc.parallelize(1 to 10).collect

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs")

        val jobDesc =
          webDriver.findElement(By.cssSelector("div[class='application-timeline-content']"))
        jobDesc.getAttribute("data-title") should include  ("collect at &lt;console&gt;:25")

        goToUi(sc, "/jobs/job/?id=0")
        webDriver.get(sc.ui.get.webUrl.stripSuffix("/") + "/jobs/job/?id=0")
        val stageDesc = webDriver.findElement(By.cssSelector("div[class='job-timeline-content']"))
        stageDesc.getAttribute("data-title") should include ("collect at &lt;console&gt;:25")

        // Open DAG Viz.
        webDriver.findElement(By.id("job-dag-viz")).click()
        val nodeDesc = webDriver.findElement(By.cssSelector("g[class='node_0 node']"))
        nodeDesc.getAttribute("name") should include ("collect at &lt;console&gt;:25")
      }
    }
  }

  test("SPARK-31882: Link URL for Stage DAGs should not depend on paged table.") {
    withSpark(newSparkContext()) { sc =>
      sc.parallelize(1 to 100).map(v => (v, v)).repartition(10).reduceByKey(_ + _).collect

      eventually(timeout(10.seconds), interval(50.microseconds)) {
        val pathWithPagedTable =
          "/jobs/job/?id=0&completedStage.page=2&completedStage.sort=Stage+Id&" +
            "completedStage.desc=true&completedStage.pageSize=1#completed"
        goToUi(sc, pathWithPagedTable)

        // Open DAG Viz.
        webDriver.findElement(By.id("job-dag-viz")).click()
        val stages = webDriver.findElements(By.cssSelector("svg[class='job'] > a"))
        stages.size() should be (3)

        stages.get(0).getAttribute("href") should include ("/stages/stage/?id=0&attempt=0")
        stages.get(1).getAttribute("href") should include ("/stages/stage/?id=1&attempt=0")
        stages.get(2).getAttribute("href") should include ("/stages/stage/?id=2&attempt=0")
      }
    }
  }

  test("SPARK-31886: Color barrier execution mode RDD correctly") {
    withSpark(newSparkContext()) { sc =>
      sc.parallelize(1 to 10).barrier.mapPartitions(identity).repartition(1).collect()

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs/job/?id=0")
        webDriver.findElement(By.id("job-dag-viz")).click()

        val stage0 = webDriver.findElement(By.cssSelector("g[id='graph_0']"))
        val stage1 = webDriver.findElement(By.cssSelector("g[id='graph_1']"))
        val barrieredOps = webDriver.findElements(By.className("barrier-rdd")).iterator()

        while (barrieredOps.hasNext) {
          val barrieredOpId = barrieredOps.next().getAttribute("innerHTML")
          val foundInStage0 =
            stage0.findElements(
              By.cssSelector("g.barrier.cluster.cluster_" + barrieredOpId))
          assert(foundInStage0.size === 1)

          val foundInStage1 =
            stage1.findElements(
              By.cssSelector("g.barrier.cluster.cluster_" + barrieredOpId))
          assert(foundInStage1.size === 0)
        }
      }
    }
  }

  test("Pagination for all jobs timeline") {
    val totalNumOfJobs = 301
    withSpark(newSparkContext()) { sc =>
      (0 until totalNumOfJobs).foreach { index =>
        // Just ignore exception.
        Try {
          sc.parallelize(1 to 10).foreach { _ =>
            if (index == 0) {
              // Mark the first job fail.
              throw new RuntimeException()
            }
          }
        }
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        goToUi(sc, "/jobs")
        webDriver.findElement(By.cssSelector("span.expand-application-timeline")).click()

        // The number of initially displayed jobs.
        val displayedJobs1 = getJobContents()
        displayedJobs1.size should be (100)

        // Jobs are sorted by submission time (equivalent to Job ID in this case)
        // regardless of whether jobs are completed or not.
        val pattern = Pattern.compile("""^.*\(Job (\d+)\)$""")
        displayedJobs1.sliding(2).foreach { case Seq(content1, content2) =>
          val matcher1 = pattern.matcher(content1)
          matcher1.find()
          val jobId1 = matcher1.group(1).toInt

          val matcher2 = pattern.matcher(content2)
          matcher2.find()
          val jobId2 = matcher2.group(1).toInt

          jobId1 should be < jobId2
        }

        goToPage(pageNo = 3, pageSize = 30)
        val displayedJobs2 = getJobContents()
        displayedJobs2.size should be (30)

        // If designated pageNo exceeds the num of total pages or < 1,
        // the pageNo is set to 1 but pageSize is kept.
        goToPage(pageNo = 100, pageSize = 50)
        val displayedJobs3 = getJobContents()
        displayedJobs3.length should be (50)
        displayedJobs3.indices.foreach { index =>
          displayedJobs3(index) should be (displayedJobs1.slice(0, 50)(index))
        }
        goToPage(pageNo = -1, pageSize = 10)
        val displayedJobs4 = getJobContents()
        displayedJobs4.indices.foreach { index =>
          displayedJobs4(index) should be (displayedJobs1.slice(0, 10)(index))
        }

        // If designated pageSize exceeds the num of jobs or < 1,
        // the pageSize is set to the num of jobs.
        goToPage(pageNo = 2, pageSize = 10000)
        val displayedJobs5 = getJobContents()
        displayedJobs5.size should be (totalNumOfJobs)
        goToPage(pageNo = 5, pageSize = 0)
        val displayedJobs6 = getJobContents()
        displayedJobs6.size should be (totalNumOfJobs)

        // The num of jobs in the last page at a pageSize can be < pageSize
        goToPage(pageNo = 4, pageSize = 100)
        val displayedJobs7 = getJobContents()
        displayedJobs7.size should be (totalNumOfJobs % 100)
      }
    }

    // WebElement#sendKeys cannot work on macOS with ChromeDriver so executeScript is used.
    def setPageNo(pageNo: Int): Unit = {
      webDriver.executeScript(s"$$('#form-event-timeline-page-no').attr('value', '$pageNo');")
    }

    def setPageSize(pageSize: Int): Unit = {
      webDriver.executeScript(s"$$('#form-event-timeline-page-size').attr('value', '$pageSize');")
    }

    // If text fields are filled by executeScript, Go button should be pushed by the method.
    def pushGoButton(): Unit = {
      webDriver.executeScript("$('#form-event-timeline-page-button').click();")
    }

    def getJobContents(): Seq[String] = {
      webDriver.executeScript(
        """
          return $('.vis-item.job').map(function(_, e) {
            return e.textContent;
          });
        """).asInstanceOf[JList[String]].asScala
    }

    def goToPage(pageNo: Int, pageSize: Int): Unit = {
      setPageNo(pageNo)
      setPageSize(pageSize)
      pushGoButton()
    }
  }

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
   */
  private def newSparkContext(
      killEnabled: Boolean = true,
      master: String = "local",
      additionalConfs: Map[String, String] = Map.empty): SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("test")
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
      .set(UI_KILL_ENABLED, killEnabled)
      .set(MEMORY_OFFHEAP_SIZE.key, "64m")
    additionalConfs.foreach { case (k, v) => conf.set(k, v) }
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  def goToUi(sc: SparkContext, path: String): Unit = {
    goToUi(sc.ui.get, path)
  }

  def goToUi(ui: SparkUI, path: String): Unit = {
    go to (ui.webUrl.stripSuffix("/") + path)
  }
}
