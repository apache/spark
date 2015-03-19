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

import scala.collection.JavaConversions._
import scala.xml.Node

import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.{By, WebDriver}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.SpanSugar._

import org.apache.spark.LocalSparkContext._
import org.apache.spark._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.shuffle.FetchFailedException


/**
 * Selenium tests for the Spark Web UI.
 */
class UISeleniumSuite extends FunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _

  override def beforeAll(): Unit = {
    webDriver = new HtmlUnitDriver
  }

  override def afterAll(): Unit = {
    if (webDriver != null) {
      webDriver.quit()
    }
  }

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
   */
  private def newSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "0")
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  test("effects of unpersist() / persist() should be reflected") {
    // Regression test for SPARK-2527
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val rdd = sc.parallelize(Seq(1, 2, 3))
      rdd.persist(StorageLevels.DISK_ONLY).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (ui.appUIAddress.stripSuffix("/") + "/storage")
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (ui.appUIAddress.stripSuffix("/") + "/storage/rdd/?id=0")
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.DISK_ONLY.description)
      }

      rdd.unpersist()
      rdd.persist(StorageLevels.MEMORY_ONLY).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (ui.appUIAddress.stripSuffix("/") + "/storage")
        val tableRowText = findAll(cssSelector("#storage-by-rdd-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (ui.appUIAddress.stripSuffix("/") + "/storage/rdd/?id=0")
        val tableRowText = findAll(cssSelector("#rdd-storage-by-block-table td")).map(_.text).toSeq
        tableRowText should contain (StorageLevels.MEMORY_ONLY.description)
      }
    }
  }

  test("failed stages should not appear to be active") {
    withSpark(newSparkContext()) { sc =>
      // Regression test for SPARK-3021
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => throw new Exception()}.collect()
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/stages")
        find(id("active")) should be(None)  // Since we hide empty tables
        find(id("failed")).get.text should be("Failed Stages (1)")
      }

      // Regression test for SPARK-2105
      class NotSerializable
      val unserializableObject = new NotSerializable
      intercept[SparkException] {
        sc.parallelize(1 to 10).map { x => unserializableObject}.collect()
      }
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/stages")
        find(id("active")) should be(None)  // Since we hide empty tables
        // The failure occurs before the stage becomes active, hence we should still show only one
        // failed stage, not two:
        find(id("failed")).get.text should be("Failed Stages (1)")
      }
    }
  }

  test("spark.ui.killEnabled should properly control kill button display") {
    def getSparkContext(killEnabled: Boolean): SparkContext = {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("test")
        .set("spark.ui.enabled", "true")
        .set("spark.ui.killEnabled", killEnabled.toString)
      new SparkContext(conf)
    }

    def hasKillLink = find(className("kill-link")).isDefined
    def runSlowJob(sc: SparkContext) {
      sc.parallelize(1 to 10).map{x => Thread.sleep(10000); x}.countAsync()
    }

    withSpark(getSparkContext(killEnabled = true)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/stages")
        assert(hasKillLink)
      }
    }

    withSpark(getSparkContext(killEnabled = false)) { sc =>
      runSlowJob(sc)
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/stages")
        assert(!hasKillLink)
      }
    }
  }

  test("jobs page should not display job group name unless some job was submitted in a job group") {
    withSpark(newSparkContext()) { sc =>
      // If no job has been run in a job group, then "(Job Group)" should not appear in the header
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        tableHeaders should not contain "Job Id (Job Group)"
      }
      // Once at least one job has been run in a job group, then we should display the group name:
      sc.setJobGroup("my-job-group", "my-job-group-description")
      sc.parallelize(Seq(1, 2, 3)).count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs")
        val tableHeaders = findAll(cssSelector("th")).map(_.text).toSeq
        tableHeaders should contain ("Job Id (Job Group)")
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
        val taskContext = TaskContext.get
        if (taskContext.taskAttemptId() == 1) {
          // Cause the post-shuffle stage to fail on its first attempt with a single task failure
          val env = SparkEnv.get
          val bmAddress = env.blockManager.blockManagerId
          val shuffleId = shuffleHandle.shuffleId
          val mapId = 0
          val reduceId = taskContext.partitionId()
          val message = "Simulated fetch failure"
          throw new FetchFailedException(bmAddress, shuffleId, mapId, reduceId, message)
        } else {
          x
        }
      }
      mappedData.count()
      eventually(timeout(5 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs")
        find(cssSelector(".stage-progress-cell")).get.text should be ("2/2 (1 failed)")
        // Ideally, the following test would pass, but currently we overcount completed tasks
        // if task recomputations occur:
        // find(cssSelector(".progress-cell .progress")).get.text should be ("2/2 (1 failed)")
        // Instead, we guarantee that the total number of tasks is always correct, while the number
        // of completed tasks may be higher:
        find(cssSelector(".progress-cell .progress")).get.text should be ("3/2 (1 failed)")
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs/job/?id=0")
        find(id("active")).get.text should be ("Active Stages (1)")
        find(id("pending")).get.text should be ("Pending Stages (2)")
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs")
        // The completed jobs table should have two rows. The first row will be the most recent job:
        val firstRow = find(cssSelector("tbody tr")).get.underlying
        val firstRowColumns = firstRow.findElements(By.tagName("td"))
        firstRowColumns(0).getText should be ("1")
        firstRowColumns(4).getText should be ("1/1 (2 skipped)")
        firstRowColumns(5).getText should be ("8/8 (16 skipped)")
        // The second row is the first run of the job, where nothing was skipped:
        val secondRow = findAll(cssSelector("tbody tr")).toSeq(1).underlying
        val secondRowColumns = secondRow.findElements(By.tagName("td"))
        secondRowColumns(0).getText should be ("0")
        secondRowColumns(4).getText should be ("3/3")
        secondRowColumns(5).getText should be ("24/24")
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs/job/?id=1")
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/jobs")
        findAll(cssSelector("tbody tr a")).foreach { link =>
          link.text.toLowerCase should include ("count")
          link.text.toLowerCase should not include "unknown"
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/"))
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should not be(None)
      }
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check whether new page exists
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/foo")
        find(cssSelector("b")).get.text should include ("html magic")
      }
      sparkUI.detachTab(newTab)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sc.ui.get.appUIAddress.stripSuffix("/"))
        find(cssSelector("""ul li a[href*="jobs"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="stages"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="storage"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="environment"]""")) should not be(None)
        find(cssSelector("""ul li a[href*="foo"]""")) should be(None)
      }
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check new page not exist
        go to (sc.ui.get.appUIAddress.stripSuffix("/") + "/foo")
        find(cssSelector("b")) should be(None)
      }
    }
  }
}
