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

import org.openqa.selenium.{By, JavascriptExecutor, WebDriver}
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
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
  extends SparkFunSuite with WebBrowser with Matchers {

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
      sc.parallelize(1 to 10).collect()

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
        val nodeDesc = webDriver.findElement(By.cssSelector("g[id='node_0']"))
        nodeDesc.getAttribute("innerHTML") should include ("collect at &lt;console&gt;:25")
      }
    }
  }

  test("SPARK-31882: Link URL for Stage DAGs should not depend on paged table.") {
    withSpark(newSparkContext()) { sc =>
      sc.parallelize(1 to 100).map(v => (v, v)).repartition(10).reduceByKey(_ + _).collect()

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
      sc.parallelize(1 to 10).barrier().mapPartitions(identity).repartition(1).collect()

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        goToUi(sc, "/jobs/job/?id=0")
        webDriver.findElement(By.id("job-dag-viz")).click()

        val stage0 = webDriver.findElement(By.cssSelector("g[id='graph_stage_0']"))
          .findElement(By.xpath(".."))
        val stage1 = webDriver.findElement(By.cssSelector("g[id='graph_stage_1']"))
          .findElement(By.xpath(".."))
        val barrieredOps = webDriver.findElements(By.className("barrier-rdd")).iterator()
        val id1 = barrieredOps.next().getAttribute("innerHTML")
        val id2 = barrieredOps.next().getAttribute("innerHTML")
        assert(!barrieredOps.hasNext())

        val prefix = "g[class='cluster barrier']#cluster_"
        assert(stage0.findElements(By.cssSelector(s"${prefix}$id1")).size === 1)
        assert(stage0.findElements(By.cssSelector(s"${prefix}$id2")).size === 1)
        assert(stage1.findElements(By.cssSelector(s"${prefix}$id1")).size === 0)
        assert(stage1.findElements(By.cssSelector(s"${prefix}$id2")).size === 1)
      }
    }
  }

  test("Search text for paged tables should not be saved") {
    withSpark(newSparkContext()) { sc =>
      sc.parallelize(1 to 10).collect()

      eventually(timeout(10.seconds), interval(1.seconds)) {
        val taskSearchBox = "$(\"input[aria-controls='active-tasks-table']\")"
        goToUi(sc, "/stages/stage/?id=0&attempt=0")
        // Wait for ajax loading done.
        Thread.sleep(20)
        setValueToSearchBox(taskSearchBox, "task1")
        val taskSearchText = getTextFromSearchBox(taskSearchBox)
        assert(taskSearchText === "task1")

        val executorSearchBox = "$(\"input[aria-controls='active-executors-table']\")"
        goToUi(sc, "/executors")
        Thread.sleep(20)
        setValueToSearchBox(executorSearchBox, "executor1")
        val executorSearchText = getTextFromSearchBox(executorSearchBox)
        assert(executorSearchText === "executor1")

        goToUi(sc, "/stages/stage/?id=0&attempt=0")
        Thread.sleep(20)
        val revisitTaskSearchText = getTextFromSearchBox(taskSearchBox)
        assert(revisitTaskSearchText === "")

        goToUi(sc, "/executors")
        Thread.sleep(20)
        val revisitExecutorSearchText = getTextFromSearchBox(executorSearchBox)
        assert(revisitExecutorSearchText === "")
      }
    }

    def setValueToSearchBox(searchBox: String, text: String): Unit = {
      webDriver.executeScript(s"$searchBox.val('$text');")
      fireDataTable(searchBox)
    }

    def getTextFromSearchBox(searchBox: String): String = {
      webDriver.executeScript(s"return $searchBox.val();").toString
    }

    def fireDataTable(searchBox: String): Unit = {
      webDriver.executeScript(
        s"""
           |var keyEvent = $$.Event('keyup');
           |// 13 means enter key.
           |keyEvent.keyCode = keyEvent.which = 13;
           |$searchBox.trigger(keyEvent);
         """.stripMargin)
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
