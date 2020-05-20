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

import org.openqa.selenium.{By, WebDriver}
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

  implicit var webDriver: WebDriver
  private val driverPropPrefix = "spark.test."

  override def beforeAll() {
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
