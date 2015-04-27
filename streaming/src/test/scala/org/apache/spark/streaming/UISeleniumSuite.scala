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

package org.apache.spark.streaming

import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.SpanSugar._

import org.apache.spark._




/**
 * Selenium tests for the Spark Web UI.
 */
class UISeleniumSuite extends FunSuite with WebBrowser with Matchers with BeforeAndAfterAll with TestSuiteBase {

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
   * Create a test SparkStreamingContext with the SparkUI enabled.
   */
  private def newSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(1))
    assert(ssc.sc.ui.isDefined, "Spark UI is not started!")
    ssc
  }

  test("attaching and detaching a Streaming tab") {
    withStreamingContext(newSparkStreamingContext()) { ssc =>
      val sparkUI = ssc.sparkContext.ui.get

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/"))
        find(cssSelector( """ul li a[href*="streaming"]""")) should not be (None)
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check whether streaming page exists
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming")
        val statisticText = findAll(cssSelector("li strong")).map(_.text).toSeq
        statisticText should contain("Network receivers:")
        statisticText should contain("Batch interval:")
      }

      ssc.stop(false)

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/"))
        find(cssSelector( """ul li a[href*="streaming"]""")) should be(None)
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming")
        val statisticText = findAll(cssSelector("li strong")).map(_.text).toSeq
        statisticText should not contain ("Network receivers:")
        statisticText should not contain ("Batch interval:")
      }
    }
  }
}
  
