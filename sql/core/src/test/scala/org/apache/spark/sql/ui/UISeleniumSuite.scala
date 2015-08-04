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

package org.apache.spark.sql.ui

import scala.concurrent.duration._

import org.apache.spark.sql.SQLContext
import org.json4s.DefaultFormats
import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser

import org.apache.spark.{SparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.ui.SparkUICssErrorHandler

/**
 * Selenium tests for the SQL tab.
 */
class UISeleniumSuite extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _
  implicit val formats = DefaultFormats


  override def beforeAll(): Unit = {
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
  }

  override def afterAll(): Unit = {
    if (webDriver != null) {
      webDriver.quit()
    }
  }


  /**
   * Create a test SQLContext with the SparkUI enabled.
   */
  private def newSQLContext(): SQLContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
    val sc = new SparkContext(conf)
    new SQLContext(sc)
  }

  test("sql tab") {
    val sqlContext = newSQLContext()
    try {
      import sqlContext.implicits._

      // Run a simple query
      Seq(
        (Array(1, 2, 3), Array(1, 2, 3)),
        (Array(2, 3, 4), Array(2, 3, 4))
      ).toDF().count()

      val sparkUI = sqlContext.sparkContext.ui.get

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // Check if the SQL tab exists
        go to (sparkUI.appUIAddress.stripSuffix("/"))
        find(cssSelector( """ul li a[href*="sql"]""")) should not be (None)

        // Check if the SQL page exists
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/sql")
        val h3Text = findAll(cssSelector("h3")).map(_.text).toSeq
        h3Text should contain ("SQL")

        // Check the completed execution table
        val h4Text = findAll(cssSelector("h4")).map(_.text).toSeq
        h4Text should contain ("Completed Queries")

        val completedTableHeaders =
          findAll(cssSelector("#completed-execution-table th")).map(_.text).toSeq
        completedTableHeaders should be {
          List("ID", "Description", "Submitted", "Duration", "Jobs", "Detail")
        }

        // Check the links
        val links =
          findAll(cssSelector("#completed-execution-table a")).flatMap(_.attribute("href")).toSeq
        links should have size 2

        // Check the execution page
        val executionPageLink = links(0)
        go to executionPageLink
        findAll(cssSelector("h3")).map(_.text).toSeq should contain ("Details for Query 0")

        val summaryText = findAll(cssSelector("li strong")).map(_.text).toSeq
        summaryText should contain ("Submitted Time:")
        summaryText should contain ("Duration:")
        summaryText should contain ("Succeeded Jobs:")
        summaryText should contain ("Detail:")

        // Check the job link is correct
        val jobLink = links(1)
        go to jobLink
        findAll(cssSelector("h3")).map(_.text).toSeq should contain ("Details for Job 0")
      }
    } finally {
      sqlContext.sparkContext.stop()
    }
  }

}
