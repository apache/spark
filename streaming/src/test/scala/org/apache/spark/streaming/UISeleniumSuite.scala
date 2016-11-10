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

import scala.collection.mutable.Queue

import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.ui.SparkUICssErrorHandler

/**
 * Selenium tests for the Spark Streaming Web UI.
 */
class UISeleniumSuite
  extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll with TestSuiteBase {

  implicit var webDriver: WebDriver = _

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

  private def setupStreams(ssc: StreamingContext): Unit = {
    val rdds = Queue(ssc.sc.parallelize(1 to 4, 4))
    val inputStream = ssc.queueStream(rdds)
    inputStream.foreachRDD { rdd =>
      rdd.foreach(_ => {})
      rdd.foreach(_ => {})
    }
    inputStream.foreachRDD { rdd =>
      rdd.foreach(_ => {})
      try {
        rdd.foreach(_ => throw new RuntimeException("Oops"))
      } catch {
        case e: SparkException if e.getMessage.contains("Oops") =>
      }
    }
  }

  test("attaching and detaching a Streaming tab") {
    withStreamingContext(newSparkStreamingContext()) { ssc =>
      setupStreams(ssc)
      ssc.start()

      val sparkUI = ssc.sparkContext.ui.get

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/"))
        find(cssSelector( """ul li a[href*="streaming"]""")) should not be (None)
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        // check whether streaming page exists
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming")
        val h3Text = findAll(cssSelector("h3")).map(_.text).toSeq
        h3Text should contain("Streaming Statistics")

        // Check stat table
        val statTableHeaders = findAll(cssSelector("#stat-table th")).map(_.text).toSeq
        statTableHeaders.exists(
          _.matches("Timelines \\(Last \\d+ batches, \\d+ active, \\d+ completed\\)")
        ) should be (true)
        statTableHeaders should contain ("Histograms")

        val statTableCells = findAll(cssSelector("#stat-table td")).map(_.text).toSeq
        statTableCells.exists(_.contains("Input Rate")) should be (true)
        statTableCells.exists(_.contains("Scheduling Delay")) should be (true)
        statTableCells.exists(_.contains("Processing Time")) should be (true)
        statTableCells.exists(_.contains("Total Delay")) should be (true)

        // Check batch tables
        val h4Text = findAll(cssSelector("h4")).map(_.text).toSeq
        h4Text.exists(_.matches("Active Batches \\(\\d+\\)")) should be (true)
        h4Text.exists(_.matches("Completed Batches \\(last \\d+ out of \\d+\\)")) should be (true)

        findAll(cssSelector("""#active-batches-table th""")).map(_.text).toSeq should be {
          List("Batch Time", "Input Size", "Scheduling Delay (?)", "Processing Time (?)",
            "Output Ops: Succeeded/Total", "Status")
        }
        findAll(cssSelector("""#completed-batches-table th""")).map(_.text).toSeq should be {
          List("Batch Time", "Input Size", "Scheduling Delay (?)", "Processing Time (?)",
            "Total Delay (?)", "Output Ops: Succeeded/Total")
        }

        val batchLinks =
          findAll(cssSelector("""#completed-batches-table a""")).flatMap(_.attribute("href")).toSeq
        batchLinks.size should be >= 1

        // Check a normal batch page
        go to (batchLinks.last) // Last should be the first batch, so it will have some jobs
        val summaryText = findAll(cssSelector("li strong")).map(_.text).toSeq
        summaryText should contain ("Batch Duration:")
        summaryText should contain ("Input data size:")
        summaryText should contain ("Scheduling delay:")
        summaryText should contain ("Processing time:")
        summaryText should contain ("Total delay:")

        findAll(cssSelector("""#batch-job-table th""")).map(_.text).toSeq should be {
          List("Output Op Id", "Description", "Output Op Duration", "Status", "Job Id",
            "Job Duration", "Stages: Succeeded/Total", "Tasks (for all stages): Succeeded/Total",
            "Error")
        }

        // Check we have 2 output op ids
        val outputOpIds = findAll(cssSelector(".output-op-id-cell")).toSeq
        outputOpIds.map(_.attribute("rowspan")) should be (List(Some("2"), Some("2")))
        outputOpIds.map(_.text) should be (List("0", "1"))

        // Check job ids
        val jobIdCells = findAll(cssSelector( """#batch-job-table a""")).toSeq
        jobIdCells.map(_.text) should be (List("0", "1", "2", "3"))

        val jobLinks = jobIdCells.flatMap(_.attribute("href"))
        jobLinks.size should be (4)

        // Check stage progress
        findAll(cssSelector(""".stage-progress-cell""")).map(_.text).toSeq should be
          (List("1/1", "1/1", "1/1", "0/1 (1 failed)"))

        // Check job progress
        findAll(cssSelector(""".progress-cell""")).map(_.text).toSeq should be
          (List("1/1", "1/1", "1/1", "0/1 (1 failed)"))

        // Check stacktrace
        val errorCells = findAll(cssSelector(""".stacktrace-details""")).map(_.underlying).toSeq
        errorCells should have size 1
        // Can't get the inner (invisible) text without running JS

        // Check the job link in the batch page is right
        go to (jobLinks(0))
        val jobDetails = findAll(cssSelector("li strong")).map(_.text).toSeq
        jobDetails should contain("Status:")
        jobDetails should contain("Completed Stages:")

        // Check a batch page without id
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming/batch/")
        webDriver.getPageSource should include ("Missing id parameter")

        // Check a non-exist batch
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming/batch/?id=12345")
        webDriver.getPageSource should include ("does not exist")
      }

      ssc.stop(false)

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/"))
        find(cssSelector( """ul li a[href*="streaming"]""")) should be(None)
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (sparkUI.appUIAddress.stripSuffix("/") + "/streaming")
        val h3Text = findAll(cssSelector("h3")).map(_.text).toSeq
        h3Text should not contain("Streaming Statistics")
      }
    }
  }
}
