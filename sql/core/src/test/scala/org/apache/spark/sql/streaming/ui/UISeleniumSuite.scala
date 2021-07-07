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

package org.apache.spark.sql.streaming.ui

import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark._
import org.apache.spark.internal.config.UI.{UI_ENABLED, UI_PORT}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.functions.{window => windowFn, _}
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
import org.apache.spark.sql.internal.StaticSQLConf.ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.ui.SparkUICssErrorHandler

class UISeleniumSuite extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
  }

  private def newSparkSession(
      master: String = "local",
      additionalConfs: Map[String, String] = Map.empty): SparkSession = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("ui-test")
      .set(SHUFFLE_PARTITIONS, 5)
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
      .set(ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST, Seq("stateOnCurrentVersionSizeBytes"))
    additionalConfs.foreach { case (k, v) => conf.set(k, v) }
    val spark = SparkSession.builder().master(master).config(conf).getOrCreate()
    assert(spark.sparkContext.ui.isDefined)
    spark
  }

  def goToUi(spark: SparkSession, path: String): Unit = {
    go to (spark.sparkContext.ui.get.webUrl.stripSuffix("/") + path)
  }

  test("SPARK-30984: Structured Streaming UI should be activated when running a streaming query") {
    quietly {
      withSparkSession(newSparkSession()) { spark =>
        import spark.implicits._
        try {
          spark.range(1, 10).count()

          goToUi(spark, "/StreamingQuery")

          val h3Text = findAll(cssSelector("h3")).map(_.text).toSeq
          h3Text should not contain ("Streaming Query")

          val input1 = spark.readStream.format("rate").load()
          val input2 = spark.readStream.format("rate").load()
          val input3 = spark.readStream.format("rate").load()
          val activeQuery =
            input1.selectExpr("timestamp", "mod(value, 100) as mod", "value")
              .withWatermark("timestamp", "0 second")
              .groupBy(windowFn($"timestamp", "10 seconds", "2 seconds"), $"mod")
              .agg(avg("value").as("avg_value"))
              .writeStream.format("noop").trigger(Trigger.ProcessingTime("5 seconds")).start()
          val completedQuery =
            input2.join(input3, "value").writeStream.format("noop").start()
          completedQuery.stop()
          val failedQuery = spark.readStream.format("rate").load().select("value").as[Long]
            .map(_ / 0).writeStream.format("noop").start()
          try {
            failedQuery.awaitTermination()
          } catch {
            case _: StreamingQueryException =>
          }

          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            // Check the query list page
            goToUi(spark, "/StreamingQuery")

            findAll(cssSelector("h3")).map(_.text).toSeq should contain("Streaming Query")

            val arrow = 0x25BE.toChar
            findAll(cssSelector("""#active-table th""")).map(_.text).toList should be {
              List("Name", "Status", "ID", "Run ID", s"Start Time $arrow", "Duration",
                "Avg Input /sec", "Avg Process /sec", "Latest Batch")
            }
            val activeQueries =
              findAll(cssSelector("""#active-table td""")).map(_.text).toSeq
            activeQueries should contain(activeQuery.id.toString)
            activeQueries should contain(activeQuery.runId.toString)
            findAll(cssSelector("""#completed-table th"""))
              .map(_.text).toList should be {
                List("Name", "Status", "ID", "Run ID", s"Start Time $arrow", "Duration",
                  "Avg Input /sec", "Avg Process /sec", "Latest Batch", "Error")
              }
            val completedQueries =
              findAll(cssSelector("""#completed-table td""")).map(_.text).toSeq
            completedQueries should contain(completedQuery.id.toString)
            completedQueries should contain(completedQuery.runId.toString)
            completedQueries should contain(failedQuery.id.toString)
            completedQueries should contain(failedQuery.runId.toString)

            // Check the query statistics page
            val activeQueryLink =
              findAll(cssSelector("""#active-table td a""")).flatMap(_.attribute("href")).next
            go to activeQueryLink

            findAll(cssSelector("h3"))
              .map(_.text).toSeq should contain("Streaming Query Statistics")
            val summaryText = findAll(cssSelector("div strong")).map(_.text).toSeq
            summaryText should contain ("Name:")
            summaryText should contain ("Id:")
            summaryText should contain ("RunId:")
            findAll(cssSelector("""#stat-table th""")).map(_.text).toSeq should be {
              List("", "Timelines", "Histograms")
            }
            summaryText should contain ("Input Rate (?)")
            summaryText should contain ("Process Rate (?)")
            summaryText should contain ("Input Rows (?)")
            summaryText should contain ("Batch Duration (?)")
            summaryText should contain ("Operation Duration (?)")
            summaryText should contain ("Global Watermark Gap (?)")
            summaryText should contain ("Aggregated Number Of Total State Rows (?)")
            summaryText should contain ("Aggregated Number Of Updated State Rows (?)")
            summaryText should contain ("Aggregated State Memory Used In Bytes (?)")
            summaryText should contain ("Aggregated Number Of Rows Dropped By Watermark (?)")
            summaryText should contain ("Aggregated Custom Metric stateOnCurrentVersionSizeBytes" +
              " (?)")
            summaryText should not contain ("Aggregated Custom Metric loadedMapCacheHitCount (?)")
            summaryText should not contain ("Aggregated Custom Metric loadedMapCacheMissCount (?)")
          }
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
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
}
