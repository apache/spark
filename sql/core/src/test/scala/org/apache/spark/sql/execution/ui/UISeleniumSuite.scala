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

package org.apache.spark.sql.execution.ui

import scala.concurrent.duration.DurationInt

import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.SparkUICssErrorHandler

class UISeleniumSuite extends SparkFunSuite with WebBrowser {

  private var spark: SparkSession = _

  implicit val webDriver: HtmlUnitDriver = new HtmlUnitDriver {
    getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
  }

  override def afterAll(): Unit = {
    try {
      webDriver.quit()
    } finally {
      super.afterAll()
    }
  }

  override def afterEach(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  test("SPARK-44737: Should not display json format errors on SQL page for non-SparkThrowables") {
    spark = SparkSession.builder()
      .master("local[1,1]")
      .appName("sql ui test")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "0")
      .getOrCreate()

    intercept[Exception](spark.sql("SET mapreduce.job.reduces = 0").isEmpty)
    eventually(timeout(10.seconds), interval(100.milliseconds)) {
      val webUrl = spark.sparkContext.uiWebUrl
      assert(webUrl.isDefined, "please turn on spark.ui.enabled")
      go to s"${webUrl.get}/SQL"
      val sd = findAll(cssSelector("""#failed-table td .stacktrace-details""")).map(_.text).toList
      assert(sd.size === 1, "SET mapreduce.job.reduces = 0 shall fail")
      assert(sd.head.startsWith("java.lang.IllegalArgumentException:"))
    }
  }
}
