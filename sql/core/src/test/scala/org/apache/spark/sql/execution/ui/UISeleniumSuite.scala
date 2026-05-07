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
import scala.jdk.CollectionConverters._

import org.apache.commons.text.StringEscapeUtils.escapeJava
import org.apache.commons.text.translate.EntityArrays._
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.tags.WebBrowserTest
import org.apache.spark.ui.SparkUICssErrorHandler
import org.apache.spark.util.Utils

@WebBrowserTest
class UISeleniumSuite extends SparkFunSuite with WebBrowser {

  private var spark: SparkSession = _

  private def creatSparkSessionWithUI: SparkSession = SparkSession.builder()
    .master("local[1,1]")
    .appName("sql ui test")
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "0")
    .getOrCreate()

  implicit val webDriver: HtmlUnitDriver = new HtmlUnitDriver {
    getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
  }

  private def findErrorMessageViaAPI(): List[String] = {
    val webUrl = spark.sparkContext.uiWebUrl.get
    val url = s"$webUrl/api/v1/applications/${spark.sparkContext.applicationId}" +
      "/sql/?details=false&planDescription=false"
    val json = Utils.tryWithResource(scala.io.Source.fromURL(url))(_.mkString)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val executions = mapper.readTree(json)
    executions.elements().asScala.toList
      .filter(e => e.has("errorMessage") && !e.get("errorMessage").isNull)
      .map(_.get("errorMessage").asText)
  }

  private def findExecutionIDViaAPI(): Int = {
    val webUrl = spark.sparkContext.uiWebUrl.get
    val url = s"$webUrl/api/v1/applications/${spark.sparkContext.applicationId}" +
      "/sql/?details=false&planDescription=false"
    val json = Utils.tryWithResource(scala.io.Source.fromURL(url))(_.mkString)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val executions = mapper.readTree(json)
    executions.elements().asScala.toList
      .filter(e => e.has("errorMessage") && !e.get("errorMessage").isNull)
      .head.get("id").asInt
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
    spark = creatSparkSessionWithUI

    intercept[Exception](spark.sql("SET mapreduce.job.reduces = 0").isEmpty)
    eventually(timeout(30.seconds), interval(3.seconds)) {
      val sd = findErrorMessageViaAPI()
      assert(sd.size === 1, "SET mapreduce.job.reduces = 0 shall fail")
      assert(sd.head.startsWith("java.lang.IllegalArgumentException:"))
    }
  }

  test("SPARK-44801: Analyzer failure shall show the query in failed table") {
    spark = creatSparkSessionWithUI

    intercept[Exception](spark.sql("SELECT * FROM I_AM_AN_INVISIBLE_TABLE").isEmpty)
    eventually(timeout(30.seconds), interval(3.seconds)) {
      val sd = findErrorMessageViaAPI()
      assert(sd.size === 1, "Analyze fail shall show the query in failed table")
      assert(sd.head.startsWith("[TABLE_OR_VIEW_NOT_FOUND]"))

      val id = findExecutionIDViaAPI()
      // check query detail page
      go to s"${spark.sparkContext.uiWebUrl.get}/SQL/execution/?id=$id"
      val planDot = findAll(cssSelector(""".dot-file""")).map(_.text).toList
      assert(planDot.size === 1)
      val planDetails = findAll(cssSelector("""#physical-plan-details""")).map(_.text).toList
      assert(planDetails.head.isEmpty)
    }
  }

  test("SPARK-44960: Escape html is not necessary for Spark UI") {
    spark = creatSparkSessionWithUI
    val escape = (BASIC_ESCAPE.keySet().asScala.toSeq ++ ISO8859_1_ESCAPE.keySet().asScala ++
      HTML40_EXTENDED_ESCAPE.keySet().asScala).mkString
    val errorMsg = escapeJava(escape.mkString)
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.sql(s"SELECT raise_error('$errorMsg')").collect()
      },
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> escape))
    eventually(timeout(30.seconds), interval(3.seconds)) {
      val errors = findErrorMessageViaAPI()
      assert(errors.nonEmpty)
      assert(!errors.head.contains("&amp;"))
    }
  }
}
