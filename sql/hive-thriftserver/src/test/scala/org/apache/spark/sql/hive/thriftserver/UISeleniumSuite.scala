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

package org.apache.spark.sql.hive.thriftserver

import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark.SparkConf
import org.apache.spark.ui.SparkUICssErrorHandler

class UISeleniumSuite extends SharedThriftServer
  with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "0")
  }

  override def beforeAll(): Unit = {
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
    super.beforeAll()
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

  test("thrift server ui test") {
    withJdbcStatement("test_map") { statement =>
      val baseURL = spark.sparkContext.uiWebUrl.get

      val queries = Seq(
        "CREATE TABLE test_map(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        go to baseURL
        find(cssSelector("""ul li a[href*="sql"]""")) should not be None
      }

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        go to (baseURL + "/sqlserver")
        find(id("sessionstat")) should not be None
        find(id("sqlstat")) should not be None

        // check whether statements exists
        queries.foreach { line =>
          findAll(cssSelector("""table tbody tr td span"""))
            .map(_.text).toList should contain (line)
        }
      }
    }
  }
}
