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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.util.Random

import com.google.common.io.Files
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark.ui.SparkUICssErrorHandler

class UISeleniumSuite
  extends HiveThriftServer2TestBase
  with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _
  var server: HiveThriftServer2 = _
  val uiPort = 20000 + Random.nextInt(10000)
  override def mode: ServerMode.Value = ServerMode.binary

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

  override protected def serverStartCommand(): Seq[String] = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    val driverClassPath = {
      // Writes a temporary log4j.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files.
      val tempLog4jConf = org.apache.spark.util.Utils.createTempDir().getCanonicalPath

      Files.write(
        """log4j.rootCategory=INFO, console
          |log4j.appender.console=org.apache.log4j.ConsoleAppender
          |log4j.appender.console.target=System.err
          |log4j.appender.console.layout=org.apache.log4j.PatternLayout
          |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
        """.stripMargin,
        new File(s"$tempLog4jConf/log4j.properties"),
        StandardCharsets.UTF_8)

      tempLog4jConf
    }

    s"""$startScript
        |  --master local
        |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
        |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
        |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
        |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
        |  --hiveconf $portConf=0
        |  --driver-class-path $driverClassPath
        |  --conf spark.ui.enabled=true
        |  --conf spark.ui.port=$uiPort
     """.stripMargin.split("\\s+").toSeq
  }

  test("thrift server ui test") {
    withJdbcStatement("test_map") { statement =>
      val baseURL = s"http://localhost:$uiPort"

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
          findAll(cssSelector("span.description-input")).map(_.text).toList should contain (line)
        }
      }
    }
  }
}
