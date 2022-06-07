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
      // Writes a temporary log4j2.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files.
      val tempLog4jConf = org.apache.spark.util.Utils.createTempDir().getCanonicalPath

      Files.write(
        """rootLogger.level = info
          |rootLogger.appenderRef.file.ref = console
          |appender.console.type = Console
          |appender.console.name = console
          |appender.console.target = SYSTEM_ERR
          |appender.console.layout.type = PatternLayout
          |appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
        """.stripMargin,
        new File(s"$tempLog4jConf/log4j2.properties"),
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

  test("SPARK-36400: Redact sensitive information in UI by config") {
    withJdbcStatement("test_tbl1", "test_tbl2") { statement =>
      val baseURL = s"http://localhost:$uiPort"

      val Seq(nonMaskedQuery, maskedQuery) = Seq("test_tbl1", "test_tbl2").map (tblName =>
        s"CREATE TABLE $tblName(a int) " +
          s"OPTIONS(url='jdbc:postgresql://localhost:5432/$tblName', " +
          "user='test_user', password='abcde')")
      statement.execute(nonMaskedQuery)

      statement.execute("SET spark.sql.redaction.string.regex=((?i)(?<=password=))('.*')")
      statement.execute(maskedQuery)

      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        go to (baseURL + "/sqlserver")
        // Take description of 2 statements executed within this test.
        val statements = findAll(cssSelector("span.description-input"))
          .map(_.text).filter(_.startsWith("CREATE")).take(2).toSeq

        val nonMaskedStatement = statements.filter(_.contains("test_tbl1"))
        nonMaskedStatement.size should be (1)
        nonMaskedStatement.head should be (nonMaskedQuery)
        val maskedStatement = statements.filter(_.contains("test_tbl2"))
        maskedStatement.size should be (1)
        maskedStatement.head should be (maskedQuery.replace("'abcde'", "*********(redacted)"))
      }

      val sessionLink =
        find(cssSelector("table#sessionstat td a")).head.underlying.getAttribute("href")
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        go to sessionLink
        val statements = findAll(
          cssSelector("span.description-input")).map(_.text).filter(_.startsWith("CREATE")).toSeq
        statements.size should be (2)

        val nonMaskedStatement = statements.filter(_.contains("test_tbl1"))
        nonMaskedStatement.size should be (1)
        nonMaskedStatement.head should be (nonMaskedQuery)
        val maskedStatement = statements.filter(_.contains("test_tbl2"))
        maskedStatement.size should be (1)
        maskedStatement.head should be (maskedQuery.replace("'abcde'", "*********(redacted)"))
      }
    }
  }
}
