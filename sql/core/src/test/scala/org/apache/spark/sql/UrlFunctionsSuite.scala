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

package org.apache.spark.sql

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class UrlFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("url parse_url function") {

    def testUrl(url: String, expected: Row): Unit = {
      checkAnswer(Seq[String]((url)).toDF("url").selectExpr(
        "parse_url(url, 'HOST')", "parse_url(url, 'PATH')",
        "parse_url(url, 'QUERY')", "parse_url(url, 'REF')",
        "parse_url(url, 'PROTOCOL')", "parse_url(url, 'FILE')",
        "parse_url(url, 'AUTHORITY')", "parse_url(url, 'USERINFO')",
        "parse_url(url, 'QUERY', 'query')"), expected)
    }

    testUrl(
      "http://userinfo@spark.apache.org/path?query=1#Ref",
      Row("spark.apache.org", "/path", "query=1", "Ref",
        "http", "/path?query=1", "userinfo@spark.apache.org", "userinfo", "1"))

    testUrl(
      "https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&q2=2#Ref%20two",
      Row("example.com", "/dir%20/pa%20th.HTML", "query=x%20y&q2=2", "Ref%20two",
        "https", "/dir%20/pa%20th.HTML?query=x%20y&q2=2", "use%20r:pas%20s@example.com",
        "use%20r:pas%20s", "x%20y"))

    testUrl(
      "http://user:pass@host",
      Row("host", "", null, null, "http", "", "user:pass@host", "user:pass", null))

    testUrl(
      "http://user:pass@host/",
      Row("host", "/", null, null, "http", "/", "user:pass@host", "user:pass", null))

    testUrl(
      "http://user:pass@host/?#",
      Row("host", "/", "", "", "http", "/?", "user:pass@host", "user:pass", null))

    testUrl(
      "http://user:pass@host/file;param?query;p2",
      Row("host", "/file;param", "query;p2", null, "http", "/file;param?query;p2",
        "user:pass@host", "user:pass", null))

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      testUrl(
        "inva lid://user:pass@host/file;param?query;p2",
        Row(null, null, null, null, null, null, null, null, null))
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val url = "inva lid://user:pass@host/file;param?query;p2"
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          sql(s"SELECT parse_url('$url', 'HOST')").collect()
        },
        condition = "INVALID_URL",
        parameters = Map(
          "url" -> url,
          "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)
        ))
    }
  }

  test("url encode/decode function") {
    def testUrl(url: String, fn: String, expected: Row): Unit = {
      checkAnswer(Seq[String]((url)).toDF("url")
        .selectExpr(s"$fn(url)"), expected)
    }

    testUrl("https://spark.apache.org", "url_encode", Row("https%3A%2F%2Fspark.apache.org"))
    testUrl("null", "url_encode", Row("null"))

    testUrl("https%3A%2F%2Fspark.apache.org", "url_decode", Row("https://spark.apache.org"))
    testUrl("null", "url_decode", Row("null"))
  }

}
