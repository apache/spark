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

package org.apache.spark.sql.catalyst.plans.logical.sql

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf

class ParsedStatementSuite extends SparkFunSuite with SQLHelper {

  test("Map[String, String] field in child class") {
    case class TestStatement(p: Map[String, String]) extends ParsedStatement

    withSQLConf(SQLConf.SQL_OPTIONS_REDACTION_PATTERN.key -> "my.password") {
      TestStatement(Map("spark.my.password" -> "secret")).toString should not contain ("secret")
    }
  }

  test("Map[Int, String] field in child class") {
    case class TestStatement(p: Map[Int, String]) extends ParsedStatement

    withSQLConf(SQLConf.SQL_OPTIONS_REDACTION_PATTERN.key -> "my.password") {
      TestStatement(Map(12345 -> "spark.my.password=secret")).toString should not contain ("secret")
    }
  }

  test("Map[String, Int] field in child class") {
    case class TestStatement(p: Map[String, Int]) extends ParsedStatement

    // Expect no exception
    TestStatement(Map("abc" -> 1)).toString
  }

  test("Map[String, Option[String]] field in child class") {
    case class TestStatement(p: Map[String, Option[String]]) extends ParsedStatement

    // Expect no exception
    TestStatement(Map("abc" -> Some("1"))).toString
  }
}
