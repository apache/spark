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

import org.apache.spark.SparkFunSuite

class SQLQueryTestHelperSuite extends SparkFunSuite with SQLQueryTestHelper {

  test("getSparkSettings: single key=value") {
    val result = getSparkSettings(Array("--SET spark.sql.foo=1"))
    assert(result.toSeq === Seq("spark.sql.foo" -> "1"))
  }

  test("getSparkSettings: multiple key=value pairs in one --SET (documented form)") {
    val result = getSparkSettings(Array("--SET spark.sql.foo=1,spark.sql.bar=2"))
    assert(result.toSeq === Seq("spark.sql.foo" -> "1", "spark.sql.bar" -> "2"))
  }

  test("getSparkSettings: multiple --SET statements") {
    val result = getSparkSettings(
      Array("--SET spark.sql.foo=1", "--SET spark.sql.bar=2"))
    assert(result.toSeq === Seq("spark.sql.foo" -> "1", "spark.sql.bar" -> "2"))
  }

  test("getSparkSettings: value containing commas (e.g. excludedRules list)") {
    val excludedRules =
      "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation," +
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val result = getSparkSettings(
      Array(s"--SET spark.sql.optimizer.excludedRules=$excludedRules"))
    assert(result.toSeq === Seq("spark.sql.optimizer.excludedRules" -> excludedRules))
  }

  test("getSparkSettings: mixed -- multiple settings where one value contains commas") {
    val excludedRules =
      "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation," +
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val result = getSparkSettings(
      Array(s"--SET spark.sql.optimizer.excludedRules=$excludedRules,spark.sql.foo=1"))
    assert(result.toSeq === Seq(
      "spark.sql.optimizer.excludedRules" -> excludedRules,
      "spark.sql.foo" -> "1"))
  }

  test("getSparkSettings: ignores non --SET comments") {
    val result = getSparkSettings(
      Array("-- a comment", "--SET spark.sql.foo=1", "-- another"))
    assert(result.toSeq === Seq("spark.sql.foo" -> "1"))
  }
}
