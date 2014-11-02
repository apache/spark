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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.Row

/**
 * A set of tests that validates support for Hive Explain command.
 */
class HiveExplainSuite extends QueryTest {
  private def check(sqlCmd: String, exists: Boolean, keywords: String*) {
    val outputs = sql(sqlCmd).collect().map(_.getString(0)).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $sqlCmd ($key doens't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $sqlCmd ($key existed in the result)")
      }
    }
  }

  test("explain extended command") {
    check(" explain   select * from src where key=123 ", true,
          "== Physical Plan ==")
    check(" explain   select * from src where key=123 ", false,
          "== Parsed Logical Plan ==",
          "== Analyzed Logical Plan ==", 
          "== Optimized Logical Plan ==")
    check(" explain   extended select * from src where key=123 ", true,
          "== Parsed Logical Plan ==", 
          "== Analyzed Logical Plan ==", 
          "== Optimized Logical Plan ==", 
          "== Physical Plan ==", 
          "Code Generation", "== RDD ==")
  }
}
