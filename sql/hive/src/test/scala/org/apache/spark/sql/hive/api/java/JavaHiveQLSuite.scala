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

package org.apache.spark.sql.hive.api.java

import scala.util.Try

import org.scalatest.FunSuite

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.api.java.{JavaSQLContext, JavaSchemaRDD}
import org.apache.spark.sql.execution.ExplainCommand
import org.apache.spark.sql.hive.test.TestHive

// Implicits
import scala.collection.JavaConversions._

class JavaHiveQLSuite extends FunSuite {
  lazy val javaCtx = new JavaSparkContext(TestHive.sparkContext)

  // There is a little trickery here to avoid instantiating two HiveContexts in the same JVM
  lazy val javaHiveCtx = new JavaHiveContext(TestHive)

  test("SELECT * FROM src") {
    assert(
      javaHiveCtx.sql("SELECT * FROM src").collect().map(_.getInt(0)) ===
        TestHive.sql("SELECT * FROM src").collect().map(_.getInt(0)).toSeq)
  }

  def isExplanation(result: JavaSchemaRDD) = {
    val explanation = result.collect().map(_.getString(0))
    explanation.size > 1 && explanation.head.startsWith("== Physical Plan ==")
  }

  test("Query Hive native command execution result") {
    val tableName = "test_native_commands"

    assertResult(0) {
      javaHiveCtx.sql(s"DROP TABLE IF EXISTS $tableName").count()
    }

    assertResult(0) {
      javaHiveCtx.sql(s"CREATE TABLE $tableName(key INT, value STRING)").count()
    }

    assert(
      javaHiveCtx
        .sql("SHOW TABLES")
        .collect()
        .map(_.getString(0))
        .contains(tableName))

    assertResult(Array(Array("key", "int"), Array("value", "string"))) {
      javaHiveCtx
        .sql(s"describe $tableName")
        .collect()
        .map(row => Array(row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[String]))
        .toArray
    }

    assert(isExplanation(javaHiveCtx.sql(
      s"EXPLAIN SELECT key, COUNT(*) FROM $tableName GROUP BY key")))

    TestHive.reset()
  }

  test("Exactly once semantics for DDL and command statements") {
    val tableName = "test_exactly_once"
    val q0 = javaHiveCtx.sql(s"CREATE TABLE $tableName(key INT, value STRING)")

    // If the table was not created, the following assertion would fail
    assert(Try(TestHive.table(tableName)).isSuccess)

    // If the CREATE TABLE command got executed again, the following assertion would fail
    assert(Try(q0.count()).isSuccess)
  }
}
