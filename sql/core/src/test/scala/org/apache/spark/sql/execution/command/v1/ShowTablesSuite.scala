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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.execution.command.{ShowTablesSuite => CommonShowTablesSuite}

class ShowTablesSuite extends CommonShowTablesSuite {
  override def catalog: String = "spark_catalog"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE DATABASE $namespace")
    sql(s"CREATE TABLE $namespace.$table (name STRING, id INT) USING PARQUET")
  }

  protected override def afterAll(): Unit = {
    sql(s"DROP TABLE $namespace.$table")
    sql(s"DROP DATABASE $namespace")
    super.afterAll()
  }

  test("show an existing table in V1 catalog") {
    val tables = sql(s"SHOW TABLES IN $catalog.test")
    assert(tables.schema.fieldNames.toSet === Set(namespaceColumn, tableColumn, "isTemporary"))
    checkAnswer(tables.select("isTemporary"), Row(false))
  }

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchDatabaseException] {
      checkAnswer(sql(s"SHOW TABLES IN $catalog.bad_test"), Seq())
    }.getMessage
    assert(msg.contains("Database 'bad_test' not found"))
  }
}
