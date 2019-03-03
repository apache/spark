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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.internal.SQLConf

class ParquetSchemaPruningSuite extends SchemaPruningSuite {
  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key

  testSchemaPruning("select a single complex field") {
    val query = sql("select name.middle from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"), Row("X.") :: Row("Y.") :: Row(null) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and its parent struct") {
    val query = sql("select name.middle, name from contacts")
    checkScan(query, "struct<name:struct<first:string,middle:string,last:string>>")
    checkAnswer(query.orderBy("id"),
      Row("X.", Row("Jane", "X.", "Doe")) ::
      Row("Y.", Row("John", "Y.", "Doe")) ::
      Row(null, Row("Janet", null, "Jones")) ::
      Row(null, Row("Jim", null, "Jones")) ::
      Nil)
  }

  testSchemaPruning("select a single complex field and the partition column") {
    val query = sql("select name.middle, p from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"),
      Row("X.", 1) :: Row("Y.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  testSchemaPruning("no unnecessary schema pruning") {
    val query =
      sql("select id, name.last, name.middle, name.first, relatives[''].last, " +
        "relatives[''].middle, relatives[''].first, friends[0].last, friends[0].middle, " +
        "friends[0].first, pets, address from contacts where p=2")
    // We've selected every field in the schema. Therefore, no schema pruning should be performed.
    // We check this by asserting that the scanned schema of the query is identical to the schema
    // of the contacts relation, even though the fields are selected in different orders.
    checkScan(query,
      "struct<id:int,name:struct<first:string,middle:string,last:string>,address:string,pets:int," +
      "friends:array<struct<first:string,middle:string,last:string>>," +
      "relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(query.orderBy("id"),
      Row(2, "Jones", null, "Janet", null, null, null, null, null, null, null, "567 Maple Drive") ::
      Row(3, "Jones", null, "Jim", null, null, null, null, null, null, null, "6242 Ash Street") ::
      Nil)
  }

  testSchemaPruning("empty schema intersection") {
    val query = sql("select name.middle from contacts where p=2")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"),
      Row(null) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and is null expression in project") {
    val query = sql("select name.first, address is not null from contacts")
    checkScan(query, "struct<name:struct<first:string>,address:string>")
    checkAnswer(query.orderBy("id"),
      Row("Jane", true) :: Row("John", true) :: Row("Janet", true) :: Row("Jim", true) :: Nil)
  }
}
