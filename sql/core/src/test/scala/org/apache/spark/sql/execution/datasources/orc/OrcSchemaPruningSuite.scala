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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.internal.SQLConf

class OrcSchemaPruningSuite extends SchemaPruningSuite {
  override protected val dataSourceName: String = "orc"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.ORC_VECTORIZED_READER_ENABLED.key

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_READER_LIST, "orc")
      .set(SQLConf.USE_V1_SOURCE_WRITER_LIST, "orc")

  testSchemaPruning("select a single complex field") {
    val query = sql("select name.middle from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"), Row("X.") :: Row("Y.") :: Nil)
  }

  testSchemaPruning("select a single complex field and its parent struct") {
    val query = sql("select name.middle, name from contacts")
    checkScan(query, "struct<name:struct<first:string,middle:string,last:string>>")
    checkAnswer(query.orderBy("id"),
      Row("X.", Row("Jane", "X.", "Doe")) ::
        Row("Y.", Row("John", "Y.", "Doe")) ::
        Nil)
  }

  testSchemaPruning("select a single complex field and the partition column") {
    val query = sql("select name.middle, p from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"),
      Row("X.", 1) :: Row("Y.", 1) ::  Nil)
  }

  testSchemaPruning("no unnecessary schema pruning") {
    val query =
      sql("select id, name.last, name.middle, name.first, relatives[''].last, " +
        "relatives[''].middle, relatives[''].first, friends[0].last, friends[0].middle, " +
        "friends[0].first, pets, address from contacts where p=1")
    // We've selected every field in the schema. Therefore, no schema pruning should be performed.
    // We check this by asserting that the scanned schema of the query is identical to the schema
    // of the contacts relation, even though the fields are selected in different orders.
    checkScan(query,
      "struct<id:int,name:struct<first:string,middle:string,last:string>,address:string,pets:int," +
        "friends:array<struct<first:string,middle:string,last:string>>," +
        "relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(query.orderBy("id"),
      Row(0, "Doe", "X.", "Jane", null, null, null, "Smith", "Z.", "Susan", 1, "123 Main Street") ::
        Row(1, "Doe", "Y.", "John", null, null, null, null, null, null, 3, "321 Wall Street") ::
        Nil)
  }

  testSchemaPruning("select a single complex field and is null expression in project") {
    val query = sql("select name.first, address is not null from contacts")
    checkScan(query, "struct<name:struct<first:string>,address:string>")
    checkAnswer(query.orderBy("id"),
      Row("Jane", true) :: Row("John", true) :: Nil)
  }

  /**
   * Overrides this because ORC datasource doesn't support schema merging currently.
   */
  override protected def withContacts(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contacts, new File(path + "/contacts/p=1"))

      spark.read.format(dataSourceName).load(path + "/contacts").createOrReplaceTempView("contacts")

      testThunk
    }
  }

  /**
   * Overrides this because ORC datasource doesn't support schema merging currently.
   */
  override protected def withContactsWithDataPartitionColumn(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contactsWithDataPartitionColumn, new File(path + "/contacts/p=1"))

      spark.read.format(dataSourceName).load(path + "/contacts").createOrReplaceTempView("contacts")

      testThunk
    }
  }
}
