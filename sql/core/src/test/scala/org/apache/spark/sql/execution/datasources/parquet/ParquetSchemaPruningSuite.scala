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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.FileSchemaPruningTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetSchemaPruningSuite
    extends QueryTest
    with ParquetTest
    with FileSchemaPruningTest
    with SharedSQLContext {
  case class FullName(first: String, middle: String, last: String)
  case class Contact(name: FullName, address: String, pets: Int, friends: Array[FullName] = Array(),
    relatives: Map[String, FullName] = Map())

  val contacts =
    Contact(FullName("Jane", "X.", "Doe"), "123 Main Street", 1) ::
    Contact(FullName("John", "Y.", "Doe"), "321 Wall Street", 3) :: Nil

  case class Name(first: String, last: String)
  case class BriefContact(name: Name, address: String)

  val briefContacts =
    BriefContact(Name("Janet", "Jones"), "567 Maple Drive") ::
    BriefContact(Name("Jim", "Jones"), "6242 Ash Street") :: Nil

  case class ContactWithDataPartitionColumn(name: FullName, address: String, pets: Int,
    friends: Array[FullName] = Array(), relatives: Map[String, FullName] = Map(), p: Int)

  case class BriefContactWithDataPartitionColumn(name: Name, address: String, p: Int)

  val contactsWithDataPartitionColumn =
    contacts.map { case Contact(name, address, pets, friends, relatives) =>
      ContactWithDataPartitionColumn(name, address, pets, friends, relatives, 1) }
  val briefContactsWithDataPartitionColumn =
    briefContacts.map { case BriefContact(name: Name, address: String) =>
      BriefContactWithDataPartitionColumn(name, address, 2) }

  testSchemaPruning("select a single complex field") {
    val query = sql("select name.middle from contacts")
    checkScanSchemata(query, "struct<name:struct<middle:string>>")
    checkAnswer(query, Row("X.") :: Row("Y.") :: Row(null) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and the partition column") {
    val query = sql("select name.middle, p from contacts")
    checkScanSchemata(query, "struct<name:struct<middle:string>>")
    checkAnswer(query, Row("X.", 1) :: Row("Y.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  testSchemaPruning("partial schema intersection - select missing subfield") {
    val query = sql("select name.middle, address from contacts where p=2")
    checkScanSchemata(query, "struct<name:struct<middle:string>,address:string>")
    checkAnswer(query,
      Row(null, "567 Maple Drive") ::
      Row(null, "6242 Ash Street") :: Nil)
  }

  testSchemaPruning("partial schema intersection - filter on subfield") {
    val query =
      sql("select name.middle, name.first, pets, address from contacts where " +
        "name.first = 'Janet' and p=2")
    checkScanSchemata(query,
      "struct<name:struct<middle:string,first:string>,pets:int,address:string>")
    checkAnswer(query,
      Row(null, "Janet", null, "567 Maple Drive") :: Nil)
  }

  testSchemaPruning("no unnecessary schema pruning") {
    val query =
      sql("select name.last, name.middle, name.first, relatives[''].last, " +
        "relatives[''].middle, relatives[''].first, friends[0].last, friends[0].middle, " +
        "friends[0].first, pets, address from contacts where p=2")
    // We've selected every field in the schema. Therefore, no schema pruning should be performed.
    // We check this by asserting that the scanned schema of the query is identical to the schema
    // of the contacts relation, even though the fields are selected in different orders.
    checkScanSchemata(query,
      "struct<name:struct<first:string,middle:string,last:string>,address:string,pets:int," +
      "friends:array<struct<first:string,middle:string,last:string>>," +
      "relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(query,
      Row("Jones", null, "Janet", null, null, null, null, null, null, null, "567 Maple Drive") ::
      Row("Jones", null, "Jim", null, null, null, null, null, null, null, "6242 Ash Street") ::
      Nil)
  }

  testSchemaPruning("empty schema intersection") {
    val query = sql("select name.middle from contacts where p=2")
    checkScanSchemata(query, "struct<name:struct<middle:string>>")
    checkAnswer(query,
      Row(null) :: Row(null) :: Nil)
  }

  private def testSchemaPruning(testName: String)(testThunk: => Unit) {
    withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false") {
      test(s"Standard mode - without partition data column - $testName") {
        withContacts(testThunk)
      }
      test(s"Standard mode - with partition data column - $testName") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }

    withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
      test(s"Legacy mode - without partition data column - $testName") {
        withContacts(testThunk)
      }
      test(s"Legacy mode - with partition data column - $testName") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }
  }

  private def withContactTables(testThunk: => Unit) {
    info("testing table without partition data column")
    withContacts(testThunk)
    info("testing table with partition data column")
    withContactsWithDataPartitionColumn(testThunk)
  }

  private def withContacts(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeParquetFile(contacts, new File(path + "/contacts/p=1"))
      makeParquetFile(briefContacts, new File(path + "/contacts/p=2"))

      spark.read.parquet(path + "/contacts").createOrReplaceTempView("contacts")

      testThunk
    }
  }

  private def withContactsWithDataPartitionColumn(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeParquetFile(contactsWithDataPartitionColumn, new File(path + "/contacts/p=1"))
      makeParquetFile(briefContactsWithDataPartitionColumn, new File(path + "/contacts/p=2"))

      spark.read.parquet(path + "/contacts").createOrReplaceTempView("contacts")

      testThunk
    }
  }
}
