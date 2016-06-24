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
import org.apache.spark.sql.test.SharedSQLContext

class ParquetSchemaPruningSuite
    extends QueryTest
    with ParquetTest
    with FileSchemaPruningTest
    with SharedSQLContext {
  case class FullName(first: String, middle: String, last: String)
  case class Contact(name: FullName, address: String, pets: Int)

  val contacts =
    Contact(FullName("Jane", "X.", "Doe"), "123 Main Street", 1) ::
    Contact(FullName("John", "Y.", "Doe"), "321 Wall Street", 3) :: Nil

  case class Name(first: String, last: String)
  case class BriefContact(name: Name, address: String)

  val briefContacts =
    BriefContact(Name("sdfaf", "123"), "xyz") ::
    BriefContact(Name("asdfaf", "xyz"), "abc") :: Nil

  testStandardAndLegacyModes("partial schema intersection") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeParquetFile(contacts, new File(path + "/contacts/p=1"))
      makeParquetFile(briefContacts, new File(path + "/contacts/p=2"))

      spark.read.parquet(path + "/contacts").createOrReplaceTempView("contacts")

      val query = sql("select name.middle, address from contacts where p=2")
      checkScanSchemata(query, "struct<name:struct<middle:string>,address:string>")
      checkAnswer(query,
        Row(null, "xyz") ::
        Row(null, "abc") :: Nil)
    }
  }

  testStandardAndLegacyModes("empty schema intersection") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeParquetFile(contacts, new File(path + "/contacts/p=1"))
      makeParquetFile(briefContacts, new File(path + "/contacts/p=2"))

      spark.read.parquet(path + "/contacts").createOrReplaceTempView("contacts")

      val query = sql("select name.middle from contacts where p=2")
      checkScanSchemata(query, "struct<name:struct<middle:string>>")
      checkAnswer(query,
        Row(null) :: Row(null) :: Nil)
    }
  }

  testStandardAndLegacyModes("aggregation over nested data") {
    withParquetTable(contacts, "contacts") {
      val query = sql("select count(distinct name.last), address from contacts group by address " +
        "order by address")
      checkScanSchemata(query, "struct<address:string,name:struct<last:string>>")
      checkAnswer(query,
        Row(1, "123 Main Street") ::
        Row(1, "321 Wall Street") :: Nil)
    }
  }

  testStandardAndLegacyModes("select function over nested data") {
    withParquetTable(contacts, "contacts") {
      val query = sql("select count(name.middle) from contacts")
      checkScanSchemata(query, "struct<name:struct<middle:string>>")
      checkAnswer(query,
        Row(2) :: Nil)
    }
  }
}
