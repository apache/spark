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

import org.scalactic.Equality

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class ParquetSchemaPruningSuite
    extends QueryTest
    with ParquetTest
    with SharedSQLContext {
  case class FullName(first: String, middle: String, last: String)
  case class Contact(
    id: Int,
    name: FullName,
    address: String,
    pets: Int,
    friends: Array[FullName] = Array.empty,
    relatives: Map[String, FullName] = Map.empty)

  val janeDoe = FullName("Jane", "X.", "Doe")
  val johnDoe = FullName("John", "Y.", "Doe")
  val susanSmith = FullName("Susan", "Z.", "Smith")

  private val contacts =
    Contact(0, janeDoe, "123 Main Street", 1, friends = Array(susanSmith),
      relatives = Map("brother" -> johnDoe)) ::
    Contact(1, johnDoe, "321 Wall Street", 3, relatives = Map("sister" -> janeDoe)) :: Nil

  case class Name(first: String, last: String)
  case class BriefContact(id: Int, name: Name, address: String)

  private val briefContacts =
    BriefContact(2, Name("Janet", "Jones"), "567 Maple Drive") ::
    BriefContact(3, Name("Jim", "Jones"), "6242 Ash Street") :: Nil

  case class ContactWithDataPartitionColumn(
    id: Int,
    name: FullName,
    address: String,
    pets: Int,
    friends: Array[FullName] = Array(),
    relatives: Map[String, FullName] = Map(),
    p: Int)

  case class BriefContactWithDataPartitionColumn(id: Int, name: Name, address: String, p: Int)

  private val contactsWithDataPartitionColumn =
    contacts.map { case Contact(id, name, address, pets, friends, relatives) =>
      ContactWithDataPartitionColumn(id, name, address, pets, friends, relatives, 1) }
  private val briefContactsWithDataPartitionColumn =
    briefContacts.map { case BriefContact(id, name, address) =>
      BriefContactWithDataPartitionColumn(id, name, address, 2) }

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

  testSchemaPruning("select a single complex field array and its parent struct array") {
    val query = sql("select friends.middle, friends from contacts where p=1")
    checkScan(query,
      "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query.orderBy("id"),
      Row(Array("Z."), Array(Row("Susan", "Z.", "Smith"))) ::
      Row(Array.empty[String], Array.empty[Row]) ::
      Nil)
  }

  testSchemaPruning("select a single complex field from a map entry and its parent map entry") {
    val query =
      sql("select relatives[\"brother\"].middle, relatives[\"brother\"] from contacts where p=1")
    checkScan(query,
      "struct<relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(query.orderBy("id"),
      Row("Y.", Row("John", "Y.", "Doe")) ::
      Row(null, null) ::
      Nil)
  }

  testSchemaPruning("select a single complex field and the partition column") {
    val query = sql("select name.middle, p from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"),
      Row("X.", 1) :: Row("Y.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  ignore("partial schema intersection - select missing subfield") {
    val query = sql("select name.middle, address from contacts where p=2")
    checkScan(query, "struct<name:struct<middle:string>,address:string>")
    checkAnswer(query.orderBy("id"),
      Row(null, "567 Maple Drive") ::
      Row(null, "6242 Ash Street") :: Nil)
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

  private def testSchemaPruning(testName: String)(testThunk: => Unit) {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      test(s"Spark vectorized reader - without partition data column - $testName") {
        withContacts(testThunk)
      }
      test(s"Spark vectorized reader - with partition data column - $testName") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }

    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      test(s"Native Parquet reader - without partition data column - $testName") {
        withContacts(testThunk)
      }
      test(s"Native Parquet reader - with partition data column - $testName") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }
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

  private val schemaEquality = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => a.sameType(otherType)
        case _ => false
      }
  }

  protected def checkScan(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchemata(df, expectedSchemaCatalogStrings: _*)
    // We check here that we can execute the query without throwing an exception. The results
    // themselves are irrelevant, and should be checked elsewhere as needed
    df.collect()
  }

  private def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      df.queryExecution.executedPlan.collect {
        case scan: FileSourceScanExec => scan.requiredSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }
}
