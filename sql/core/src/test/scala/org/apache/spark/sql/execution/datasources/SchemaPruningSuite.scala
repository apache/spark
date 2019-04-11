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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.scalactic.Equality

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

abstract class SchemaPruningSuite
  extends QueryTest
  with FileBasedDataSourceTest
  with SchemaPruningTest
  with SharedSQLContext {
  case class FullName(first: String, middle: String, last: String)
  case class Company(name: String, address: String)
  case class Employer(id: Int, company: Company)
  case class Contact(
    id: Int,
    name: FullName,
    address: String,
    pets: Int,
    friends: Array[FullName] = Array.empty,
    relatives: Map[String, FullName] = Map.empty,
    employer: Employer = null,
    relations: Map[FullName, String] = Map.empty)

  val janeDoe = FullName("Jane", "X.", "Doe")
  val johnDoe = FullName("John", "Y.", "Doe")
  val susanSmith = FullName("Susan", "Z.", "Smith")

  val employer = Employer(0, Company("abc", "123 Business Street"))
  val employerWithNullCompany = Employer(1, null)

  val contacts =
    Contact(0, janeDoe, "123 Main Street", 1, friends = Array(susanSmith),
      relatives = Map("brother" -> johnDoe), employer = employer,
      relations = Map(johnDoe -> "brother")) ::
    Contact(1, johnDoe, "321 Wall Street", 3, relatives = Map("sister" -> janeDoe),
      employer = employerWithNullCompany, relations = Map(janeDoe -> "sister")) :: Nil

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
    employer: Employer = null,
    relations: Map[FullName, String] = Map(),
    p: Int)

  case class BriefContactWithDataPartitionColumn(id: Int, name: Name, address: String, p: Int)

  val contactsWithDataPartitionColumn =
    contacts.map {case Contact(id, name, address, pets, friends, relatives, employer, relations) =>
      ContactWithDataPartitionColumn(id, name, address, pets, friends, relatives, employer,
        relations, 1) }
  val briefContactsWithDataPartitionColumn =
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

  testSchemaPruning("partial schema intersection - select missing subfield") {
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

  testSchemaPruning("select a single complex field and in where clause") {
    val query1 = sql("select name.first from contacts where name.first = 'Jane'")
    checkScan(query1, "struct<name:struct<first:string>>")
    checkAnswer(query1, Row("Jane") :: Nil)

    val query2 = sql("select name.first, name.last from contacts where name.first = 'Jane'")
    checkScan(query2, "struct<name:struct<first:string,last:string>>")
    checkAnswer(query2, Row("Jane", "Doe") :: Nil)

    val query3 = sql("select name.first from contacts " +
      "where employer.company.name = 'abc' and p = 1")
    checkScan(query3, "struct<name:struct<first:string>," +
      "employer:struct<company:struct<name:string>>>")
    checkAnswer(query3, Row("Jane") :: Nil)

    val query4 = sql("select name.first, employer.company.name from contacts " +
      "where employer.company is not null and p = 1")
    checkScan(query4, "struct<name:struct<first:string>," +
      "employer:struct<company:struct<name:string>>>")
    checkAnswer(query4, Row("Jane", "abc") :: Nil)
  }

  testSchemaPruning("select nullable complex field and having is not null predicate") {
    val query = sql("select employer.company from contacts " +
      "where employer is not null and p = 1")
    checkScan(query, "struct<employer:struct<company:struct<name:string,address:string>>>")
    checkAnswer(query, Row(Row("abc", "123 Business Street")) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and is null expression in project") {
    val query = sql("select name.first, address is not null from contacts")
    checkScan(query, "struct<name:struct<first:string>,address:string>")
    checkAnswer(query.orderBy("id"),
      Row("Jane", true) :: Row("John", true) :: Row("Janet", true) :: Row("Jim", true) :: Nil)
  }

  testSchemaPruning("select a single complex field array and in clause") {
    val query = sql("select friends.middle from contacts where friends.first[0] = 'Susan'")
    checkScan(query,
      "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query.orderBy("id"),
      Row(Array("Z.")) :: Nil)
  }

  testSchemaPruning("select a single complex field from a map entry and in clause") {
    val query =
      sql("select relatives[\"brother\"].middle from contacts " +
        "where relatives[\"brother\"].first = 'John'")
    checkScan(query,
      "struct<relatives:map<string,struct<first:string,middle:string>>>")
    checkAnswer(query.orderBy("id"),
      Row("Y.") :: Nil)
  }

  testSchemaPruning("select one complex field and having is null predicate on another " +
      "complex field") {
    val query = sql("select * from contacts")
      .where("name.middle is not null")
      .select(
        "id",
        "name.first",
        "name.middle",
        "name.last"
      )
      .where("last = 'Jones'")
      .select(count("id")).toDF()
    checkScan(query,
      "struct<id:int,name:struct<middle:string,last:string>>")
    checkAnswer(query, Row(0) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field and having is null predicate on " +
      "another deep nested complex field") {
    val query = sql("select * from contacts")
      .where("employer.company.address is not null")
      .selectExpr(
        "id",
        "name.first",
        "name.middle",
        "name.last",
        "employer.id as employer_id"
      )
      .where("employer_id = 0")
      .select(count("id")).toDF()
    checkScan(query,
      "struct<id:int,employer:struct<id:int,company:struct<address:string>>>")
    checkAnswer(query, Row(1) :: Nil)
  }

  testSchemaPruning("select nested field from a complex map key using map_keys") {
    val query = sql("select map_keys(relations).middle[0], p from contacts")
    checkScan(query, "struct<relations:map<struct<middle:string>,string>>")
    checkAnswer(query, Row("Y.", 1) :: Row("X.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  testSchemaPruning("select nested field from a complex map value using map_values") {
    val query = sql("select map_values(relatives).middle[0], p from contacts")
    checkScan(query, "struct<relatives:map<string,struct<middle:string>>>")
    checkAnswer(query, Row("Y.", 1) :: Row("X.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  protected def testSchemaPruning(testName: String)(testThunk: => Unit) {
    test(s"Spark vectorized reader - without partition data column - $testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "true") {
        withContacts(testThunk)
      }
    }
    test(s"Spark vectorized reader - with partition data column - $testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "true") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }

    test(s"Non-vectorized reader - without partition data column - $testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "false") {
        withContacts(testThunk)
      }
    }
    test(s"Non-vectorized reader - with partition data column - $testName") {
      withSQLConf(vectorizedReaderEnabledKey-> "false") {
        withContactsWithDataPartitionColumn(testThunk)
      }
    }
  }

  private def withContacts(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contacts, new File(path + "/contacts/p=1"))
      makeDataSourceFile(briefContacts, new File(path + "/contacts/p=2"))

      // Providing user specified schema. Inferred schema from different data sources might
      // be different.
      val schema = "`id` INT,`name` STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>, " +
        "`address` STRING,`pets` INT,`friends` ARRAY<STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>>,`relatives` MAP<STRING, STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>>,`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, " +
        "`address`: STRING>>,`relations` MAP<STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>,STRING>,`p` INT"
      spark.read.format(dataSourceName).schema(schema).load(path + "/contacts")
        .createOrReplaceTempView("contacts")

      testThunk
    }
  }

  private def withContactsWithDataPartitionColumn(testThunk: => Unit) {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contactsWithDataPartitionColumn, new File(path + "/contacts/p=1"))
      makeDataSourceFile(briefContactsWithDataPartitionColumn, new File(path + "/contacts/p=2"))

      // Providing user specified schema. Inferred schema from different data sources might
      // be different.
      val schema = "`id` INT,`name` STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>, " +
        "`address` STRING,`pets` INT,`friends` ARRAY<STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>>,`relatives` MAP<STRING, STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>>,`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, " +
        "`address`: STRING>>,`relations` MAP<STRUCT<`first`: STRING, `middle`: STRING, " +
        "`last`: STRING>,STRING>,`p` INT"
      spark.read.format(dataSourceName).schema(schema).load(path + "/contacts")
        .createOrReplaceTempView("contacts")

      testThunk
    }
  }

  case class MixedCaseColumn(a: String, B: Int)
  case class MixedCase(id: Int, CoL1: String, coL2: MixedCaseColumn)

  private val mixedCaseData =
    MixedCase(0, "r0c1", MixedCaseColumn("abc", 1)) ::
    MixedCase(1, "r1c1", MixedCaseColumn("123", 2)) ::
    Nil

  testExactCaseQueryPruning("select with exact column names") {
    val query = sql("select CoL1, coL2.B from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(query.orderBy("id"),
      Row("r0c1", 1) ::
      Row("r1c1", 2) ::
      Nil)
  }

  testMixedCaseQueryPruning("select with lowercase column names") {
    val query = sql("select col1, col2.b from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(query.orderBy("id"),
      Row("r0c1", 1) ::
      Row("r1c1", 2) ::
      Nil)
  }

  testMixedCaseQueryPruning("select with different-case column names") {
    val query = sql("select cOL1, cOl2.b from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(query.orderBy("id"),
      Row("r0c1", 1) ::
      Row("r1c1", 2) ::
      Nil)
  }

  testMixedCaseQueryPruning("filter with different-case column names") {
    val query = sql("select id from mixedcase where Col2.b = 2")
    checkScan(query, "struct<id:int,coL2:struct<B:int>>")
    checkAnswer(query.orderBy("id"), Row(1) :: Nil)
  }

  // Tests schema pruning for a query whose column and field names are exactly the same as the table
  // schema's column and field names. N.B. this implies that `testThunk` should pass using either a
  // case-sensitive or case-insensitive query parser
  private def testExactCaseQueryPruning(testName: String)(testThunk: => Unit) {
    test(s"Case-sensitive parser - mixed-case schema - $testName") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        withMixedCaseData(testThunk)
      }
    }
    testMixedCaseQueryPruning(testName)(testThunk)
  }

  // Tests schema pruning for a query whose column and field names may differ in case from the table
  // schema's column and field names
  private def testMixedCaseQueryPruning(testName: String)(testThunk: => Unit) {
    test(s"Case-insensitive parser - mixed-case schema - $testName") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        withMixedCaseData(testThunk)
      }
    }
  }

  // Tests given test function with Spark vectorized reader and non-vectorized reader.
  private def withMixedCaseData(testThunk: => Unit) {
    withDataSourceTable(mixedCaseData, "mixedcase") {
      testThunk
    }
  }

  protected val schemaEquality = new Equality[StructType] {
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

  protected def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
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
