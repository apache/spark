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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExecBase, FileScan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class SchemaPruningSuite
  extends QueryTest
  with FileBasedDataSourceTest
  with SchemaPruningTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
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
  case class Department(
    depId: Int,
    depName: String,
    contactId: Int,
    employer: Employer)

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")

  case class Employee(id: Int, name: FullName, employer: Company)

  val janeDoe = FullName("Jane", "X.", "Doe")
  val johnDoe = FullName("John", "Y.", "Doe")
  val susanSmith = FullName("Susan", "Z.", "Smith")

  val company = Company("abc", "123 Business Street")

  val employer = Employer(0, company)
  val employerWithNullCompany = Employer(1, null)
  val employerWithNullCompany2 = Employer(2, null)

  val contacts =
    Contact(0, janeDoe, "123 Main Street", 1, friends = Array(susanSmith),
      relatives = Map("brother" -> johnDoe), employer = employer,
      relations = Map(johnDoe -> "brother")) ::
    Contact(1, johnDoe, "321 Wall Street", 3, relatives = Map("sister" -> janeDoe),
      employer = employerWithNullCompany, relations = Map(janeDoe -> "sister")) :: Nil

  val departments =
    Department(0, "Engineering", 0, employer) ::
    Department(1, "Marketing", 1, employerWithNullCompany) ::
    Department(2, "Operation", 4, employerWithNullCompany2) :: Nil

  val employees = Employee(0, janeDoe, company) :: Employee(1, johnDoe, company) :: Nil

  // Test data for chained generators - mimics real-world nested array structures
  case class ServedItem(id: Int, name: String, clicked: Boolean, revenue: Double)
  case class Request(available: Boolean, clientMode: String, servedItems: Array[ServedItem])
  case class Session(
    publisherId: Long,
    endOfSession: Long,
    requests: Array[Request])

  val servedItems1 = Array(
    ServedItem(1, "item1", clicked = true, revenue = 10.0),
    ServedItem(2, "item2", clicked = false, revenue = 0.0))
  val servedItems2 = Array(
    ServedItem(3, "item3", clicked = true, revenue = 15.0))

  val sessions =
    Session(100L, 1234567890L,
      requests = Array(
        Request(available = true, clientMode = "mobile", servedItems = servedItems1),
        Request(available = false, clientMode = "desktop", servedItems = servedItems2))) ::
    Session(200L, 1234567900L,
      requests = Array(
        Request(available = true, clientMode = "mobile", servedItems = Array.empty))) :: Nil

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

  testSchemaPruning("select only top-level fields") {
    val query = sql("select address from contacts")
    checkScan(query, "struct<address:string>")
    checkAnswer(query.orderBy("id"),
      Row("123 Main Street") ::
      Row("321 Wall Street") ::
      Row("567 Maple Drive") ::
      Row("6242 Ash Street") ::
      Nil)
  }

  testSchemaPruning("select a single complex field with disabled nested schema pruning") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
      val query = sql("select name.middle from contacts")
      checkScan(query, "struct<name:struct<first:string,middle:string,last:string>>")
      checkAnswer(query.orderBy("id"), Row("X.") :: Row("Y.") :: Row(null) :: Row(null) :: Nil)
    }
  }

  testSchemaPruning("select only input_file_name()") {
    val query = sql("select input_file_name() from contacts")
    checkScan(query, "struct<>")
  }

  testSchemaPruning("select only expressions without references") {
    val query = sql("select count(*) from contacts")
    checkScan(query, "struct<>")
    checkAnswer(query, Row(4))
  }

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

  testSchemaPruning("select explode of nested field of array of struct") {
    // Config combinations
    val configs = Seq((true, true), (true, false), (false, true), (false, false))

    configs.foreach { case (nestedPruning, nestedPruningOnExpr) =>
      withSQLConf(
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedPruning.toString,
          SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key -> nestedPruningOnExpr.toString) {
        val query1 = spark.table("contacts")
          .select(explode(col("friends.first")))
        if (nestedPruning) {
          // If `NESTED_SCHEMA_PRUNING_ENABLED` is enabled,
          // even disabling `NESTED_PRUNING_ON_EXPRESSIONS`,
          // nested schema is still pruned at scan node.
          checkScan(query1, "struct<friends:array<struct<first:string>>>")
        } else {
          checkScan(query1, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        }
        checkAnswer(query1, Row("Susan") :: Nil)

        val query2 = spark.table("contacts")
          .select(explode(col("friends.first")), col("friends.middle"))
        if (nestedPruning) {
          checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
        } else {
          checkScan(query2, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        }
        checkAnswer(query2, Row("Susan", Array("Z.")) :: Nil)

        val query3 = spark.table("contacts")
          .select(explode(col("friends.first")), col("friends.middle"), col("friends.last"))
        checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        checkAnswer(query3, Row("Susan", Array("Z."), Array("Smith")) :: Nil)
      }
    }
  }

  testSchemaPruning("SPARK-34638: nested column prune on generator output") {
    val query1 = spark.table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first")
    checkScan(query1, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query1, Row("Susan") :: Nil)

    // SPARK-47230: Now we DO prune multiple field case!
    val query2 = spark.table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first", "friend.middle")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    val query3 = spark.table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first", "friend.middle", "friend")
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row("Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)
  }

  testSchemaPruning("SPARK-34638: nested column prune on generator output - case-sensitivity") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val query1 = spark.table("contacts")
        .select(explode(col("friends")).as("friend"))
        .select("friend.First")
      checkScan(query1, "struct<friends:array<struct<first:string>>>")
      checkAnswer(query1, Row("Susan") :: Nil)

      val query2 = spark.table("contacts")
        .select(explode(col("friends")).as("friend"))
        .select("friend.MIDDLE")
      checkScan(query2, "struct<friends:array<struct<middle:string>>>")
      checkAnswer(query2, Row("Z.") :: Nil)
    }
  }

  testSchemaPruning("SPARK-41961: nested schema pruning on table-valued generator functions") {
    val query1 = sql("select friend.first from contacts, lateral explode(friends) t(friend)")
    checkScan(query1, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query1, Row("Susan") :: Nil)

    // SPARK-47230: Now we DO prune multiple field case!
    val query2 = sql(
      "select friend.first, friend.middle from contacts, lateral explode(friends) t(friend)")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    val query3 = sql(
      """
        |select friend.first, friend.middle, friend
        |from contacts, lateral explode(friends) t(friend)
        |""".stripMargin)
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row("Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)
  }

  testSchemaPruning("SPARK-47230: nested column prune on POSEXPLODE output") {
    // Single field access
    val query1 = spark.table("contacts")
      .select(posexplode(col("friends")).as(Seq("pos", "friend")))
      .select("friend.first")
    checkScan(query1, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query1, Row("Susan") :: Nil)

    // Multiple field access - this is the NEW capability enabled by SPARK-47230
    val query2 = spark.table("contacts")
      .select(posexplode(col("friends")).as(Seq("pos", "friend")))
      .select("friend.first", "friend.middle")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    // All fields including the whole struct
    val query3 = spark.table("contacts")
      .select(posexplode(col("friends")).as(Seq("pos", "friend")))
      .select("pos", "friend.first", "friend.middle", "friend")
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row(0, "Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)

    // Using position in selection
    val query4 = spark.table("contacts")
      .select(posexplode(col("friends")).as(Seq("pos", "friend")))
      .select("pos", "friend.last")
    checkScan(query4, "struct<friends:array<struct<last:string>>>")
    checkAnswer(query4, Row(0, "Smith") :: Nil)
  }

  testSchemaPruning("SPARK-47230: nested schema pruning on POSEXPLODE with LATERAL VIEW") {
    // Single field
    val query1 = sql(
      "select friend.first from contacts, lateral posexplode(friends) t(pos, friend)")
    checkScan(query1, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query1, Row("Susan") :: Nil)

    // Multiple fields - NEW capability
    val query2 = sql(
      """select friend.first, friend.middle
        |from contacts, lateral posexplode(friends) t(pos, friend)""".stripMargin)
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    // With position
    val query3 = sql(
      """
        |select pos, friend.first, friend.last
        |from contacts, lateral posexplode(friends) t(pos, friend)
        |""".stripMargin)
    checkScan(query3, "struct<friends:array<struct<first:string,last:string>>>")
    checkAnswer(query3, Row(0, "Susan", "Smith") :: Nil)

    // All fields
    val query4 = sql(
      """
        |select pos, friend.first, friend.middle, friend
        |from contacts, lateral posexplode(friends) t(pos, friend)
        |""".stripMargin)
    val friendsSchema =
      "struct<friends:array<struct<first:string,middle:string,last:string>>>"
    checkScan(query4, friendsSchema)
    checkAnswer(query4, Row(0, "Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)
  }

  testSchemaPruning(
    "SPARK-47230: posexplode keeps requested nested block reasons without widening structs") {
    withSQLConf(SQLConf.OPTIMIZER_V2_PENDING_SCAN_ENABLED.key -> "true") {
      val jsonData =
        """
          |[
          |  {
          |    "publisherId": 1,
          |    "endOfSession": 1000,
          |    "pv_requests": [
          |      {
          |        "available": true,
          |        "rankerVerboseDataList": [
          |          {
          |            "blockedItemsList": [
          |              {
          |                "blockReason": "reason_r0_req0_rv0",
          |                "extra": "extra_r0_req0_rv0"
          |              },
          |              { "blockReason": "second_r0_req0_rv1" }
          |            ]
          |          }
          |        ],
          |        "servedItems": [ { "id": 1, "name": "item_0_0", "clicked": true } ]
          |      },
          |      {
          |        "available": false,
          |        "rankerVerboseDataList": [
          |          {
          |            "blockedItemsList": [
          |              {
          |                "blockReason": "reason_r0_req1_rv0",
          |                "extra": "extra_r0_req1_rv0"
          |              }
          |            ]
          |          }
          |        ],
          |        "servedItems": [ { "id": 11, "name": "item_0_1", "clicked": false } ]
          |      }
          |    ]
          |  },
          |  {
          |    "publisherId": 2,
          |    "endOfSession": 1001,
          |    "pv_requests": [
          |      {
          |        "available": true,
          |        "rankerVerboseDataList": [
          |          {
          |            "blockedItemsList": [
          |              {
          |                "blockReason": "reason_r1_req0_rv0",
          |                "extra": "extra_r1_req0_rv0"
          |              },
          |              { "blockReason": "second_r1_req0_rv1" }
          |            ]
          |          }
          |        ],
          |        "servedItems": [ { "id": 1, "name": "item_1_0", "clicked": false } ]
          |      },
          |      {
          |        "available": false,
          |        "rankerVerboseDataList": [
          |          {
          |            "blockedItemsList": [
          |              {
          |                "blockReason": "reason_r1_req1_rv0",
          |                "extra": "extra_r1_req1_rv0"
          |              }
          |            ]
          |          }
          |        ],
          |        "servedItems": [ { "id": 11, "name": "item_1_1", "clicked": true } ]
          |      }
          |    ]
          |  }
          |]
          |""".stripMargin

      withTempPath { path =>
        val jsonDS = spark.createDataset(Seq(jsonData))(org.apache.spark.sql.Encoders.STRING)
        val rawDF = spark.read.json(jsonDS)
        rawDF.write.mode("overwrite").parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("rawdata")

        val result = spark.sql(
          """
            |WITH pageviews AS (
            |  SELECT *,
            |         request.available AS request_available
            |  FROM rawdata
            |  LATERAL VIEW OUTER posexplode(pv_requests) AS requestIdx, request
            |  LATERAL VIEW OUTER posexplode(request.servedItems) AS servedItemIdx, servedItem
            |)
            |SELECT request.rankerVerboseDataList.blockedItemsList[0].blockReason AS blockReason,
            |       request.available AS request_available
            |FROM pageviews
            |""".stripMargin)

        val hasV2Scan = result.queryExecution.executedPlan.exists {
          case _: DataSourceV2ScanExecBase => true
          case _ => false
        }
        val schemaPrefix =
          "struct<pv_requests:array<struct<available:boolean," +
            "rankerVerboseDataList:array<struct<blockedItemsList:" +
            "array<struct<blockReason:string>>>>,"
        val expectedSchema =
          if (hasV2Scan) {
            schemaPrefix + "servedItems:array<struct<>>>>>"
          } else {
            schemaPrefix +
              "servedItems:array<struct<clicked:boolean,id:bigint,name:string>>>>>"
          }

        checkScan(result, expectedSchema)

        checkAnswer(
          result,
          Row(Seq("reason_r0_req0_rv0", "second_r0_req0_rv1"), true) ::
            Row(Seq("reason_r0_req1_rv0"), false) ::
            Row(Seq("reason_r1_req0_rv0", "second_r1_req0_rv1"), true) ::
            Row(Seq("reason_r1_req1_rv0"), false) :: Nil)
      }
    }
  }

  testSchemaPruning("SPARK-47230: multi-field pruning with case-insensitive EXPLODE") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // Mixed case field names with multiple fields
      val query1 = spark.table("contacts")
        .select(explode(col("friends")).as("friend"))
        .select("friend.FIRST", "friend.Middle")
      checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query1, Row("Susan", "Z.") :: Nil)

      // With LATERAL VIEW
      val query2 = sql(
        "select friend.FiRsT, friend.MiDdLe from contacts, lateral explode(friends) t(friend)")
      checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query2, Row("Susan", "Z.") :: Nil)
    }
  }

  testSchemaPruning("SPARK-47230: multi-field pruning with case-insensitive POSEXPLODE") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // Mixed case with POSEXPLODE
      val query1 = spark.table("contacts")
        .select(posexplode(col("friends")).as(Seq("pos", "friend")))
        .select("friend.First", "friend.MIDDLE")
      checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query1, Row("Susan", "Z.") :: Nil)

      // With LATERAL VIEW
      val query2 = sql(
        """select friend.LAST, friend.first
          |from contacts, lateral posexplode(friends) t(pos, friend)""".stripMargin)
      checkScan(query2, "struct<friends:array<struct<first:string,last:string>>>")
      checkAnswer(query2, Row("Smith", "Susan") :: Nil)
    }
  }

  testSchemaPruning("SPARK-47230: chained LATERAL VIEWs with multi-field pruning") {
    // Direct query on contacts table with LATERAL VIEW
    val query = sql(
      """
        |select name.first as contact_name, friend.first as friend_first, friend.middle
        |from contacts
        |lateral view explode(friends) t1 as friend
        |where id = 0
        |""".stripMargin)
    checkScan(query, "struct<id:int,name:struct<first:string>," +
      "friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query, Row("Jane", "Susan", "Z.") :: Nil)
  }

  testSchemaPruning("SPARK-47230: EXPLODE with filter predicates on nested fields") {
    // Filter on exploded struct fields - should only prune to needed fields
    val query1 = spark.table("contacts")
      .select(explode(col("friends")).as("friend"))
      .where("friend.middle = 'Z.'")
      .select("friend.first", "friend.middle")
    checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query1, Row("Susan", "Z.") :: Nil)

    // SQL syntax with WHERE clause
    val query2 = sql(
      """
        |select friend.first, friend.last
        |from contacts, lateral explode(friends) t(friend)
        |where friend.middle is not null
        |""".stripMargin)
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query2, Row("Susan", "Smith") :: Nil)

    // Complex filter with multiple conditions
    val query3 = sql(
      """
        |select friend.first
        |from contacts, lateral explode(friends) t(friend)
        |where friend.first = 'Susan' and friend.middle = 'Z.'
        |""".stripMargin)
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query3, Row("Susan") :: Nil)
  }

  testSchemaPruning("SPARK-47230: POSEXPLODE with filter on position and nested fields") {
    // Filter using position
    val query1 = sql(
      """
        |select friend.first, friend.middle
        |from contacts, lateral posexplode(friends) t(pos, friend)
        |where pos = 0
        |""".stripMargin)
    checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query1, Row("Susan", "Z.") :: Nil)

    // Filter on both position and field
    val query2 = sql(
      """
        |select pos, friend.last
        |from contacts, lateral posexplode(friends) t(pos, friend)
        |where pos < 1 and friend.first = 'Susan'
        |""".stripMargin)
    checkScan(query2, "struct<friends:array<struct<first:string,last:string>>>")
    checkAnswer(query2, Row(0, "Smith") :: Nil)
  }

  testSchemaPruning("SPARK-47230: aggregate functions on exploded multi-field structs") {
    // Aggregate on multiple fields from exploded array
    val query1 = sql(
      """
        |select count(distinct friend.first), count(distinct friend.middle)
        |from contacts, lateral explode(friends) t(friend)
        |""".stripMargin)
    checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query1, Row(1, 1) :: Nil)

    // Group by with exploded fields
    val query2 = sql(
      """
        |select friend.first, count(*)
        |from contacts, lateral explode(friends) t(friend)
        |group by friend.first
        |""".stripMargin)
    checkScan(query2, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query2, Row("Susan", 1) :: Nil)
  }

  testSchemaPruning("SPARK-47230: join after posexplode with multi-field pruning") {
    // Join contacts with departments after posexploding friends array
    // This tests that schema pruning works correctly when joining exploded data
    val query = sql(
      """
        |select c.name.first, pos, friend.first, friend.middle, d.depName
        |from contacts c
        |cross join lateral posexplode(c.friends) t(pos, friend)
        |join departments d on c.id = d.contactId
        |where friend.first = 'Susan'
        |""".stripMargin)
    // Schema pruning should only read the fields we need from each table:
    // - contacts: id (for join), name.first, friends.first, friends.middle
    // - departments: contactId (for join), depName
    checkScan(query,
      "struct<id:int,name:struct<first:string>,friends:array<struct<first:string,middle:string>>>",
      "struct<depName:string,contactId:int>")
    checkAnswer(query, Row("Jane", 0, "Susan", "Z.", "Engineering") :: Nil)
  }

  testSchemaPruning("SPARK-47230: CTE with SELECT * and EXPLODE should not over-preserve") {
    // Test that CTE intermediate `SELECT *` doesn't cause false positives
    // The fix should only preserve full struct when both direct reference AND
    // GetStructField accesses occur in the SAME Project, not across CTE boundaries
    val query1 = sql(
      """
        |WITH cte AS (
        |  SELECT * FROM contacts, LATERAL EXPLODE(friends) t(friend)
        |)
        |SELECT friend.first, friend.middle FROM cte WHERE id = 0
        |""".stripMargin)
    // Should prune to only first and middle fields, not preserve full struct
    checkScan(query1, "struct<id:int,friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query1, Row("Susan", "Z.") :: Nil)

    // Variation: CTE with explicit columns
    val query2 = sql(
      """
        |WITH exploded AS (
        |  SELECT id, friend FROM contacts, LATERAL EXPLODE(friends) t(friend)
        |)
        |SELECT friend.first, friend.last FROM exploded WHERE id = 0
        |""".stripMargin)
    checkScan(query2, "struct<id:int,friends:array<struct<first:string,last:string>>>")
    checkAnswer(query2, Row("Susan", "Smith") :: Nil)
  }

  testSchemaPruning("SPARK-47230: CTE with SELECT * and POSEXPLODE should not over-preserve") {
    // Similar test for POSEXPLODE to ensure CTE doesn't cause false positives
    val query1 = sql(
      """
        |WITH cte AS (
        |  SELECT * FROM contacts, LATERAL POSEXPLODE(friends) t(pos, friend)
        |)
        |SELECT friend.first, friend.middle FROM cte WHERE id = 0
        |""".stripMargin)
    // Should prune to only first and middle fields
    checkScan(query1, "struct<id:int,friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query1, Row("Susan", "Z.") :: Nil)

    // Variation: CTE with position in final SELECT
    val query2 = sql(
      """
        |WITH exploded AS (
        |  SELECT * FROM contacts, LATERAL POSEXPLODE(friends) t(pos, friend)
        |)
        |SELECT pos, friend.first, friend.last FROM exploded WHERE id = 0
        |""".stripMargin)
    checkScan(query2, "struct<id:int,friends:array<struct<first:string,last:string>>>")
    checkAnswer(query2, Row(0, "Susan", "Smith") :: Nil)
  }

  testSchemaPruning("SPARK-47230: CTE with chained EXPLODE generators") {
    // Test chained LATERAL VIEW OUTER explode similar to real-world queries
    // This simulates: SELECT *, field FROM table LATERAL VIEW explode(...)
    // LATERAL VIEW explode(...)

    // Since contacts.friends is Array[FullName] (not nested arrays), we simulate
    // chained generators by exploding friends array and then using map_values on relatives
    val query1 = sql(
      """
        |WITH exploded_data AS (
        |  SELECT *, friend.first as friend_first
        |  FROM contacts
        |  LATERAL VIEW OUTER explode(friends) t1 AS friend
        |  WHERE p = 1
        |)
        |SELECT friend_first, friend.middle, friend.last
        |FROM exploded_data
        |WHERE id = 0
        |""".stripMargin)
    // Should prune to only the fields we actually use
    checkScan(query1,
      "struct<id:int,friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query1, Row("Susan", "Z.", "Smith") :: Nil)

    // Aggregate query after chained generators (similar to user's real query pattern)
    val query2 = sql(
      """
        |WITH exploded_data AS (
        |  SELECT *, friend.first as friend_first
        |  FROM contacts
        |  LATERAL VIEW OUTER explode(friends) t1 AS friend
        |  WHERE p = 1
        |)
        |SELECT count(*), max(friend.middle), sum(if(friend.first = 'Susan', 1, 0))
        |FROM exploded_data
        |GROUP BY id
        |""".stripMargin)
    checkScan(query2, "struct<id:int,friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row(1, "Z.", 1) :: Row(1, null, 0) :: Nil)
  }

  testSchemaPruning("SPARK-47230: CTE with chained POSEXPLODE generators") {
    // Test chained LATERAL VIEW OUTER posexplode similar to real-world queries
    // Pattern: CTE with SELECT *, field extraction, then chained posexplodes with aggregation

    val query1 = sql(
      """
        |WITH exploded_data AS (
        |  SELECT *, friend.first as friend_first
        |  FROM contacts
        |  LATERAL VIEW OUTER posexplode(friends) t1 AS friendPos, friend
        |  WHERE p = 1
        |)
        |SELECT friendPos, friend_first, friend.middle, friend.last
        |FROM exploded_data
        |WHERE id = 0
        |""".stripMargin)
    // Should prune to only the fields we actually use
    checkScan(query1,
      "struct<id:int,friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query1, Row(0, "Susan", "Z.", "Smith") :: Nil)

    // Aggregate query with posexplode (matching user's real query pattern)
    val query2 = sql(
      """
        |WITH exploded_data AS (
        |  SELECT *, friend.middle as friend_middle
        |  FROM contacts
        |  LATERAL VIEW OUTER posexplode(friends) t1 AS friendPos, friend
        |  WHERE p = 1
        |)
        |SELECT
        |  count(*) as cnt,
        |  max(id) as max_id,
        |  sum(if(friend.first = 'Susan', 1, 0)) as susan_count,
        |  sum(if(friend_middle IS NOT NULL, 1, 0)) as has_middle
        |FROM exploded_data
        |GROUP BY id
        |""".stripMargin)
    checkScan(query2, "struct<id:int,friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query2, Row(1, 0, 1, 1) :: Row(1, 1, 0, 0) :: Nil)

    // Query with both position and nested field access in WHERE clause
    val query3 = sql(
      """
        |WITH exploded_data AS (
        |  SELECT *
        |  FROM contacts
        |  LATERAL VIEW OUTER posexplode(friends) t1 AS friendPos, friend
        |)
        |SELECT friend.first, friend.last
        |FROM exploded_data
        |WHERE friendPos = 0 AND friend.middle = 'Z.'
        |""".stripMargin)
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row("Susan", "Smith") :: Nil)
  }


  // Case classes for nested data with 4 fields at each level
  case class InnerStruct(a: Int, b: Int, c: Int, d: Int)
  case class MiddleStruct(w: Int, x: Int, y: Int, z: Int, nested: Array[InnerStruct])
  case class OuterData(
    id: Int, name: String, extra1: Int, extra2: String, items: Array[MiddleStruct])

  private val nestedChainedData =
    OuterData(1, "test", 100, "unused1",
      Array(
        MiddleStruct(5, 10, 20, 25, Array(InnerStruct(1, 3, 7, 9), InnerStruct(2, 4, 8, 10))),
        MiddleStruct(15, 30, 40, 45, Array(InnerStruct(5, 6, 11, 12)))
      )) :: Nil

  testSchemaPruning(
    "SPARK-47230: CTE with SELECT * over 2 chained EXPLODE selecting from all levels") {
    // Test CTE with SELECT * over chained generators with 4+ fields at each level,
    // but only selecting 2 fields from each level to validate pruning of the other 2 fields:
    // - Top level: 4 table columns (id, name, extra1, extra2) - select only 2 (id, name)
    // - 1st generator level: 4 struct fields (w, x, y, z) - select only 2 (x, y)
    // - 2nd generator level: 4 struct fields (a, b, c, d) - select only 2 (a, b)
    withDataSourceTable(nestedChainedData, "nested_data") {
      // CTE with SELECT * over 2 chained explodes (each over array of structs)
      // Only select 2 fields from each level - others should be pruned
      val query = sql(
        """
          |WITH exploded AS (
          |  SELECT *
          |  FROM nested_data
          |  LATERAL VIEW OUTER explode(items) t1 AS item1
          |  LATERAL VIEW OUTER explode(item1.nested) t2 AS item2
          |)
          |SELECT id, name, item1.x, item1.y, item2.a, item2.b
          |FROM exploded
          |""".stripMargin)

      // Should prune unused fields:
      // - Top level: extra1, extra2 should be pruned (only id, name selected)
      // - 1st level: w, z, nested should be pruned from item1 (only x, y selected from item1)
      // - 2nd level: c, d should be pruned (only a, b selected)
      checkScan(query,
        "struct<id:int,name:string," +
        "items:array<struct<x:int,y:int,nested:array<struct<a:int,b:int>>>>>")
      checkAnswer(query,
        Row(1, "test", 10, 20, 1, 3) ::
        Row(1, "test", 10, 20, 2, 4) ::
        Row(1, "test", 30, 40, 5, 6) :: Nil)
    }
  }

  testSchemaPruning(
    "SPARK-47230: CTE with SELECT * over 2 chained POSEXPLODE selecting from all levels") {
    // Test CTE with SELECT * over chained posexplode with 4+ fields at each level,
    // but only selecting 2 fields from each level to validate pruning of the other 2 fields:
    // - Top level: 4 table columns (id, name, extra1, extra2) - select only 2 (id, name)
    // - 1st generator level: position + 4 struct fields (w, x, y, z)
    //   select only pos + 2 fields (x, y)
    // - 2nd generator level: position + 4 struct fields (a, b, c, d)
    //   select only pos + 2 fields (a, b)
    withDataSourceTable(nestedChainedData, "nested_data") {
      // CTE with SELECT * over 2 chained posexplodes
      // Only select 2 fields from each level (plus positions) - others should be pruned
      val query = sql(
        """
          |WITH exploded AS (
          |  SELECT *
          |  FROM nested_data
          |  LATERAL VIEW OUTER posexplode(items) t1 AS pos1, item1
          |  LATERAL VIEW OUTER posexplode(item1.nested) t2 AS pos2, item2
          |)
          |SELECT id, name, pos1, item1.x, item1.y, pos2, item2.a, item2.b
          |FROM exploded
          |""".stripMargin)

      // Should prune unused fields:
      // - Top level: extra1, extra2 should be pruned (only id, name selected)
      // - 1st level: w, z should be pruned (only pos1, x, y selected)
      // - 2nd level: c, d should be pruned (only pos2, a, b selected)
      checkScan(query,
        "struct<id:int,name:string," +
        "items:array<struct<x:int,y:int,nested:array<struct<a:int,b:int>>>>>")
      checkAnswer(query,
        Row(1, "test", 0, 10, 20, 0, 1, 3) ::
        Row(1, "test", 0, 10, 20, 1, 2, 4) ::
        Row(1, "test", 1, 30, 40, 0, 5, 6) :: Nil)
    }
  }

  testSchemaPruning(
    "SPARK-47230: chained POSEXPLODE with SELECT * on session requests prunes to thin schema") {
    withSQLConf(SQLConf.OPTIMIZER_V2_PENDING_SCAN_ENABLED.key -> "true") {
      withDataSourceTable(sessions, "sessions_data") {
        val query = sql(
        """
          |WITH exploded_data AS (
          |  SELECT *,
          |    request.available AS request_available
          |  FROM sessions_data
          |  LATERAL VIEW OUTER posexplode(requests) t1 AS requestIdx, request
          |  LATERAL VIEW OUTER posexplode(request.servedItems) t2 AS servedItemIdx, servedItem
          |)
          |SELECT
          |  publisherId,
          |  max(endOfSession) AS endOfSession,
          |  sum(CASE WHEN request.available THEN 1 ELSE 0 END) AS available_requests,
          |  sum(CASE WHEN request.clientMode = 'mobile' THEN 1 ELSE 0 END) AS mobile_requests,
          |  sum(CASE WHEN coalesce(servedItem.clicked, false) THEN 1 ELSE 0 END) AS clicked_items,
          |  sum(servedItem.revenue) AS revenue_total
          |FROM exploded_data
          |GROUP BY publisherId
          |""".stripMargin)

        checkScan(query,
          "struct<publisherId:bigint,endOfSession:bigint," +
            "requests:array<struct<available:boolean,clientMode:string," +
            "servedItems:array<struct<clicked:boolean,revenue:double>>>>>")

        checkAnswer(query.orderBy("publisherId"),
          Row(100L, 1234567890L, 2L, 2L, 2L, 25.0) ::
            Row(200L, 1234567900L, 1L, 1L, 0L, null) :: Nil)
      }
    }
  }

  // TODO SPARK-47230: Re-enable when MapType pruning with chained generators is fixed
  // Currently disabled due to expression ID conflict when pruning map value types
  // testSchemaPruning("SPARK-47230: CTE with SELECT * over 2 chained generators with contacts")

  testSchemaPruning("select one deep nested complex field after repartition") {
    val query = sql("select * from contacts")
      .repartition(100)
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(query,
      "struct<employer:struct<id:int,company:struct<address:string>>>")
    checkAnswer(query, Row(0) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after repartition by expression") {
    val query1 = sql("select * from contacts")
      .repartition(100, col("id"))
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(query1,
      "struct<id:int,employer:struct<id:int,company:struct<address:string>>>")
    checkAnswer(query1, Row(0) :: Nil)

    val query2 = sql("select * from contacts")
      .repartition(100, col("employer"))
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(query2,
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query2, Row(0) :: Nil)

    val query3 = sql("select * from contacts")
      .repartition(100, col("employer.company"))
      .where("employer.company.address is not null")
      .selectExpr("employer.company as employer_company")
    checkScan(query3,
      "struct<employer:struct<company:struct<name:string,address:string>>>")
    checkAnswer(query3, Row(Row("abc", "123 Business Street")) :: Nil)

    val query4 = sql("select * from contacts")
      .repartition(100, col("employer.company.address"))
      .where("employer.company.address is not null")
      .selectExpr("employer.company.address as employer_company_addr")
    checkScan(query4,
      "struct<employer:struct<company:struct<address:string>>>")
    checkAnswer(query4, Row("123 Business Street") :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after join") {
    val query1 = sql("select contacts.name.middle from contacts, departments where " +
        "contacts.id = departments.contactId")
    checkScan(query1,
      "struct<id:int,name:struct<middle:string>>",
    "struct<contactId:int>")
    checkAnswer(query1, Row("X.") :: Row("Y.") :: Nil)

    val query2 = sql("select contacts.name.middle from contacts, departments where " +
      "contacts.employer = departments.employer")
    checkScan(query2,
      "struct<name:struct<middle:string>," +
        "employer:struct<id:int,company:struct<name:string,address:string>>>",
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query2, Row("X.") :: Row("Y.") :: Nil)

    val query3 = sql("select contacts.employer.company.name from contacts, departments where " +
      "contacts.employer = departments.employer")
    checkScan(query3,
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>",
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query3, Row("abc") :: Row(null) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after outer join") {
    val query1 = sql("select departments.contactId, contacts.name.middle from departments " +
      "left outer join contacts on departments.contactId = contacts.id")
    checkScan(query1,
      "struct<contactId:int>",
      "struct<id:int,name:struct<middle:string>>")
    checkAnswer(query1, Row(0, "X.") :: Row(1, "Y.") :: Row(4, null) :: Nil)

    val query2 = sql("select contacts.name.first, departments.employer.company.name " +
      "from contacts right outer join departments on contacts.id = departments.contactId")
    checkScan(query2,
      "struct<id:int,name:struct<first:string>>",
      "struct<contactId:int,employer:struct<company:struct<name:string>>>")
    checkAnswer(query2,
      Row("Jane", "abc") ::
        Row("John", null) ::
        Row(null, null) :: Nil)
  }

  testSchemaPruning("select nested field in aggregation function of Aggregate") {
    val query1 = sql("select count(name.first) from contacts group by name.last")
    checkScan(query1, "struct<name:struct<first:string,last:string>>")
    checkAnswer(query1, Row(2) :: Row(2) :: Nil)

    val query2 = sql("select count(name.first), sum(pets) from contacts group by id")
    checkScan(query2, "struct<id:int,name:struct<first:string>,pets:int>")
    checkAnswer(query2, Row(1, 1) :: Row(1, null) :: Row(1, 3) :: Row(1, null) :: Nil)

    val query3 = sql("select count(name.first), first(name) from contacts group by id")
    checkScan(query3, "struct<id:int,name:struct<first:string,middle:string,last:string>>")
    checkAnswer(query3,
      Row(1, Row("Jane", "X.", "Doe")) ::
        Row(1, Row("Jim", null, "Jones")) ::
        Row(1, Row("John", "Y.", "Doe")) ::
        Row(1, Row("Janet", null, "Jones")) :: Nil)

    val query4 = sql("select count(name.first), sum(pets) from contacts group by name.last")
    checkScan(query4, "struct<name:struct<first:string,last:string>,pets:int>")
    checkAnswer(query4, Row(2, null) :: Row(2, 4) :: Nil)
  }

  testSchemaPruning("select nested field in window function") {
    val windowSql =
      """
        |with contact_rank as (
        |  select row_number() over (partition by address order by id desc) as rank,
        |  contacts.*
        |  from contacts
        |)
        |select name.first, rank from contact_rank
        |where name.first = 'Jane' AND rank = 1
        |""".stripMargin
    val query = sql(windowSql)
    checkScan(query, "struct<id:int,name:struct<first:string>,address:string>")
    checkAnswer(query, Row("Jane", 1) :: Nil)
  }

  testSchemaPruning("select nested field in window function and then order by") {
    val windowSql =
      """
        |with contact_rank as (
        |  select row_number() over (partition by address order by id desc) as rank,
        |  contacts.*
        |  from contacts
        |  order by name.last, name.first
        |)
        |select name.first, rank from contact_rank
        |""".stripMargin
    val query = sql(windowSql)
    checkScan(query, "struct<id:int,name:struct<first:string,last:string>,address:string>")
    checkAnswer(query,
      Row("Jane", 1) ::
        Row("John", 1) ::
        Row("Janet", 1) ::
        Row("Jim", 1) :: Nil)
  }

  testSchemaPruning("select nested field in Sort") {
    val query1 = sql("select name.first, name.last from contacts order by name.first, name.last")
    checkScan(query1, "struct<name:struct<first:string,last:string>>")
    checkAnswer(query1,
      Row("Jane", "Doe") ::
        Row("Janet", "Jones") ::
        Row("Jim", "Jones") ::
        Row("John", "Doe") :: Nil)

    withTempView("tmp_contacts") {
      // Create a repartitioned view because `SORT BY` is a local sort
      sql("select * from contacts").repartition(1).createOrReplaceTempView("tmp_contacts")
      val sortBySql =
        """
          |select name.first, name.last from tmp_contacts
          |sort by name.first, name.last
          |""".stripMargin
      val query2 = sql(sortBySql)
      checkScan(query2, "struct<name:struct<first:string,last:string>>")
      checkAnswer(query2,
        Row("Jane", "Doe") ::
          Row("Janet", "Jones") ::
          Row("Jim", "Jones") ::
          Row("John", "Doe") :: Nil)
    }
  }

  testSchemaPruning("select nested field in Expand") {
    import org.apache.spark.sql.catalyst.dsl.expressions._

    val query1 = Expand(
      Seq(
        Seq($"name.first", $"name.last"),
        Seq(Concat(Seq($"name.first", $"name.last")),
          Concat(Seq($"name.last", $"name.first")))
      ),
      Seq($"a".string, $"b".string),
      sql("select * from contacts").logicalPlan
    ).toDF()
    checkScan(query1, "struct<name:struct<first:string,last:string>>")
    checkAnswer(query1,
      Row("Jane", "Doe") ::
        Row("JaneDoe", "DoeJane") ::
        Row("John", "Doe") ::
        Row("JohnDoe", "DoeJohn") ::
        Row("Jim", "Jones") ::
        Row("JimJones", "JonesJim") ::
        Row("Janet", "Jones") ::
        Row("JanetJones", "JonesJanet") :: Nil)

    val name = StructType.fromDDL("first string, middle string, last string")
    val query2 = Expand(
      Seq(Seq($"name", $"name.last")),
      Seq($"a".struct(name), $"b".string),
      sql("select * from contacts").logicalPlan
    ).toDF()
    checkScan(query2, "struct<name:struct<first:string,middle:string,last:string>>")
    checkAnswer(query2,
      Row(Row("Jane", "X.", "Doe"), "Doe") ::
        Row(Row("John", "Y.", "Doe"), "Doe") ::
        Row(Row("Jim", null, "Jones"), "Jones") ::
        Row(Row("Janet", null, "Jones"), "Jones") ::Nil)
  }

  testSchemaPruning("SPARK-32163: nested pruning should work even with cosmetic variations") {
    withTempView("contact_alias") {
      sql("select * from contacts")
        .repartition(100, col("name.first"), col("name.last"))
        .selectExpr("name").createOrReplaceTempView("contact_alias")

      val query1 = sql("select name.first from contact_alias")
      checkScan(query1, "struct<name:struct<first:string,last:string>>")
      checkAnswer(query1, Row("Jane") :: Row("John") :: Row("Jim") :: Row("Janet") ::Nil)

      sql("select * from contacts")
        .select(explode(col("friends.first")), col("friends"))
        .createOrReplaceTempView("contact_alias")

      val query2 = sql("select friends.middle, col from contact_alias")
      checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query2, Row(Array("Z."), "Susan") :: Nil)
    }
  }

  testSchemaPruning("SPARK-38918: nested schema pruning with correlated subqueries") {
    withContacts {
      withEmployees {
        val query = sql(
          """
            |select count(*)
            |from contacts c
            |where not exists (select null from employees e where e.name.first = c.name.first
            |  and e.employer.name = c.employer.company.name)
            |""".stripMargin)
        checkScan(query,
          "struct<name:struct<first:string,middle:string,last:string>," +
            "employer:struct<id:int,company:struct<name:string,address:string>>>",
          "struct<name:struct<first:string,middle:string,last:string>," +
            "employer:struct<name:string,address:string>>")
        checkAnswer(query, Row(3))
      }
    }
  }

  protected def testSchemaPruning(testName: String)(testThunk: => Unit): Unit = {
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

  private def withContacts(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contacts, new File(path + "/contacts/p=1"))
      makeDataSourceFile(briefContacts, new File(path + "/contacts/p=2"))
      makeDataSourceFile(departments, new File(path + "/departments"))

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

      val departmentSchema = "`depId` INT,`depName` STRING,`contactId` INT, " +
        "`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>"
      spark.read.format(dataSourceName).schema(departmentSchema).load(path + "/departments")
        .createOrReplaceTempView("departments")

      testThunk
    }
  }

  private def withContactsWithDataPartitionColumn(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(contactsWithDataPartitionColumn, new File(path + "/contacts/p=1"))
      makeDataSourceFile(briefContactsWithDataPartitionColumn, new File(path + "/contacts/p=2"))
      makeDataSourceFile(departments, new File(path + "/departments"))

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

      val departmentSchema = "`depId` INT,`depName` STRING,`contactId` INT, " +
        "`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>"
      spark.read.format(dataSourceName).schema(departmentSchema).load(path + "/departments")
        .createOrReplaceTempView("departments")

      testThunk
    }
  }

  private def withEmployees(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(employees, new File(path + "/employees"))

      // Providing user specified schema. Inferred schema from different data sources might
      // be different.
      val schema = "`id` INT,`name` STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>, " +
        "`employer` STRUCT<`name`: STRING, `address`: STRING>"
      spark.read.format(dataSourceName).schema(schema).load(path + "/employees")
        .createOrReplaceTempView("employees")

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

  testMixedCaseQueryPruning("subquery filter with different-case column names") {
    withTempView("temp") {
      val spark = this.spark
      import spark.implicits._

      val df = Seq(2).toDF("col2")
      df.createOrReplaceTempView("temp")

      val query = sql("select id from mixedcase where Col2.b IN (select col2 from temp)")
      checkScan(query, "struct<id:int,coL2:struct<B:int>>")
      checkAnswer(query.orderBy("id"), Row(1) :: Nil)
    }
  }

  // Tests schema pruning for a query whose column and field names are exactly the same as the table
  // schema's column and field names. N.B. this implies that `testThunk` should pass using either a
  // case-sensitive or case-insensitive query parser
  private def testExactCaseQueryPruning(testName: String)(testThunk: => Unit): Unit = {
    test(s"Case-sensitive parser - mixed-case schema - $testName") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        withMixedCaseData(testThunk)
      }
    }
    testMixedCaseQueryPruning(testName)(testThunk)
  }

  // Tests schema pruning for a query whose column and field names may differ in case from the table
  // schema's column and field names
  private def testMixedCaseQueryPruning(testName: String)(testThunk: => Unit): Unit = {
    test(s"Case-insensitive parser - mixed-case schema - $testName") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        withMixedCaseData(testThunk)
      }
    }
  }

  // Tests given test function with Spark vectorized reader and non-vectorized reader.
  private def withMixedCaseData(testThunk: => Unit): Unit = {
    withDataSourceTable(mixedCaseData, "mixedcase") {
      testThunk
    }
  }

  protected val schemaEquality = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => DataTypeUtils.sameType(a, otherType)
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
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.requiredSchema
      }
    val v2ScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: DataSourceV2ScanExecBase =>
          scan.scan match {
            case fileScan: FileScan => fileScan.readDataSchema
            case other => other.readSchema()
          }
      }
    val collectedSchemas = fileSourceScanSchemata ++ v2ScanSchemata
    assert(collectedSchemas.size === expectedSchemaCatalogStrings.size,
      s"Found ${collectedSchemas.size} file/V2 sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    collectedSchemas.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }

  testSchemaPruning("SPARK-34963: extract case-insensitive struct field from array") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val query1 = spark.table("contacts")
        .select("friends.First", "friends.MiDDle")
      checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query1,
        Row(Array.empty[String], Array.empty[String]) ::
          Row(Array("Susan"), Array("Z.")) ::
          Row(null, null) ::
          Row(null, null) :: Nil)

      val query2 = spark.table("contacts")
        .where("friends.First is not null")
        .select("friends.First", "friends.MiDDle")
      checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(query2,
        Row(Array.empty[String], Array.empty[String]) ::
          Row(Array("Susan"), Array("Z.")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-34963: extract case-insensitive struct field from struct") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val query1 = spark.table("contacts")
        .select("Name.First", "NAME.MiDDle")
      checkScan(query1, "struct<name:struct<first:string,middle:string>>")
      checkAnswer(query1,
        Row("Jane", "X.") ::
          Row("Janet", null) ::
          Row("Jim", null) ::
          Row("John", "Y.") :: Nil)

      val query2 = spark.table("contacts")
        .where("Name.MIDDLE is not null")
        .select("Name.First", "NAME.MiDDle")
      checkScan(query2, "struct<name:struct<first:string,middle:string>>")
      checkAnswer(query2,
        Row("Jane", "X.") ::
          Row("John", "Y.") :: Nil)
    }
  }

  test("SPARK-34638: queries should not fail on unsupported cases") {
    withTable("nested_array") {
      sql("select * from values array(array(named_struct('a', 1, 'b', 3), " +
        "named_struct('a', 2, 'b', 4))) T(items)").write.saveAsTable("nested_array")
      val query = sql("select d.a from (select explode(c) d from " +
        "(select explode(items) c from nested_array))")
      checkAnswer(query, Row(1) :: Row(2) :: Nil)
    }

    withTable("map") {
      sql("select * from values map(1, named_struct('a', 1, 'b', 3), " +
        "2, named_struct('a', 2, 'b', 4)) T(items)").write.saveAsTable("map")
      val query = sql("select d.a from (select explode(items) (c, d) from map)")
      checkAnswer(query, Row(1) :: Row(2) :: Nil)
    }
  }

  test("SPARK-36352: Spark should check result plan's output schema name") {
    withMixedCaseData {
      val query = sql("select cOL1, cOl2.B from mixedcase")
      assert(query.queryExecution.executedPlan.schema.catalogString ==
        "struct<cOL1:string,B:int>")
      checkAnswer(query.orderBy("id"),
        Row("r0c1", 1) ::
          Row("r1c1", 2) ::
          Nil)
    }
  }

  test("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation") {
    import testImplicits._

    withTempView("table") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        val jsonStr =
          """
            |{
            |  "items": [
            |  {"itemId": 1, "itemData": "a"},
            |  {"itemId": 2, "itemData": "b"}
            |]}
            |""".stripMargin
        val df = spark.read.json(Seq(jsonStr).toDS)
        makeDataSourceFile(df, new File(path))

        spark.read.format(dataSourceName).load(path)
          .createOrReplaceTempView("table")

        val read = spark.table("table")
        val query = read.select(explode($"items").as(Symbol("item"))).select(count($"*"))

        checkScan(query, "struct<items:array<struct<itemId:long>>>")
        checkAnswer(query, Row(2) :: Nil)
      }
    }
  }

  test("SPARK-37577: Fix ClassCastException: ArrayType cannot be cast to StructType") {
    import testImplicits._

    val schema = StructType(Seq(
      StructField("array", ArrayType(StructType(
        Seq(StructField("string", StringType, false),
          StructField("inner_array", ArrayType(StructType(
            Seq(StructField("inner_string", StringType, false))), true), false)
        )), false))
    ))

    val count = spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
      .select(explode($"array").alias("element"))
      .select("element.*")
      .select(explode($"inner_array"))
      .count()
    assert(count == 0)
  }

  testSchemaPruning("SPARK-38977: schema pruning with correlated EXISTS subquery") {

    import testImplicits._

    withTempView("ids", "first_names") {
      val df1 = Seq(1, 2, 3).toDF("value")
      df1.createOrReplaceTempView("ids")

      val df2 = Seq("John", "Bob").toDF("value")
      df2.createOrReplaceTempView("first_names")

      val query = sql(
        """SELECT name FROM contacts c
          |WHERE
          |  EXISTS (SELECT 1 FROM ids i WHERE i.value = c.id)
          |  AND
          |  EXISTS (SELECT 1 FROM first_names n WHERE c.name.first = n.value)
          |""".stripMargin)

      checkScan(query, "struct<id:int,name:struct<first:string,middle:string,last:string>>")

      checkAnswer(query, Row(Row("John", "Y.", "Doe")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-38977: schema pruning with correlated NOT EXISTS subquery") {

    import testImplicits._

    withTempView("ids", "first_names") {
      val df1 = Seq(1, 2, 3).toDF("value")
      df1.createOrReplaceTempView("ids")

      val df2 = Seq("John", "Bob").toDF("value")
      df2.createOrReplaceTempView("first_names")

      val query = sql(
        """SELECT name FROM contacts c
          |WHERE
          |  NOT EXISTS (SELECT 1 FROM ids i WHERE i.value = c.id)
          |  AND
          |  NOT EXISTS (SELECT 1 FROM first_names n WHERE c.name.first = n.value)
          |""".stripMargin)

      checkScan(query, "struct<id:int,name:struct<first:string,middle:string,last:string>>")

      checkAnswer(query, Row(Row("Jane", "X.", "Doe")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-38977: schema pruning with correlated IN subquery") {

    import testImplicits._

    withTempView("ids", "first_names") {
      val df1 = Seq(1, 2, 3).toDF("value")
      df1.createOrReplaceTempView("ids")

      val df2 = Seq("John", "Bob").toDF("value")
      df2.createOrReplaceTempView("first_names")

      val query = sql(
        """SELECT name FROM contacts c
          |WHERE
          |  id IN (SELECT * FROM ids i WHERE c.pets > i.value)
          |  AND
          |  name.first IN (SELECT * FROM first_names n WHERE c.name.last < n.value)
          |""".stripMargin)

      checkScan(query,
        "struct<id:int,name:struct<first:string,middle:string,last:string>,pets:int>")

      checkAnswer(query, Row(Row("John", "Y.", "Doe")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-38977: schema pruning with correlated NOT IN subquery") {

    import testImplicits._

    withTempView("ids", "first_names") {
      val df1 = Seq(1, 2, 3).toDF("value")
      df1.createOrReplaceTempView("ids")

      val df2 = Seq("John", "Janet", "Jim", "Bob").toDF("value")
      df2.createOrReplaceTempView("first_names")

      val query = sql(
        """SELECT name FROM contacts c
          |WHERE
          |  id NOT IN (SELECT * FROM ids i WHERE c.pets > i.value)
          |  AND
          |  name.first NOT IN (SELECT * FROM first_names n WHERE c.name.last > n.value)
          |""".stripMargin)

      checkScan(query,
        "struct<id:int,name:struct<first:string,middle:string,last:string>,pets:int>")

      checkAnswer(query, Row(Row("Jane", "X.", "Doe")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-40033: Schema pruning support through element_at") {
    // nested struct fields inside array
    val query1 =
      sql("""
            |SELECT
            |element_at(friends, 1).first, element_at(friends, 1).last
            |FROM contacts WHERE id = 0
            |""".stripMargin)
    checkScan(query1, "struct<id:int,friends:array<struct<first:string,last:string>>>")
    checkAnswer(query1.orderBy("id"),
      Row("Susan", "Smith"))

    // nested struct fields inside map values
    val query2 =
      sql("""
            |SELECT
            |element_at(relatives, "brother").first, element_at(relatives, "brother").middle
            |FROM contacts WHERE id = 0
            |""".stripMargin)
    checkScan(query2, "struct<id:int,relatives:map<string,struct<first:string,middle:string>>>")
    checkAnswer(query2.orderBy("id"),
      Row("John", "Y."))
  }

  testSchemaPruning("SPARK-41017: column pruning through 2 filters") {
    import testImplicits._
    val query = spark.table("contacts").filter(rand() > 0.5).filter(rand() < 0.8)
      .select($"id", $"name.first")
    checkScan(query, "struct<id:int, name:struct<first:string>>")
  }

  testSchemaPruning("SPARK-42163: GetArrayItem and GetMapItem with non-foldable index") {
    // Technically, there's no reason that we can't support a non-foldable index, it's just tricky
    // with the existing pruning code. If we ever do support it, this test can be modified to check
    // for a narrower scan schema.
    val arrayQuery =
      sql("""
            |SELECT
            |employer.company, friends[employer.id].first
            |FROM contacts
            |""".stripMargin)
    checkScan(arrayQuery,
        """struct<friends:array<struct<first:string,middle:string,last:string>>,
          |employer:struct<id:int,company:struct<name:string,address:string>>>""".stripMargin)
    checkAnswer(arrayQuery,
      Row(Row("abc", "123 Business Street"), "Susan") ::
      Row(null, null) :: Row(null, null) :: Row(null, null) :: Nil)

    val mapQuery =
      sql("""
            |SELECT
            |employer.id, relatives[employer.company.name].first
            |FROM contacts
            |""".stripMargin)
    checkScan(mapQuery,
        """struct<relatives:map<string,struct<first:string,middle:string,last:string>>,
          |employer:struct<id:int,company:struct<name:string>>>""".stripMargin)
    checkAnswer(mapQuery, Row(0, null) :: Row(1, null) ::
      Row(null, null) :: Row(null, null) :: Nil)
  }
}
