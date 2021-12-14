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
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
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

  val janeDoe = FullName("Jane", "X.", "Doe")
  val johnDoe = FullName("John", "Y.", "Doe")
  val susanSmith = FullName("Susan", "Z.", "Smith")

  val employer = Employer(0, Company("abc", "123 Business Street"))
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

    // Currently we don't prune multiple field case.
    val query2 = spark.table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first", "friend.middle")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
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
      Seq('a.string, 'b.string),
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
      Seq('a.struct(name), 'b.string),
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
      collect(df.queryExecution.executedPlan) {
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
        val query = read.select(explode($"items").as('item)).select(count($"*"))

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
}
