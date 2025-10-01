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
import org.apache.spark.sql.expressions.Window
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
  case class Department(depId: Int, depName: String, contactId: Int, employer: Employer)

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
    Contact(
      0,
      janeDoe,
      "123 Main Street",
      1,
      friends = Array(susanSmith),
      relatives = Map("brother" -> johnDoe),
      employer = employer,
      relations = Map(johnDoe -> "brother")) ::
      Contact(
        1,
        johnDoe,
        "321 Wall Street",
        3,
        relatives = Map("sister" -> janeDoe),
        employer = employerWithNullCompany,
        relations = Map(janeDoe -> "sister")) :: Nil

  val departments =
    Department(0, "Engineering", 0, employer) ::
      Department(1, "Marketing", 1, employerWithNullCompany) ::
      Department(2, "Operation", 4, employerWithNullCompany2) :: Nil

  val employees = Employee(0, janeDoe, company) :: Employee(1, johnDoe, company) :: Nil

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

  // Case classes for nested column aliasing tests
  case class Item(itemId: Int, itemName: String, itemPrice: Double, itemCategory: String)
  case class SubItem(itemId: Int, itemName: String, itemStatus: String, itemValue: Double)
  case class SubContainer(containerId: Int, items: Array[SubItem], containerType: String)
  case class Request(requestId: Int, timestamp: String, userAgent: String, items: Array[Item])
  case class PageView(pageId: Int, userId: String, requests: Array[Request])
  case class NestedItem(id: Int, name: String, price: Double, category: String, active: Boolean)
  case class Container(id: Int, items: Array[NestedItem])
  case class Session(sessionId: Int, startTime: String, events: Array[String])
  case class User(userId: Int, userName: String, sessions: Array[Session])
  case class JsonNestedItem(id: Int, name: String, price: Double, tags: Array[String])
  case class JsonContainer(containerId: Int, items: Array[JsonNestedItem], metadata: String)
  case class ParquetOrder(orderId: Int, orderDate: String, total: Double)
  case class ParquetCustomer(customerId: Int, customerName: String, orders: Array[ParquetOrder])
  case class FilterableItem(
      id: Int,
      name: String,
      category: String,
      price: Double,
      active: Boolean)
  case class FilterableContainer(
      containerId: Int,
      containerType: String,
      items: Array[FilterableItem])
  case class SalesRecord(salesId: Int, amount: Double, region: String, quarter: Int)
  case class SalesData(year: Int, records: Array[SalesRecord], totalRevenue: Double)

  case class DocumentContent(title: String, body: String, tags: Array[String])
  case class Document(docId: Int, content: DocumentContent, metadata: Map[String, String])
  case class DocumentWithTags(
      docId: Int,
      title: String,
      tags: Array[Tag],
      categories: Array[String])

  case class LeftRecord(id: Int, leftData: String, leftValue: Double)
  case class RightRecord(id: Int, rightData: String, rightCode: String)
  case class LeftContainer(leftItems: Array[LeftRecord])
  case class RightContainer(rightItems: Array[RightRecord])

  case class Address(street: String, city: String, zipCode: String, country: String)
  case class PersonalInfo(firstName: String, lastName: String, address: Address, age: Int)
  case class Employee2(empId: Int, personalInfo: PersonalInfo, department: String, salary: Double)
  case class Company2(companyId: Int, employees: Array[Employee2], companyName: String)

  case class CteRecord(recordId: Int, recordName: String, recordType: String, recordData: Double)
  case class CteData(dataId: Int, records: Array[CteRecord], metadata: String)
  case class Skill(skillId: Int, skillName: String, level: String)
  case class Project(projectId: Int, projectName: String, skills: Array[Skill])
  case class TestDepartment(deptId: Int, deptName: String, projects: Array[Project])
  case class Organization(orgId: Int, orgName: String, departments: Array[TestDepartment])

  case class AddressTag(street: String, city: String, zipCode: String, country: String)
  case class PersonalInfoTag(firstName: String, lastName: String, address: AddressTag, age: Int)
  case class EmployeeTag(
      empId: Int,
      personalInfo: PersonalInfoTag,
      department: String,
      salary: Double)
  case class CompanyTag(companyId: Int, employees: Array[EmployeeTag], companyName: String)

  case class SkillTag(skillId: Int, skillName: String, level: String, certified: Boolean)
  case class ProjectTag(
      projectId: Int,
      projectName: String,
      skills: Array[SkillTag],
      budget: Double)
  case class DepartmentTag(
      deptId: Int,
      deptName: String,
      projects: Array[ProjectTag],
      manager: String)
  case class OrganizationTag(orgId: Int, departments: Array[DepartmentTag], orgType: String)

  case class Customer(customerId: Int, customerName: String, address: String, phoneNumber: String)
  case class CustomerData(customers: Array[Customer], lastUpdated: String)

  case class Level3(field: String)
  case class Level2(field: String, level3: Array[Level3])
  case class Level1(field: String, level2: Array[Level2])
  case class RootLevel(field: String, level1: Array[Level1])

  case class Level4Tag(field4: String, data4: Int, unused4: String)
  case class Level3Tag(field3: String, level4: Level4Tag, unused3: Double)
  case class Level2Tag(field2: String, level3: Level3Tag, unused2: Boolean)
  case class Level1Tag(field1: String, level2: Level2Tag, unused1: Array[String])
  case class RootLevelTag(items: Array[Level1Tag], rootField: String)

  // Aggregation test classes (use unique names to avoid TypeTag issues with local classes)
  case class AggEvent(eventId: Int, eventName: String, eventType: String)
  case class AggSession(sessionId: Int, duration: Int, events: Array[AggEvent])
  case class AggUser(userId: Int, userName: String, sessions: Array[AggSession])

  case class ParquetProduct(productId: Int, productName: String, productPrice: Double)
  case class ParquetOrderAdvanced(
      orderId: Int,
      orderDate: String,
      products: Array[ParquetProduct])
  case class ParquetCustomerAdvanced(
      customerId: Int,
      customerName: String,
      orders: Array[ParquetOrderAdvanced])

  case class MixedCaseColumn(a: String, B: Int)
  case class MixedCase(id: Int, CoL1: String, coL2: MixedCaseColumn)
  case class FilterableItemTag(
      id: Int,
      name: String,
      category: String,
      price: Double,
      active: Boolean)
  case class FilterableContainerv(
      containerId: Int,
      containerType: String,
      items: Array[FilterableItemTag])

  case class ContactInfo(phone: String, email: String, preferred: String)
  case class CustomerProfileWithContactInfo(
      profileId: Int,
      contacts: Array[ContactInfo],
      preferences: Map[String, String])
  case class CustomerWithOrders(
      customerId: Int,
      profile: CustomerProfileWithContactInfo,
      orders: Array[Int],
      active: Boolean)
  case class CustomerDataExtended(customers: Array[CustomerWithOrders], lastUpdated: String)

  case class Tag(tagId: Int, tagName: String, tagType: String)

  val contactsWithDataPartitionColumn =
    contacts.map {
      case Contact(id, name, address, pets, friends, relatives, employer, relations) =>
        ContactWithDataPartitionColumn(
          id,
          name,
          address,
          pets,
          friends,
          relatives,
          employer,
          relations,
          1)
    }
  val briefContactsWithDataPartitionColumn =
    briefContacts.map { case BriefContact(id, name, address) =>
      BriefContactWithDataPartitionColumn(id, name, address, 2)
    }

  testSchemaPruning("select only top-level fields") {
    val query = sql("select address from contacts")
    checkScan(query, "struct<address:string>")
    checkAnswer(
      query.orderBy("id"),
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
    checkAnswer(
      query.orderBy("id"),
      Row("X.", Row("Jane", "X.", "Doe")) ::
        Row("Y.", Row("John", "Y.", "Doe")) ::
        Row(null, Row("Janet", null, "Jones")) ::
        Row(null, Row("Jim", null, "Jones")) ::
        Nil)
  }

  testSchemaPruning("select a single complex field array and its parent struct array") {
    val query = sql("select friends.middle, friends from contacts where p=1")
    checkScan(query, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(
      query.orderBy("id"),
      Row(Array("Z."), Array(Row("Susan", "Z.", "Smith"))) ::
        Row(Array.empty[String], Array.empty[Row]) ::
        Nil)
  }

  testSchemaPruning("select a single complex field from a map entry and its parent map entry") {
    val query =
      sql("select relatives[\"brother\"].middle, relatives[\"brother\"] from contacts where p=1")
    checkScan(
      query,
      "struct<relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(
      query.orderBy("id"),
      Row("Y.", Row("John", "Y.", "Doe")) ::
        Row(null, null) ::
        Nil)
  }

  testSchemaPruning("select a single complex field and the partition column") {
    val query = sql("select name.middle, p from contacts")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(
      query.orderBy("id"),
      Row("X.", 1) :: Row("Y.", 1) :: Row(null, 2) :: Row(null, 2) :: Nil)
  }

  testSchemaPruning("partial schema intersection - select missing subfield") {
    val query = sql("select name.middle, address from contacts where p=2")
    checkScan(query, "struct<name:struct<middle:string>,address:string>")
    checkAnswer(
      query.orderBy("id"),
      Row(null, "567 Maple Drive") ::
        Row(null, "6242 Ash Street") :: Nil)
  }

  testSchemaPruning("no unnecessary schema pruning") {
    val query =
      sql(
        "select id, name.last, name.middle, name.first, relatives[''].last, " +
          "relatives[''].middle, relatives[''].first, friends[0].last, friends[0].middle, " +
          "friends[0].first, pets, address from contacts where p=2")
    // We've selected every field in the schema. Therefore, no schema pruning should be performed.
    // We check this by asserting that the scanned schema of the query is identical to the schema
    // of the contacts relation, even though the fields are selected in different orders.
    checkScan(
      query,
      "struct<id:int,name:struct<first:string,middle:string,last:string>,address:string,pets:int," +
        "friends:array<struct<first:string,middle:string,last:string>>," +
        "relatives:map<string,struct<first:string,middle:string,last:string>>>")
    checkAnswer(
      query.orderBy("id"),
      Row(
        2,
        "Jones",
        null,
        "Janet",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "567 Maple Drive") ::
        Row(
          3,
          "Jones",
          null,
          "Jim",
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          "6242 Ash Street") ::
        Nil)
  }

  testSchemaPruning("empty schema intersection") {
    val query = sql("select name.middle from contacts where p=2")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"), Row(null) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and in where clause") {
    val query1 = sql("select name.first from contacts where name.first = 'Jane'")
    checkScan(query1, "struct<name:struct<first:string>>")
    checkAnswer(query1, Row("Jane") :: Nil)

    val query2 = sql("select name.first, name.last from contacts where name.first = 'Jane'")
    checkScan(query2, "struct<name:struct<first:string,last:string>>")
    checkAnswer(query2, Row("Jane", "Doe") :: Nil)

    val query3 = sql(
      "select name.first from contacts " +
        "where employer.company.name = 'abc' and p = 1")
    checkScan(
      query3,
      "struct<name:struct<first:string>," +
        "employer:struct<company:struct<name:string>>>")
    checkAnswer(query3, Row("Jane") :: Nil)

    val query4 = sql(
      "select name.first, employer.company.name from contacts " +
        "where employer.company is not null and p = 1")
    checkScan(
      query4,
      "struct<name:struct<first:string>," +
        "employer:struct<company:struct<name:string>>>")
    checkAnswer(query4, Row("Jane", "abc") :: Nil)
  }

  testSchemaPruning("select nullable complex field and having is not null predicate") {
    val query = sql(
      "select employer.company from contacts " +
        "where employer is not null and p = 1")
    checkScan(query, "struct<employer:struct<company:struct<name:string,address:string>>>")
    checkAnswer(query, Row(Row("abc", "123 Business Street")) :: Row(null) :: Nil)
  }

  testSchemaPruning("select a single complex field and is null expression in project") {
    val query = sql("select name.first, address is not null from contacts")
    checkScan(query, "struct<name:struct<first:string>,address:string>")
    checkAnswer(
      query.orderBy("id"),
      Row("Jane", true) :: Row("John", true) :: Row("Janet", true) :: Row("Jim", true) :: Nil)
  }

  testSchemaPruning("select a single complex field array and in clause") {
    val query = sql("select friends.middle from contacts where friends.first[0] = 'Susan'")
    checkScan(query, "struct<friends:array<struct<first:string,middle:string>>>")
    checkAnswer(query.orderBy("id"), Row(Array("Z.")) :: Nil)
  }

  testSchemaPruning("select a single complex field from a map entry and in clause") {
    val query =
      sql(
        "select relatives[\"brother\"].middle from contacts " +
          "where relatives[\"brother\"].first = 'John'")
    checkScan(query, "struct<relatives:map<string,struct<first:string,middle:string>>>")
    checkAnswer(query.orderBy("id"), Row("Y.") :: Nil)
  }

  testSchemaPruning(
    "select one complex field and having is null predicate on another " +
      "complex field") {
    val query = sql("select * from contacts")
      .where("name.middle is not null")
      .select("id", "name.first", "name.middle", "name.last")
      .where("last = 'Jones'")
      .select(count("id"))
      .toDF()
    checkScan(query, "struct<id:int,name:struct<middle:string,last:string>>")
    checkAnswer(query, Row(0) :: Nil)
  }

  testSchemaPruning(
    "select one deep nested complex field and having is null predicate on " +
      "another deep nested complex field") {
    val query = sql("select * from contacts")
      .where("employer.company.address is not null")
      .selectExpr("id", "name.first", "name.middle", "name.last", "employer.id as employer_id")
      .where("employer_id = 0")
      .select(count("id"))
      .toDF()
    checkScan(query, "struct<id:int,employer:struct<id:int,company:struct<address:string>>>")
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
        val query1 = spark
          .table("contacts")
          .select(explode(col("friends.first")))
        if (nestedPruning) {
          // If `NESTED_SCHEMA_PRUNING_ENABLED` is enabled,
          // even disabling `NESTED_PRUNING_ON_EXPRESSIONS`,
          // nested schema is still pruned at scan node.
          checkScan(query1, "struct<friends:array<struct<first:string>>>")
        } else {
          checkScan(
            query1,
            "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        }
        checkAnswer(query1, Row("Susan") :: Nil)

        val query2 = spark
          .table("contacts")
          .select(explode(col("friends.first")), col("friends.middle"))
        if (nestedPruning) {
          checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
        } else {
          checkScan(
            query2,
            "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        }
        checkAnswer(query2, Row("Susan", Array("Z.")) :: Nil)

        val query3 = spark
          .table("contacts")
          .select(explode(col("friends.first")), col("friends.middle"), col("friends.last"))
        checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
        checkAnswer(query3, Row("Susan", Array("Z."), Array("Smith")) :: Nil)
      }
    }
  }

  testSchemaPruning("SPARK-34638: nested column prune on generator output") {
    val query1 = spark
      .table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first")
    checkScan(query1, "struct<friends:array<struct<first:string>>>")
    checkAnswer(query1, Row("Susan") :: Nil)

    // Currently we don't prune multiple field case.
    val query2 = spark
      .table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first", "friend.middle")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    val query3 = spark
      .table("contacts")
      .select(explode(col("friends")).as("friend"))
      .select("friend.first", "friend.middle", "friend")
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row("Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)
  }

  testSchemaPruning("SPARK-34638: nested column prune on generator output - case-sensitivity") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val query1 = spark
        .table("contacts")
        .select(explode(col("friends")).as("friend"))
        .select("friend.First")
      checkScan(query1, "struct<friends:array<struct<first:string>>>")
      checkAnswer(query1, Row("Susan") :: Nil)

      val query2 = spark
        .table("contacts")
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

    // Currently we don't prune multiple field case.
    val query2 =
      sql("select friend.first, friend.middle from contacts, lateral explode(friends) t(friend)")
    checkScan(query2, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query2, Row("Susan", "Z.") :: Nil)

    val query3 = sql("""
        |select friend.first, friend.middle, friend
        |from contacts, lateral explode(friends) t(friend)
        |""".stripMargin)
    checkScan(query3, "struct<friends:array<struct<first:string,middle:string,last:string>>>")
    checkAnswer(query3, Row("Susan", "Z.", Row("Susan", "Z.", "Smith")) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after repartition") {
    val query = sql("select * from contacts")
      .repartition(100)
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(query, "struct<employer:struct<id:int,company:struct<address:string>>>")
    checkAnswer(query, Row(0) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after repartition by expression") {
    val query1 = sql("select * from contacts")
      .repartition(100, col("id"))
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(query1, "struct<id:int,employer:struct<id:int,company:struct<address:string>>>")
    checkAnswer(query1, Row(0) :: Nil)

    val query2 = sql("select * from contacts")
      .repartition(100, col("employer"))
      .where("employer.company.address is not null")
      .selectExpr("employer.id as employer_id")
    checkScan(
      query2,
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query2, Row(0) :: Nil)

    val query3 = sql("select * from contacts")
      .repartition(100, col("employer.company"))
      .where("employer.company.address is not null")
      .selectExpr("employer.company as employer_company")
    checkScan(query3, "struct<employer:struct<company:struct<name:string,address:string>>>")
    checkAnswer(query3, Row(Row("abc", "123 Business Street")) :: Nil)

    val query4 = sql("select * from contacts")
      .repartition(100, col("employer.company.address"))
      .where("employer.company.address is not null")
      .selectExpr("employer.company.address as employer_company_addr")
    checkScan(query4, "struct<employer:struct<company:struct<address:string>>>")
    checkAnswer(query4, Row("123 Business Street") :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after join") {
    val query1 = sql(
      "select contacts.name.middle from contacts, departments where " +
        "contacts.id = departments.contactId")
    checkScan(query1, "struct<id:int,name:struct<middle:string>>", "struct<contactId:int>")
    checkAnswer(query1, Row("X.") :: Row("Y.") :: Nil)

    val query2 = sql(
      "select contacts.name.middle from contacts, departments where " +
        "contacts.employer = departments.employer")
    checkScan(
      query2,
      "struct<name:struct<middle:string>," +
        "employer:struct<id:int,company:struct<name:string,address:string>>>",
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query2, Row("X.") :: Row("Y.") :: Nil)

    val query3 = sql(
      "select contacts.employer.company.name from contacts, departments where " +
        "contacts.employer = departments.employer")
    checkScan(
      query3,
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>",
      "struct<employer:struct<id:int,company:struct<name:string,address:string>>>")
    checkAnswer(query3, Row("abc") :: Row(null) :: Nil)
  }

  testSchemaPruning("select one deep nested complex field after outer join") {
    val query1 = sql(
      "select departments.contactId, contacts.name.middle from departments " +
        "left outer join contacts on departments.contactId = contacts.id")
    checkScan(query1, "struct<contactId:int>", "struct<id:int,name:struct<middle:string>>")
    checkAnswer(query1, Row(0, "X.") :: Row(1, "Y.") :: Row(4, null) :: Nil)

    val query2 = sql(
      "select contacts.name.first, departments.employer.company.name " +
        "from contacts right outer join departments on contacts.id = departments.contactId")
    checkScan(
      query2,
      "struct<id:int,name:struct<first:string>>",
      "struct<contactId:int,employer:struct<company:struct<name:string>>>")
    checkAnswer(
      query2,
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
    checkAnswer(
      query3,
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
    checkAnswer(
      query,
      Row("Jane", 1) ::
        Row("John", 1) ::
        Row("Janet", 1) ::
        Row("Jim", 1) :: Nil)
  }

  testSchemaPruning("select nested field in Sort") {
    val query1 = sql("select name.first, name.last from contacts order by name.first, name.last")
    checkScan(query1, "struct<name:struct<first:string,last:string>>")
    checkAnswer(
      query1,
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
      checkAnswer(
        query2,
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
        Seq(Concat(Seq($"name.first", $"name.last")), Concat(Seq($"name.last", $"name.first")))),
      Seq($"a".string, $"b".string),
      sql("select * from contacts").logicalPlan).toDF()
    checkScan(query1, "struct<name:struct<first:string,last:string>>")
    checkAnswer(
      query1,
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
      sql("select * from contacts").logicalPlan).toDF()
    checkScan(query2, "struct<name:struct<first:string,middle:string,last:string>>")
    checkAnswer(
      query2,
      Row(Row("Jane", "X.", "Doe"), "Doe") ::
        Row(Row("John", "Y.", "Doe"), "Doe") ::
        Row(Row("Jim", null, "Jones"), "Jones") ::
        Row(Row("Janet", null, "Jones"), "Jones") :: Nil)
  }

  testSchemaPruning("SPARK-32163: nested pruning should work even with cosmetic variations") {
    withTempView("contact_alias") {
      sql("select * from contacts")
        .repartition(100, col("name.first"), col("name.last"))
        .selectExpr("name")
        .createOrReplaceTempView("contact_alias")

      val query1 = sql("select name.first from contact_alias")
      checkScan(query1, "struct<name:struct<first:string,last:string>>")
      checkAnswer(query1, Row("Jane") :: Row("John") :: Row("Jim") :: Row("Janet") :: Nil)

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
        val query = sql("""
            |select count(*)
            |from contacts c
            |where not exists (select null from employees e where e.name.first = c.name.first
            |  and e.employer.name = c.employer.company.name)
            |""".stripMargin)
        checkScan(
          query,
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
      withSQLConf(vectorizedReaderEnabledKey -> "false") {
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
      spark.read
        .format(dataSourceName)
        .schema(schema)
        .load(path + "/contacts")
        .createOrReplaceTempView("contacts")

      val departmentSchema = "`depId` INT,`depName` STRING,`contactId` INT, " +
        "`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>"
      spark.read
        .format(dataSourceName)
        .schema(departmentSchema)
        .load(path + "/departments")
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
      spark.read
        .format(dataSourceName)
        .schema(schema)
        .load(path + "/contacts")
        .createOrReplaceTempView("contacts")

      val departmentSchema = "`depId` INT,`depName` STRING,`contactId` INT, " +
        "`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>"
      spark.read
        .format(dataSourceName)
        .schema(departmentSchema)
        .load(path + "/departments")
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
      spark.read
        .format(dataSourceName)
        .schema(schema)
        .load(path + "/employees")
        .createOrReplaceTempView("employees")

      testThunk
    }
  }

  private val mixedCaseData =
    MixedCase(0, "r0c1", MixedCaseColumn("abc", 1)) ::
      MixedCase(1, "r1c1", MixedCaseColumn("123", 2)) ::
      Nil

  testExactCaseQueryPruning("select with exact column names") {
    val query = sql("select CoL1, coL2.B from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(
      query.orderBy("id"),
      Row("r0c1", 1) ::
        Row("r1c1", 2) ::
        Nil)
  }

  testMixedCaseQueryPruning("select with lowercase column names") {
    val query = sql("select col1, col2.b from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(
      query.orderBy("id"),
      Row("r0c1", 1) ::
        Row("r1c1", 2) ::
        Nil)
  }

  testMixedCaseQueryPruning("select with different-case column names") {
    val query = sql("select cOL1, cOl2.b from mixedcase")
    checkScan(query, "struct<CoL1:string,coL2:struct<B:int>>")
    checkAnswer(
      query.orderBy("id"),
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
      collect(df.queryExecution.executedPlan) { case scan: FileSourceScanExec =>
        scan.requiredSchema
      }
    assert(
      fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
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
      val query1 = spark
        .table("contacts")
        .select("friends.First", "friends.MiDDle")
      checkScan(query1, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(
        query1,
        Row(Array.empty[String], Array.empty[String]) ::
          Row(Array("Susan"), Array("Z.")) ::
          Row(null, null) ::
          Row(null, null) :: Nil)

      val query2 = spark
        .table("contacts")
        .where("friends.First is not null")
        .select("friends.First", "friends.MiDDle")
      checkScan(query2, "struct<friends:array<struct<first:string,middle:string>>>")
      checkAnswer(
        query2,
        Row(Array.empty[String], Array.empty[String]) ::
          Row(Array("Susan"), Array("Z.")) :: Nil)
    }
  }

  testSchemaPruning("SPARK-34963: extract case-insensitive struct field from struct") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val query1 = spark
        .table("contacts")
        .select("Name.First", "NAME.MiDDle")
      checkScan(query1, "struct<name:struct<first:string,middle:string>>")
      checkAnswer(
        query1,
        Row("Jane", "X.") ::
          Row("Janet", null) ::
          Row("Jim", null) ::
          Row("John", "Y.") :: Nil)

      val query2 = spark
        .table("contacts")
        .where("Name.MIDDLE is not null")
        .select("Name.First", "NAME.MiDDle")
      checkScan(query2, "struct<name:struct<first:string,middle:string>>")
      checkAnswer(
        query2,
        Row("Jane", "X.") ::
          Row("John", "Y.") :: Nil)
    }
  }

  test("SPARK-34638: queries should not fail on unsupported cases") {
    withTable("nested_array") {
      sql(
        "select * from values array(array(named_struct('a', 1, 'b', 3), " +
          "named_struct('a', 2, 'b', 4))) T(items)").write.saveAsTable("nested_array")
      val query = sql(
        "select d.a from (select explode(c) d from " +
          "(select explode(items) c from nested_array))")
      checkAnswer(query, Row(1) :: Row(2) :: Nil)
    }

    withTable("map") {
      sql(
        "select * from values map(1, named_struct('a', 1, 'b', 3), " +
          "2, named_struct('a', 2, 'b', 4)) T(items)").write.saveAsTable("map")
      val query = sql("select d.a from (select explode(items) (c, d) from map)")
      checkAnswer(query, Row(1) :: Row(2) :: Nil)
    }
  }

  test("SPARK-36352: Spark should check result plan's output schema name") {
    withMixedCaseData {
      val query = sql("select cOL1, cOl2.B from mixedcase")
      assert(
        query.queryExecution.executedPlan.schema.catalogString ==
          "struct<cOL1:string,B:int>")
      checkAnswer(
        query.orderBy("id"),
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

        spark.read
          .format(dataSourceName)
          .load(path)
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

    val schema = StructType(
      Seq(StructField(
        "array",
        ArrayType(
          StructType(
            Seq(
              StructField("string", StringType, false),
              StructField(
                "inner_array",
                ArrayType(StructType(Seq(StructField("inner_string", StringType, false))), true),
                false))),
          false))))

    val count = spark
      .createDataFrame(sparkContext.emptyRDD[Row], schema)
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

      val query = sql("""SELECT name FROM contacts c
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

      val query = sql("""SELECT name FROM contacts c
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

      val query = sql("""SELECT name FROM contacts c
          |WHERE
          |  id IN (SELECT * FROM ids i WHERE c.pets > i.value)
          |  AND
          |  name.first IN (SELECT * FROM first_names n WHERE c.name.last < n.value)
          |""".stripMargin)

      checkScan(
        query,
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

      val query = sql("""SELECT name FROM contacts c
          |WHERE
          |  id NOT IN (SELECT * FROM ids i WHERE c.pets > i.value)
          |  AND
          |  name.first NOT IN (SELECT * FROM first_names n WHERE c.name.last > n.value)
          |""".stripMargin)

      checkScan(
        query,
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
    checkAnswer(query1.orderBy("id"), Row("Susan", "Smith"))

    // nested struct fields inside map values
    val query2 =
      sql("""
            |SELECT
            |element_at(relatives, "brother").first, element_at(relatives, "brother").middle
            |FROM contacts WHERE id = 0
            |""".stripMargin)
    checkScan(query2, "struct<id:int,relatives:map<string,struct<first:string,middle:string>>>")
    checkAnswer(query2.orderBy("id"), Row("John", "Y."))
  }

  testSchemaPruning("SPARK-41017: column pruning through 2 filters") {
    import testImplicits._
    val query = spark
      .table("contacts")
      .filter(rand() > 0.5)
      .filter(rand() < 0.8)
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
    checkScan(
      arrayQuery,
      """struct<friends:array<struct<first:string,middle:string,last:string>>,
          |employer:struct<id:int,company:struct<name:string,address:string>>>""".stripMargin)
    checkAnswer(
      arrayQuery,
      Row(Row("abc", "123 Business Street"), "Susan") ::
        Row(null, null) :: Row(null, null) :: Row(null, null) :: Nil)

    val mapQuery =
      sql("""
            |SELECT
            |employer.id, relatives[employer.company.name].first
            |FROM contacts
            |""".stripMargin)
    checkScan(
      mapQuery,
      """struct<relatives:map<string,struct<first:string,middle:string,last:string>>,
          |employer:struct<id:int,company:struct<name:string>>>""".stripMargin)
    checkAnswer(
      mapQuery,
      Row(0, null) :: Row(1, null) ::
        Row(null, null) :: Row(null, null) :: Nil)
  }

  testSchemaPruning("SPARK-XXXXX: multi-field explode column pruning optimization") {
    // Create test data with nested structure similar to pageviews -> requests -> items

    val testData = PageView(
      1,
      "user1",
      Array(
        Request(
          101,
          "2023-01-01",
          "browser1",
          Array(Item(201, "item1", 10.0, "cat1"), Item(202, "item2", 20.0, "cat2"))),
        Request(102, "2023-01-02", "browser2", Array(Item(203, "item3", 30.0, "cat3"))))) :: Nil

    withDataSourceTable(testData, "pageviews") {
      // Query that only needs itemId and itemName - should prune itemPrice, itemCategory,
      // userAgent, timestamp
      val query1 = spark
        .table("pageviews")
        .select(explode(col("requests")).as("request"))
        .select(explode(col("request.items")).as("item"))
        .select("item.itemId", "item.itemName")

      // With the optimization, unnecessary fields should be pruned
      checkScan(
        query1,
        "struct<requests:array<struct<items:array<struct<itemId:int,itemName:string>>>>>")
      checkAnswer(query1, Row(201, "item1") :: Row(202, "item2") :: Row(203, "item3") :: Nil)

      // Query with multiple nested levels - should still optimize
      val query2 = spark
        .table("pageviews")
        .select("pageId", "requests.requestId", "requests.items.itemId")
      // Should prune unneeded fields at all levels
      checkScan(
        query2,
        "struct<pageId:int,requests:array<struct<requestId:int,items:array<struct<itemId:int>>>>>")
    }
  }

  testSchemaPruning("SPARK-XXXXX: explode with filter and multiple field selection") {
    // Test that optimization works even with filters

    val testData = Container(
      1,
      Array(
        NestedItem(1, "item1", 10.0, "cat1", true),
        NestedItem(2, "item2", 20.0, "cat2", false),
        NestedItem(3, "item3", 30.0, "cat3", true))) :: Nil

    withDataSourceTable(testData, "containers") {
      val query = spark
        .table("containers")
        .select(explode(col("items")).as("item"))
        .filter("item.active = true")
        .select("item.id", "item.name")

      // Should prune price and category fields, keep active for filter
      checkScan(query, "struct<items:array<struct<id:int,name:string,active:boolean>>>")
      checkAnswer(query, Row(1, "item1") :: Row(3, "item3") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: nested explode with aggregation") {
    // Test that optimization works with aggregations on exploded data
    val testData = AggUser(
      1,
      "user1",
      Array(
        AggSession(
          101,
          3600,
          Array(
            AggEvent(1001, "login", "auth"),
            AggEvent(1002, "click", "interaction"),
            AggEvent(1003, "purchase", "transaction"))),
        AggSession(
          102,
          1800,
          Array(AggEvent(1004, "login", "auth"), AggEvent(1005, "logout", "auth"))))) :: Nil

    withDataSourceTable(testData, "users") {
      val query = spark
        .table("users")
        .select(explode(col("sessions")).as("session"))
        .select(explode(col("session.events")).as("event"))
        .groupBy("event.eventType")
        .count()

      // Should only read eventType field, prune eventId, eventName, duration, userName
      checkScan(query, "struct<sessions:array<struct<events:array<struct<eventType:string>>>>>")
      checkAnswer(query, Row("auth", 3) :: Row("interaction", 1) :: Row("transaction", 1) :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: multi-format support - JSON with nested explode") {

    val testData = JsonContainer(
      1,
      Array(
        JsonNestedItem(101, "item1", 10.5, Array("tag1", "tag2")),
        JsonNestedItem(102, "item2", 20.0, Array("tag3", "tag4", "tag5"))),
      "test-metadata") :: Nil

    withDataSourceTable(testData, "json_containers") {
      val query = spark
        .table("json_containers")
        .select(explode(col("items")).as("item"))
        .select("item.id", "item.name")

      // Should prune price, tags, and metadata fields
      checkScan(query, "struct<items:array<struct<id:int,name:string>>>")
      checkAnswer(query, Row(101, "item1") :: Row(102, "item2") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: multi-level explode with Parquet optimization") {

    val testData = ParquetCustomerAdvanced(
      1,
      "John Doe",
      Array(
        ParquetOrderAdvanced(
          101,
          "2023-01-01",
          Array(ParquetProduct(201, "Product1", 10.0), ParquetProduct(202, "Product2", 20.0))),
        ParquetOrderAdvanced(
          102,
          "2023-01-02",
          Array(ParquetProduct(203, "Product3", 30.0))))) :: Nil

    withDataSourceTable(testData, "parquet_customers") {
      val query = spark
        .table("parquet_customers")
        .select(explode(col("orders")).as("order"))
        .select(explode(col("order.products")).as("product"))
        .select("product.productId", "product.productName")

      // Should prune customerName, orderDate, productPrice
      checkScan(
        query,
        "struct<orders:array<struct<products:array<struct<productId:int,productName:string>>>>>")
      checkAnswer(
        query,
        Row(201, "Product1") :: Row(202, "Product2") :: Row(203, "Product3") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: complex filter predicates with explode optimization") {

    val testData = FilterableContainer(
      1,
      "premium",
      Array(
        FilterableItem(1, "item1", "electronics", 100.0, true),
        FilterableItem(2, "item2", "books", 20.0, false),
        FilterableItem(3, "item3", "electronics", 150.0, true),
        FilterableItem(4, "item4", "clothing", 50.0, true))) :: Nil

    withDataSourceTable(testData, "filterable_containers") {
      // Complex filter with multiple conditions
      val query = spark
        .table("filterable_containers")
        .select(explode(col("items")).as("item"))
        .filter("item.active = true AND item.category = 'electronics' AND item.price > 50.0")
        .select("item.id", "item.name")

      // Should keep active, category, price for filter, prune containerType
      checkScan(
        query,
        "struct<items:array<struct<id:int,name:string,category:string,price:double,active:boolean>>>")
      checkAnswer(query, Row(1, "item1") :: Row(3, "item3") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: window functions over exploded nested data") {

    val testData = SalesData(
      2023,
      Array(
        SalesRecord(1, 1000.0, "North", 1),
        SalesRecord(2, 2000.0, "South", 1),
        SalesRecord(3, 1500.0, "North", 2),
        SalesRecord(4, 2500.0, "South", 2)),
      7000.0) :: Nil

    withDataSourceTable(testData, "sales_data") {
      val query = spark
        .table("sales_data")
        .select(explode(col("records")).as("record"))
        .select(
          col("record.salesId"),
          col("record.amount"),
          col("record.region"),
          row_number()
            .over(
              Window
                .partitionBy(col("record.region"))
                .orderBy(col("record.amount").desc))
            .as("rank"))

      // Should prune year, quarter, totalRevenue
      checkScan(query, "struct<records:array<struct<salesId:int,amount:double,region:string>>>")
      checkAnswer(
        query,
        Row(2, 2000.0, "South", 1) :: Row(4, 2500.0, "South", 1) ::
          Row(1, 1000.0, "North", 2) :: Row(3, 1500.0, "North", 2) :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: lateral view with multiple explodes") {

    val testData = DocumentWithTags(
      1,
      "Test Document",
      Array(Tag(101, "spark", "technology"), Tag(102, "sql", "technology")),
      Array("tutorial", "advanced")) :: Nil

    withDataSourceTable(testData, "documents") {
      // Using SQL lateral view syntax
      val query = sql("""
        SELECT tag.tagId, tag.tagName, category
        FROM documents 
        LATERAL VIEW explode(tags) t AS tag
        LATERAL VIEW explode(categories) c AS category
        WHERE tag.tagType = 'technology'
      """)

      // Should prune title, tagType is kept for filter
      checkScan(
        query,
        "struct<tags:array<struct<tagId:int,tagName:string,tagType:string>>,categories:array<string>>")
      checkAnswer(
        query,
        Row(101, "spark", "tutorial") :: Row(101, "spark", "advanced") ::
          Row(102, "sql", "tutorial") :: Row(102, "sql", "advanced") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: join between exploded tables") {

    val leftData =
      LeftContainer(Array(LeftRecord(1, "left1", 10.0), LeftRecord(2, "left2", 20.0))) :: Nil

    val rightData = RightContainer(
      Array(RightRecord(1, "right1", "code1"), RightRecord(2, "right2", "code2"))) :: Nil

    withDataSourceTable(leftData, "left_containers") {
      withDataSourceTable(rightData, "right_containers") {

        val query = spark
          .table("left_containers")
          .select(explode(col("leftItems")).as("left"))
          .join(
            spark
              .table("right_containers")
              .select(explode(col("rightItems")).as("right")),
            col("left.id") === col("right.id"))
          .select("left.id", "left.leftData", "right.rightCode")

        // Should prune leftValue and rightData
        checkAnswer(query, Row(1, "left1", "code1") :: Row(2, "left2", "code2") :: Nil)
      }
    }
  }

  testSchemaPruning("SPARK-XXXXX: subquery with exploded data") {

    val testData = SubContainer(
      1,
      Array(
        SubItem(101, "item1", "active", 100.0),
        SubItem(102, "item2", "inactive", 200.0),
        SubItem(103, "item3", "active", 150.0)),
      "standard") :: Nil

    withDataSourceTable(testData, "sub_containers") {
      val query = sql("""
        SELECT item.itemId, item.itemName
        FROM (
          SELECT explode(items) as item 
          FROM sub_containers 
          WHERE containerType = 'standard'
        ) exploded
        WHERE item.itemStatus = 'active'
      """)

      // Should prune itemValue, keep itemStatus and containerType for filters
      checkAnswer(query, Row(101, "item1") :: Row(103, "item3") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: CTE with exploded nested data") {

    val testData = CteData(
      1,
      Array(
        CteRecord(1, "record1", "typeA", 10.0),
        CteRecord(2, "record2", "typeB", 20.0),
        CteRecord(3, "record3", "typeA", 30.0)),
      "test-meta") :: Nil

    withDataSourceTable(testData, "cte_data") {
      val query = sql("""
        WITH exploded_records AS (
          SELECT explode(records) as record, metadata
          FROM cte_data
        ),
        filtered_records AS (
          SELECT record.recordId, record.recordName, record.recordType
          FROM exploded_records
          WHERE record.recordType = 'typeA'
        )
        SELECT recordId, recordName
        FROM filtered_records
      """)

      // Should prune recordData and metadata
      checkAnswer(query, Row(1, "record1") :: Row(3, "record3") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: performance test - very wide schema with selective access") {
    // Create a very wide schema to test pruning efficiency
    val wideFields = (1 to 100).map(i => s"field$i" -> s"value$i")
    val wideItem = wideFields.toMap + ("id" -> "1") + ("name" -> "test")
    val wideSchema = spark.read.json(spark.sparkContext.parallelize(Seq(s"""{"items": [${wideItem
        .map { case (k, v) => s""""$k": "$v"""" }
        .mkString("{", ",", "}")}]}""")))

    wideSchema.createOrReplaceTempView("wide_data")

    val query = sql("""
      SELECT item.id, item.name
      FROM wide_data
      LATERAL VIEW explode(items) t AS item
    """)

    // Should dramatically prune the schema to only include id and name
    checkScan(query, "struct<items:array<struct<id:string,name:string>>>")
    checkAnswer(query, Row("1", "test") :: Nil)
  }

  testSchemaPruning("SPARK-XXXXX: exploding nested structs within structs") {

    val testData = CompanyTag(
      1,
      Array(
        EmployeeTag(
          101,
          PersonalInfoTag("John", "Doe", AddressTag("123 Main St", "NYC", "10001", "USA"), 30),
          "Engineering",
          75000.0),
        EmployeeTag(
          102,
          PersonalInfoTag("Jane", "Smith", AddressTag("456 Oak Ave", "SF", "94102", "USA"), 28),
          "Marketing",
          65000.0)),
      "TechCorp") :: Nil

    withDataSourceTable(testData, "companies") {
      val query = spark
        .table("companies")
        .select(explode(col("employees")).as("emp"))
        .select("emp.empId", "emp.personalInfo.firstName", "emp.personalInfo.address.city")

      // Should prune salary, department, companyName, lastName, age, street, zipCode, country
      checkScan(
        query,
        "struct<employees:array<struct<empId:int,personalInfo:struct<firstName:string,address:struct<city:string>>>>>")
      checkAnswer(query, Row(101, "John", "NYC") :: Row(102, "Jane", "SF") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: nested list of structs containing lists of structs") {

    val testData = OrganizationTag(
      1,
      Array(
        DepartmentTag(
          10,
          "Engineering",
          Array(
            ProjectTag(
              100,
              "Project A",
              Array(
                SkillTag(1, "Scala", "Expert", true),
                SkillTag(2, "Spark", "Advanced", false)),
              100000.0),
            ProjectTag(
              101,
              "Project B",
              Array(SkillTag(3, "Python", "Intermediate", true)),
              75000.0)),
          "Alice"),
        DepartmentTag(
          20,
          "Data Science",
          Array(
            ProjectTag(
              200,
              "ML Project",
              Array(
                SkillTag(4, "TensorFlow", "Advanced", true),
                SkillTag(5, "Statistics", "Expert", false)),
              150000.0)),
          "Bob")),
      "Tech") :: Nil

    withDataSourceTable(testData, "organizations") {
      // Three-level exploding: departments -> projects -> skills
      val query = spark
        .table("organizations")
        .select(explode(col("departments")).as("dept"))
        .select(explode(col("dept.projects")).as("project"))
        .select(explode(col("project.skills")).as("skill"))
        .select("skill.skillId", "skill.skillName", "skill.level")

      // Should prune orgType, manager, budget, certified, and other unused fields
      checkScan(
        query,
        """struct<departments:array<struct<projects:array<struct<skills:array<struct<skillId:int,skillName:string,level:string>>>>>>>""")
      checkAnswer(
        query,
        Row(1, "Scala", "Expert") :: Row(2, "Spark", "Advanced") ::
          Row(3, "Python", "Intermediate") :: Row(4, "TensorFlow", "Advanced") ::
          Row(5, "Statistics", "Expert") :: Nil)
    }
  }

  testSchemaPruning("SPARK-XXXXX: nested map of structs with explode") {
    // Using raw SQL since case classes don't support maps easily
    val mapData = spark.sql("""
      SELECT map(
        'user1', named_struct('userId', 1, 'userName', 'John', 'userAge', 25, 'userEmail', 'john@test.com'),
        'user2', named_struct('userId', 2, 'userName', 'Jane', 'userAge', 30, 'userEmail', 'jane@test.com')
      ) as userMap,
      'active' as status,
      array(
        map('config1', named_struct('configId', 101, 'configValue', 'value1', 'configType', 'string')),
        map('config2', named_struct('configId', 102, 'configValue', 'value2', 'configType', 'number'))
      ) as configMaps
    """)

    mapData.createOrReplaceTempView("map_data")

    val query = sql("""
      SELECT user.key as userKey, user.value.userId, user.value.userName
      FROM map_data
      LATERAL VIEW explode(userMap) t AS user
    """)

    // Should prune status, configMaps, userAge, userEmail
    checkAnswer(query, Row("user1", 1, "John") :: Row("user2", 2, "Jane") :: Nil)
  }

  testSchemaPruning("SPARK-XXXXX: complex nested arrays with maps and structs") {
    val complexData = spark.sql("""
      SELECT array(
        named_struct(
          'regionId', 1,
          'regionName', 'North',
          'stores', array(
            named_struct(
              'storeId', 101,
              'storeName', 'Store A',
              'products', map(
                'electronics', array(
                  named_struct('productId', 1001, 'productName', 'Laptop', 'price', 999.99, 'category', 'computers'),
                  named_struct('productId', 1002, 'productName', 'Mouse', 'price', 19.99, 'category', 'accessories')
                ),
                'books', array(
                  named_struct('productId', 2001, 'productName', 'Scala Book', 'price', 49.99, 'category', 'programming')
                )
              ),
              'revenue', 50000.0
            )
          )
        )
      ) as regions,
      'Q1-2023' as quarter
    """)

    complexData.createOrReplaceTempView("complex_data")

    val query = sql("""
      SELECT store.storeId, product.productId, product.productName
      FROM complex_data
      LATERAL VIEW explode(regions) r AS region
      LATERAL VIEW explode(region.stores) s AS store  
      LATERAL VIEW explode(store.products) p AS productType, products
      LATERAL VIEW explode(products) prod AS product
    """)

    // Should prune quarter, regionName, storeName, revenue, price, category
    checkAnswer(
      query,
      Row(101, 1001, "Laptop") :: Row(101, 1002, "Mouse") :: Row(101, 2001, "Scala Book") :: Nil)
  }

  testSchemaPruning("SPARK-XXXXX: mixed nested structures with conditional access") {

    // Simulate this with SQL since maps in case classes are complex
    val mixedData = spark.sql("""
      SELECT array(
        named_struct(
          'customerId', 1,
          'profile', named_struct(
            'profileId', 101,
            'contacts', array(
              named_struct('phone', '123-456-7890', 'email', 'test1@example.com', 'preferred', 'email'),
              named_struct('phone', '987-654-3210', 'email', 'test2@example.com', 'preferred', 'phone')
            ),
            'preferences', map('newsletter', 'true', 'sms', 'false')
          ),
          'orders', array(1001, 1002, 1003),
          'active', true
        )
      ) as customers,
      '2023-01-01' as lastUpdated
    """)

    mixedData.createOrReplaceTempView("customer_data")

    val query = sql("""
      SELECT customer.customerId, contact.phone, contact.preferred
      FROM customer_data
      LATERAL VIEW explode(customers) c AS customer
      LATERAL VIEW explode(customer.profile.contacts) ct AS contact
      WHERE contact.preferred = 'phone'
    """)

    // Should prune lastUpdated, orders, active, profileId, preferences, email
    checkAnswer(query, Row(1, "987-654-3210", "phone") :: Nil)
  }

  testSchemaPruning("SPARK-XXXXX: deeply nested struct in array optimization") {
    val deepData = RootLevelTag(
      Array(
        Level1Tag(
          "f1",
          Level2Tag("f2", Level3Tag("f3", Level4Tag("f4", 42, "unused"), 3.14), false),
          Array("u1")),
        Level1Tag(
          "f1b",
          Level2Tag("f2b", Level3Tag("f3b", Level4Tag("f4b", 43, "unused2"), 2.71), true),
          Array("u2"))),
      "root") :: Nil

    withDataSourceTable(deepData, "deep_data") {
      val query = spark
        .table("deep_data")
        .select(explode(col("items")).as("item"))
        .select(
          "item.field1",
          "item.level2.field2",
          "item.level2.level3.level4.field4",
          "item.level2.level3.level4.data4")

      // Should prune rootField, unused1, unused2, unused3, unused4, field3
      checkScan(
        query,
        """struct<items:array<struct<field1:string,level2:struct<field2:string,level3:struct<level4:struct<field4:string,data4:int>>>>>>""")
      checkAnswer(query, Row("f1", "f2", "f4", 42) :: Row("f1b", "f2b", "f4b", 43) :: Nil)
    }
  }
}
