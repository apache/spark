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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/* A test JdbcRelationProvider used to provide persistent schema */
class TestJdbcRelationProvider extends RelationProvider {
  override def createRelation(sqlCtx: SQLContext, parameters: Map[String, String])
  : BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = sqlCtx
      override def schema: StructType = {
        new StructType().add(StructField("a", StringType)).add(StructField("b", IntegerType))
      }
    }
  }
}

class JdbcUtilsSuite extends SharedSparkSession {

  val tableSchema = StructType(Seq(
    StructField("C1", StringType, false), StructField("C2", IntegerType, false)))
  val caseSensitive = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
  val caseInsensitive = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  test("Parse user specified column types") {
    assert(JdbcUtils.getCustomSchema(tableSchema, null, caseInsensitive) === tableSchema)
    assert(JdbcUtils.getCustomSchema(tableSchema, "", caseInsensitive) === tableSchema)

    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", IntegerType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", IntegerType, false))))

    assert(
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", StringType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", StringType, false))))

    // Throw AnalysisException
    val duplicate = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, c1 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, false), StructField("c1", StringType, false)))
    }
    assert(duplicate.getMessage.contains(
      "Found duplicate column(s) in the customSchema option value"))

    // Throw ParseException
    val dataTypeNotSupported = intercept[ParseException]{
      JdbcUtils.getCustomSchema(tableSchema, "c3 DATEE, C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, false), StructField("C2", StringType, false)))
    }
    assert(dataTypeNotSupported.getMessage.contains("DataType datee is not supported"))

    val mismatchedInput = intercept[ParseException]{
      JdbcUtils.getCustomSchema(tableSchema, "c3 DATE. C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, false), StructField("C2", StringType, false)))
    }
    assert(mismatchedInput.getMessage.contains("mismatched input '.' expecting"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - number mismatch") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (a STRING)
    val specifiedSchema = new StructType()
      .add(StructField("a", StringType))
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(
      "The number of fields between persistent schema and user specified schema mismatch"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - wrong name") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (a STRING, c INT)
    val specifiedSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("c", IntegerType)) // wrong field name
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(s"persistentFields: ${persistentSchema("b").toDDL}"))
    assert(msg.contains(s"specifiedFields: ${specifiedSchema("c").toDDL}"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - wrong type") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (a STRING, b STRING)
    val specifiedSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StringType)) // wrong filed type
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(s"persistentFields: ${persistentSchema("b").toDDL}"))
    assert(msg.contains(s"specifiedFields: ${specifiedSchema("b").toDDL}"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - wrong name & type") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (a STRING, c STRING)
    val specifiedSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("c", StringType)) // wrong filed name and type
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(s"persistentFields: ${persistentSchema("b").toDDL}"))
    assert(msg.contains(s"specifiedFields: ${specifiedSchema("c").toDDL}"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - wrong order") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (b INT, a STRING)
    val specifiedSchema = new StructType() // wrong order
      .add(StructField("b", IntegerType))
      .add(StructField("a", StringType))
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(s"persistentFields: ${persistentSchema.map(_.toDDL).mkString(", ")}"))
    assert(msg.contains(s"specifiedFields: ${specifiedSchema.map(_.toDDL).mkString(", ")}"))
  }

  test("SPARK-30151: user-specified schema not match relation schema - complex type") {
    // persistent: (a STRING, b INT)
    val persistentSchema =
      DataSource(spark, classOf[TestJdbcRelationProvider].getCanonicalName)
        .resolveRelation()
        .schema
    // specified: (a STRING, b STRUCT<c INT>)
    val specifiedSchema = new StructType()
      .add(StructField("a", StringType))
      .add(StructField("b", StructType(StructField("c", IntegerType) :: Nil))) // complex type
    val msg = intercept[AnalysisException] {
      DataSource(
        spark,
        classOf[TestJdbcRelationProvider].getCanonicalName,
        userSpecifiedSchema = Some(specifiedSchema))
        .resolveRelation()
    }.getMessage
    assert(msg.contains(s"persistentFields: ${persistentSchema("b").toDDL}"))
    assert(msg.contains(s"specifiedFields: ${specifiedSchema("b").toDDL}"))
  }
}
