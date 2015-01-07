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

package org.apache.spark.sql.hbase.api.java

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hbase.{HBaseMainTest, QueryTest}

import scala.util.Try

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.api.java.{JavaSQLContext, JavaSchemaRDD}
import org.apache.spark.sql.execution.ExplainCommand
import org.apache.spark.sql.hbase.HBaseMainTest._

// Implicits
import scala.collection.JavaConversions._

class JavaHbaseSuite extends QueryTest with BeforeAndAfterAll {
  // Make sure the tables are loaded.
//  TestData

  val hbc = {
    HBaseMainTest.main(null)
    HBaseMainTest.hbc
  }
  import hbc._

  lazy val javaCtx = new JavaSparkContext(HBaseMainTest.sc)

  // There is a little trickery here to avoid instantiating two HiveContexts in the same JVM
  lazy val javaHbaseCtx = new JavaHbaseContext(javaCtx)

  test("aggregation with codegen") {
    val originalValue = javaHbaseCtx.sqlContext.getConf(SQLConf.CODEGEN_ENABLED, "false")  //codegenEnabled
    javaHbaseCtx.sqlContext.setConf(SQLConf.CODEGEN_ENABLED, "true")
    val d = javaHbaseCtx.sql("SELECT col1 FROM ta GROUP BY col1").collect()
    d.foreach(println)
    javaHbaseCtx.sqlContext.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("dsl simple select") {
    val tableA = javaHbaseCtx.sql("SELECT * FROM ta").schemaRDD
//    tableA.foreach(println)
    checkAnswer(
      tableA.where('col7 === 1).orderBy('col2.asc).select('col4),
      Seq(Seq(1))
    )
  }

  test("metadata is propagated correctly") {
    val tableA = sql("SELECT col7, col1, col3 FROM ta")
    val schema = tableA.schema
    val docKey = "doc"
    val docValue = "first name"
    val metadata = new MetadataBuilder()
      .putString(docKey, docValue)
      .build()
    val schemaWithMeta = new StructType(Seq(
      schema("col7"), schema("col1").copy(metadata = metadata), schema("col3")))
    val personWithMeta = applySchema(tableA, schemaWithMeta)
    def validateMetadata(rdd: SchemaRDD): Unit = {
      assert(rdd.schema("col1").metadata.getString(docKey) == docValue)
    }
    personWithMeta.registerTempTable("personWithMeta")
    validateMetadata(personWithMeta.select('col1))
    validateMetadata(personWithMeta.select("col1".attr))
    validateMetadata(personWithMeta.select('col7, 'col1))
    validateMetadata(sql("SELECT * FROM personWithMeta"))
    validateMetadata(sql("SELECT col7, col1 FROM personWithMeta"))
    validateMetadata(sql("SELECT * FROM personWithMeta JOIN salary ON col7 = personId"))
    validateMetadata(sql("SELECT col1, salary FROM personWithMeta JOIN salary ON col7 = personId"))
  }


  test("Query Hive native command execution result") {
    val tableName = "test_native_commands"

//    assertResult(0) {
//      javaHbaseCtx.sql(s"""CREATE TABLE $tableName (column2 INTEGER, column1 INTEGER, column4 FLOAT,
//          column3 SHORT, PRIMARY KEY(column1, column2))
//          MAPPED BY (testNamespace.ht0, COLS=[column3=family1.qualifier1,
//          column4=family2.qualifier2])""").count()
//    }

    val a = javaHbaseCtx
      .sql("SHOW TABLES")
      .schemaRDD
    val b = sql("SHOW TABLES")

    println("\n###################")
    a.collect().foreach(println)
    println("###################")
    b.collect().foreach(println)
    println("###################")
//    assert(
//      javaHbaseCtx
//        .sql("SHOW TABLES")
//        .collect()
//        .map(_.getString(0))
//        .contains(tableName))

//    assertResult(0) {
//      javaHbaseCtx.sql(s"DROP TABLE $tableName").count()
//    }

//    assertResult(Array(Array("key", "int"), Array("value", "string"))) {
//      javaHbaseCtx
//        .sql(s"describe $tableName")
//        .collect()
//        .map(row => Array(row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[String]))
//        .toArray
//    }

//    assert(isExplanation(javaHbaseCtx.sql(
//      s"EXPLAIN SELECT key, COUNT(*) FROM $tableName GROUP BY key")))

//    TestHbase.reset()
  }

//  test("Exactly once semantics for DDL and command statements") {
//    val tableName = "test_exactly_once"
//    val q0 = javaHbaseCtx.sql(s"CREATE TABLE $tableName(key INT, value STRING)")
//
//    // If the table was not created, the following assertion would fail
//    assert(Try(TestHbase.table(tableName)).isSuccess)
//
//    // If the CREATE TABLE command got executed again, the following assertion would fail
//    assert(Try(q0.count()).isSuccess)
//  }
}
