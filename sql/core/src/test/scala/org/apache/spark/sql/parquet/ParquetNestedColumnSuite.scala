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

package org.apache.spark.sql.parquet

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._


class ParquetNestedColumnSuite extends QueryTest with FunSuiteLike with BeforeAndAfterAll {
  TestData

  override def beforeAll() {
    ParquetTestData.writeNestedFile1()
    ParquetTestData.writeNestedFile2()
    ParquetTestData.writeNestedFile3()
    ParquetTestData.writeNestedFile4()
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testNestedDir1)
    Utils.deleteRecursively(ParquetTestData.testNestedDir2)
    Utils.deleteRecursively(ParquetTestData.testNestedDir3)
    Utils.deleteRecursively(ParquetTestData.testNestedDir4)
  }

  test("Projection in addressbook") {
    val data = parquetFile(ParquetTestData.testNestedDir1.toString).toSchemaRDD
    data.registerTempTable("data")
    val query = sql("SELECT owner, contacts[1].name FROM data")
    val tmp = query.collect()
    assert(tmp.size === 2)
    assert(tmp(0).size === 2)
    assert(tmp(0)(0) === "Julien Le Dem")
    assert(tmp(0)(1) === "Chris Aniszczyk")
    assert(tmp(1)(0) === "A. Nonymous")
    assert(tmp(1)(1) === null)
  }

  test("Simple query on nested int data") {
    val data = parquetFile(ParquetTestData.testNestedDir2.toString).toSchemaRDD
    data.registerTempTable("data")
    val result1 = sql("SELECT entries[0].value FROM data").collect()
    assert(result1.size === 1)
    assert(result1(0).size === 1)
    assert(result1(0)(0) === 2.5)
    val result2 = sql("SELECT entries[0] FROM data").collect()
    assert(result2.size === 1)
    val subresult1 = result2(0)(0).asInstanceOf[CatalystConverter.StructScalaType[_]]
    assert(subresult1.size === 2)
    assert(subresult1(0) === 2.5)
    assert(subresult1(1) === false)
    val result3 = sql("SELECT outerouter FROM data").collect()
    val subresult2 = result3(0)(0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(subresult2(0).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 7)
    assert(subresult2(1).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 8)
    assert(result3(0)(0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](1)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 9)
  }

  test("nested structs") {
    val data = parquetFile(ParquetTestData.testNestedDir3.toString)
      .toSchemaRDD
    data.registerTempTable("data")
    val result1 = sql("SELECT booleanNumberPairs[0].value[0].truth FROM data").collect()
    assert(result1.size === 1)
    assert(result1(0).size === 1)
    assert(result1(0)(0) === false)
    val result2 = sql("SELECT booleanNumberPairs[0].value[1].truth FROM data").collect()
    assert(result2.size === 1)
    assert(result2(0).size === 1)
    assert(result2(0)(0) === true)
    val result3 = sql("SELECT booleanNumberPairs[1].value[0].truth FROM data").collect()
    assert(result3.size === 1)
    assert(result3(0).size === 1)
    assert(result3(0)(0) === false)
  }

  test("simple map") {
    val data = TestSQLContext
      .parquetFile(ParquetTestData.testNestedDir4.toString)
      .toSchemaRDD
    data.registerTempTable("mapTable")
    val result1 = sql("SELECT data1 FROM mapTable").collect()
    assert(result1.size === 1)
    assert(result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, _]]
      .getOrElse("key1", 0) === 1)
    assert(result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, _]]
      .getOrElse("key2", 0) === 2)
    val result2 = sql( """SELECT data1["key1"] FROM mapTable""").collect()
    assert(result2(0)(0) === 1)
  }

  test("map with struct values") {
    val data = parquetFile(ParquetTestData.testNestedDir4.toString).toSchemaRDD
    data.registerTempTable("mapTable")
    val result1 = sql("SELECT data2 FROM mapTable").collect()
    assert(result1.size === 1)
    val entry1 = result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("seven", null)
    assert(entry1 != null)
    assert(entry1(0) === 42)
    assert(entry1(1) === "the answer")
    val entry2 = result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("eight", null)
    assert(entry2 != null)
    assert(entry2(0) === 49)
    assert(entry2(1) === null)
    val result2 = sql( """SELECT data2["seven"].payload1, data2["seven"].payload2 FROM mapTable""").collect()
    assert(result2.size === 1)
    assert(result2(0)(0) === 42.toLong)
    assert(result2(0)(1) === "the answer")
  }
}
