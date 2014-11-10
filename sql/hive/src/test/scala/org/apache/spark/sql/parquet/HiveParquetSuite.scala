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

import java.io.File

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Row}
import org.apache.spark.sql.catalyst.types.{DataType, StringType, IntegerType}
import org.apache.spark.sql.{parquet, SchemaRDD}
import org.apache.spark.util.Utils

// Implicits
import org.apache.spark.sql.hive.test.TestHive._

case class Cases(lower: String, UPPER: String)

class HiveParquetSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val dirname = Utils.createTempDir()

  var testRDD: SchemaRDD = null

  override def beforeAll() {
    // write test data
    ParquetTestData.writeFile()
    testRDD = parquetFile(ParquetTestData.testDir.toString)
    testRDD.registerTempTable("testsource")
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testDir)
    Utils.deleteRecursively(dirname)
    reset() // drop all tables that were registered as part of the tests
  }

  // in case tests are failing we delete before and after each test
  override def beforeEach() {
    Utils.deleteRecursively(dirname)
  }

  override def afterEach() {
    Utils.deleteRecursively(dirname)
  }

  test("Case insensitive attribute names") {
    val tempFile = File.createTempFile("parquet", "")
    tempFile.delete()
    sparkContext.parallelize(1 to 10)
      .map(_.toString)
      .map(i => Cases(i, i))
      .saveAsParquetFile(tempFile.getCanonicalPath)

    parquetFile(tempFile.getCanonicalPath).registerTempTable("cases")
    sql("SELECT upper FROM cases").collect().map(_.getString(0)) === (1 to 10).map(_.toString)
    sql("SELECT LOWER FROM cases").collect().map(_.getString(0)) === (1 to 10).map(_.toString)
  }

  test("SELECT on Parquet table") {
    val rdd = sql("SELECT * FROM testsource").collect()
    assert(rdd != null)
    assert(rdd.forall(_.size == 6))
  }

  test("Simple column projection + filter on Parquet table") {
    val rdd = sql("SELECT myboolean, mylong FROM testsource WHERE myboolean=true").collect()
    assert(rdd.size === 5, "Filter returned incorrect number of rows")
    assert(rdd.forall(_.getBoolean(0)), "Filter returned incorrect Boolean field value")
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    sql("SELECT * FROM src").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerTempTable("ptable")
    val rddOne = sql("SELECT * FROM src").collect().sortBy(_.getInt(0))
    val rddTwo = sql("SELECT * from ptable").collect().sortBy(_.getInt(0))

    compareRDDs(rddOne, rddTwo, "src (Hive)", Seq("key:Int", "value:String"))
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    sql("SELECT * FROM testsource").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerTempTable("ptable")
    // let's do three overwrites for good measure
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    val rddCopy = sql("SELECT * FROM ptable").collect()
    val rddOrig = sql("SELECT * FROM testsource").collect()
    assert(rddCopy.size === rddOrig.size, "INSERT OVERWRITE changed size of table??")
    compareRDDs(rddOrig, rddCopy, "testsource", ParquetTestData.testSchemaFieldNames)
  }

  private def compareRDDs(rddOne: Array[Row], rddTwo: Array[Row], tableName: String, fieldNames: Seq[String]) {
    var counter = 0
    (rddOne, rddTwo).zipped.foreach {
      (a,b) => (a,b).zipped.toArray.zipWithIndex.foreach {
        case ((value_1, value_2), index) =>
          assert(value_1 === value_2, s"table $tableName row $counter field ${fieldNames(index)} don't match")
      }
    counter = counter + 1
    }
  }
}
