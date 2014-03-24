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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.hive.TestHive

class HiveParquetSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  val filename = getTempFilePath("parquettest").getCanonicalFile.toURI.toString

  // runs a SQL and optionally resolves one Parquet table
  def runQuery(
      querystr: String,
      tableName: Option[String] = None,
      filename: Option[String] = None): Array[Row] = {

    // call to resolve references in order to get CREATE TABLE AS to work
    val query = TestHive
      .parseSql(querystr)
    val finalQuery =
      if (tableName.nonEmpty && filename.nonEmpty)
        resolveParquetTable(tableName.get, filename.get, query)
      else
        query
    TestHive.executePlan(finalQuery)
      .toRdd
      .collect()
  }

  // stores a query output to a Parquet file
  def storeQuery(querystr: String, filename: String): Unit = {
    val query = WriteToFile(
      filename,
      TestHive.parseSql(querystr))
    TestHive
      .executePlan(query)
      .stringResult()
  }

  /**
   * TODO: This function is necessary as long as there is no notion of a Catalog for
   * Parquet tables. Once such a thing exists this functionality should be moved there.
   */
  def resolveParquetTable(tableName: String, filename: String, plan: LogicalPlan): LogicalPlan = {
    TestHive.loadTestTable("src") // may not be loaded now
    plan.transform {
      case relation @ UnresolvedRelation(databaseName, name, alias) =>
        if (name == tableName)
          ParquetRelation(tableName, filename)
        else
          relation
      case op @ InsertIntoCreatedTable(databaseName, name, child) =>
        if (name == tableName) {
          // note: at this stage the plan is not yet analyzed but Parquet needs to know the schema
          // and for that we need the child to be resolved
          val relation = ParquetRelation.create(
              filename,
              TestHive.analyzer(child),
              TestHive.sparkContext.hadoopConfiguration,
              Some(tableName))
          InsertIntoTable(
            relation.asInstanceOf[BaseRelation],
            Map.empty,
            child,
            overwrite = false)
        } else
          op
    }
  }

  override def beforeAll() {
    // write test data
    ParquetTestData.writeFile()
    // Override initial Parquet test table
    TestHive.catalog.registerTable(Some[String]("parquet"), "testsource", ParquetTestData.testData)
  }

  override def afterAll() {
    ParquetTestData.testFile.delete()
  }

  override def beforeEach() {
    new File(filename).getAbsoluteFile.delete()
  }

  override def afterEach() {
    new File(filename).getAbsoluteFile.delete()
  }

  test("SELECT on Parquet table") {
    val rdd = runQuery("SELECT * FROM parquet.testsource")
    assert(rdd != null)
    assert(rdd.forall(_.size == 6))
  }

  test("Simple column projection + filter on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM parquet.testsource WHERE myboolean=true")
    assert(rdd.size === 5, "Filter returned incorrect number of rows")
    assert(rdd.forall(_.getBoolean(0)), "Filter returned incorrect Boolean field value")
  }

  test("Converting Hive to Parquet Table via WriteToFile") {
    storeQuery("SELECT * FROM src", filename)
    val rddOne = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    val rddTwo = runQuery("SELECT * from ptable", Some("ptable"), Some(filename)).sortBy(_.getInt(0))
    compareRDDs(rddOne, rddTwo, "src (Hive)", Seq("key:Int", "value:String"))
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    storeQuery("SELECT * FROM parquet.testsource", filename)
    runQuery("INSERT OVERWRITE TABLE ptable SELECT * FROM parquet.testsource", Some("ptable"), Some(filename))
    runQuery("INSERT OVERWRITE TABLE ptable SELECT * FROM parquet.testsource", Some("ptable"), Some(filename))
    val rddCopy = runQuery("SELECT * FROM ptable", Some("ptable"), Some(filename))
    val rddOrig = runQuery("SELECT * FROM parquet.testsource")
    compareRDDs(rddOrig, rddCopy, "parquet.testsource", ParquetTestData.testSchemaFieldNames)
  }

  test("CREATE TABLE AS Parquet table") {
    runQuery("CREATE TABLE ptable AS SELECT * FROM src", Some("ptable"), Some(filename))
    val rddCopy = runQuery("SELECT * FROM ptable", Some("ptable"), Some(filename))
      .sortBy[Int](_.apply(0) match {
        case x: Int => x
        case _ => 0
      })
    val rddOrig = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    compareRDDs(rddOrig, rddCopy, "src (Hive)", Seq("key:Int", "value:String"))
  }

  private def compareRDDs(rddOne: Array[Row], rddTwo: Array[Row], tableName: String, fieldNames: Seq[String]) {
    var counter = 0
    (rddOne, rddTwo).zipped.foreach {
      (a,b) => (a,b).zipped.toArray.zipWithIndex.foreach {
        case ((value_1:Array[Byte], value_2:Array[Byte]), index) =>
          assert(new String(value_1) === new String(value_2), s"table $tableName row $counter field ${fieldNames(index)} don't match")
        case ((value_1, value_2), index) =>
          assert(value_1 === value_2, s"table $tableName row $counter field ${fieldNames(index)} don't match")
      }
    counter = counter + 1
    }
  }
}
