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

package org.apache.spark.sql.hive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive

/* Implicits */
import org.apache.spark.sql.hive.test.TestHive._

case class TestData(key: Int, value: String)

class InsertIntoHiveTableSuite extends QueryTest {
  val testData = TestHive.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString)))
  testData.registerTempTable("testData")

  test("insertInto() HiveTable") {
    createTable[TestData]("createAndInsertTest")

    // Add some data.
    testData.insertInto("createAndInsertTest")

    // Make sure the table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq
    )

    // Add more data.
    testData.insertInto("createAndInsertTest")

    // Make sure the table has been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq ++ testData.collect().toSeq
    )

    // Now overwrite.
    testData.insertInto("createAndInsertTest", overwrite = true)

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq
    )
  }

  test("Double create fails when allowExisting = false") {
    createTable[TestData]("doubleCreateAndInsertTest")

    intercept[org.apache.hadoop.hive.ql.metadata.HiveException] {
      createTable[TestData]("doubleCreateAndInsertTest", allowExisting = false)
    }
  }

  test("Double create does not fail when allowExisting = true") {
    createTable[TestData]("createAndInsertTest")
    createTable[TestData]("createAndInsertTest")
  }

  test("SPARK-4052: scala.collection.Map as value type of MapType") {
    val schema = StructType(StructField("m", MapType(StringType, StringType), true) :: Nil)
    val rowRDD = TestHive.sparkContext.parallelize(
      (1 to 100).map(i => Row(scala.collection.mutable.HashMap(s"key$i" -> s"value$i"))))
    val schemaRDD = applySchema(rowRDD, schema)
    schemaRDD.registerTempTable("tableWithMapValue")
    sql("CREATE TABLE hiveTableWithMapValue(m MAP <STRING, STRING>)")
    sql("INSERT OVERWRITE TABLE hiveTableWithMapValue SELECT m FROM tableWithMapValue")

    checkAnswer(
      sql("SELECT * FROM hiveTableWithMapValue"),
      rowRDD.collect().toSeq
    )

    sql("DROP TABLE hiveTableWithMapValue")
  }
}
