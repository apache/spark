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

package org.apache.spark.sql.hive.execution

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, QueryTest}
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.sources.{TableScan, BaseRelation, RelationProvider}

class SimpleScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleScan(parameters("from").toInt, parameters("TO").toInt)(sqlContext)
  }
}

case class SimpleScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends TableScan {

  override def schema =
    StructType(StructField("i", IntegerType, nullable = false) :: Nil)

  override def buildScan() = sqlContext.sparkContext.parallelize(from to to).map(Row(_))
}

/**
 * A set of tests that validates support for Hive Explain command.
 */
class HiveDescribeSuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE TABLE my_desc_table(key INT, value STRING)")

    sql(
      s"""
        |CREATE TABLE my_desc_datasource
        |USING org.apache.spark.sql.hive.execution.SimpleScanSource
        |OPTIONS (
        | From '1',
        | To '10'
        |)
      """.stripMargin)

    sql("create view my_desc_view as select 1 as key, 2 as value from my_desc_datasource limit 1")
    sql("cache table my_desc_cache as select 1 as key, 2 as value from my_desc_datasource limit 1")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sql("drop view my_desc_view")
    sql("uncache table my_desc_cache")
    sql("drop table my_desc_datasource")
    sql("drop table my_desc_table")
  }

  test("describe command") {
    checkExistence(sql("describe my_desc_table"), true,
                   "key", "value")
    checkExistence(sql("describe my_desc_table"), false,
                   "Detailed Table Information")

    checkExistence(sql("desc my_desc_view"), true,
                   "key", "value")
    checkExistence(sql("describe my_desc_view"), false,
      "Detailed View Information")

    checkExistence(sql("desc my_desc_datasource"), true,
      "i", "from deserializer")
    checkExistence(sql("desc my_desc_datasource"), false,
      "Detailed DataSourceProvider Table Information")

    checkExistence(sql("desc my_desc_cache"), true,
                   "key", "value")
  }

  test("describe extended command") {
    checkExistence(sql("describe extended my_desc_table"), true,
      "key", "value")
    checkExistence(sql("describe extended my_desc_table"), true,
      "Detailed Table Information")

    checkExistence(sql("desc extended my_desc_view"), true,
      "key", "value")
    checkExistence(sql("describe extended my_desc_view"), true,
      "Detailed View Information")

    checkExistence(sql("desc extended my_desc_datasource"), true,
      "i", "from deserializer")
    checkExistence(sql("desc extended my_desc_datasource"), true,
      "Detailed DataSourceProvider Table Information")

    checkExistence(sql("desc extended my_desc_cache"), true,
      "key", "value")
    // TODO we currently don't support print the extended info for cached table
  }
}
