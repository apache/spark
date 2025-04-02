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
package org.apache.spark.sql.hive.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveSourceRowLevelOperationSuite extends QueryTest with TestHiveSingleton
  with BeforeAndAfter with SQLTestUtils {

  before {
    spark.conf.set("spark.sql.catalog.cat", classOf[InMemoryRowLevelOperationTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat")
  }

  test("SPARK-45943: merge into using hive table without stats") {
    val inMemCatNs = "cat.ns1"
    val inMemCatTable = "in_mem_cat_table"
    withTable("hive_table", s"$inMemCatNs.$inMemCatTable") {
      // create hive table without stats
      sql("create table hive_table(pk int, salary int, dep string)")

      sql(
        s"""
           |create table $inMemCatNs.$inMemCatTable (
           |  pk INT NOT NULL,
           |  salary INT,
           |  dep STRING)
           |PARTITIONED BY (dep)
           | """.stripMargin)

      try {
        // three-part naming is not supported in
        // org.apache.spark.sql.hive.test.TestHiveQueryExecution.analyzed.{referencedTables}
        sql(s"use $inMemCatNs")
        sql(
          s"""MERGE INTO $inMemCatTable t
             |USING (SELECT pk, salary, dep FROM spark_catalog.default.hive_table) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = s.salary
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)
      } finally {
        sql("use spark_catalog.default")
      }
    }
  }
}
