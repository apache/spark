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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class AttributeReferenceSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  val confName: String = SQLConf.ATTR_COMPARE_NEW.key

  val query: String = "select * from (select a.id as newid, a.name from a join b" +
    " where a.id = b.id) temp  join c  on temp.newid = c.id"

  val tableNames = Seq("a", "b", "c")

  // Create 3 tables with schema id INT, name STRING
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    tableNames.foreach {
      name =>
        sql(
          s"""
             |CREATE TABLE $name (id INT, name STRING)
             |STORED AS PARQUET
          """.stripMargin)
    }
  }


  test(s"Tests to check for presence of exchange when falg is enable/disabled") {
    val confs = Array("true" -> 3, "false" -> 4)

    confs.foreach { conf =>
      withSQLConf(confName -> conf._1) {
        val df = spark.sql(query)
        val executedPlan = df.queryExecution.executedPlan
        val exchanges = executedPlan.collect {
          case e: Exchange => e
        }
        assert(exchanges.size == conf._2)
      }
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    tableNames.foreach {
      name =>
        spark.sql(s"drop table if exists $name")
    }
  }
}
