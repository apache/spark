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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class TableResolutionSuite extends QueryTest with SharedSparkSession with BeforeAndAfter{

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  test("V2 commands should look up temp view first") {
    val tbl = "t"
    val commands = Seq(
      s"DESCRIBE $tbl",
      s"SHOW TBLPROPERTIES $tbl",
      s"ALTER TABLE $tbl ADD COLUMN data string"
    )

    withTempView(s"$tbl") {
      withTable(s"testcat.ns.$tbl") {
        sql(s"CREATE TEMPORARY VIEW $tbl AS SELECT 1 AS i")
        sql(s"CREATE TABLE testcat.ns.$tbl USING csv AS SELECT 2 AS i")
        sql("USE testcat.ns")

        commands.foreach { command =>
          val ex = intercept[AnalysisException] {
            sql(command)
          }
          assert(ex.getMessage.contains("Invalid command: 't' is a view not a table."))
        }
      }
    }
  }
}
