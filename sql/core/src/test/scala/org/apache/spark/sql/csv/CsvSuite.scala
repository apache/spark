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

package org.apache.spark.sql.csv

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.TestSQLContext._

class CsvSuite extends QueryTest {
  import TestCsvData._

  test("Simple CSV with header") {
    val csvSchemaRDD = csvRDD(diamondCSVWithHeader, header = true)
    csvSchemaRDD.registerAsTable("diamonds")

    checkAnswer(
      sql("select cut from diamonds limit 3"),
      Seq(Seq("Ideal"), Seq("Premium"), Seq("Good")))

    checkAnswer(
      sql("select count(*) from diamonds"),
      3
    )
  }

  test("Simple CSV without header") {
    val csvSchemaRDD = csvRDD(salesCSVWithoutHeader, delimiter = "; ")
    csvSchemaRDD.registerAsTable("sales")

    checkAnswer(
      sql("select distinct V0 from sales"),
      "2003"
    )
  }

  test("Quoted CSV with new lines") {
    val csvSchemaRDD = csvRDD(carCSVWithQuotes, delimiter = ", ", header = true)
    csvSchemaRDD.registerAsTable("cars")

    checkAnswer(
      sql("select Model from cars limit 1"),
      """Ford
        |Pampa""".stripMargin
    )

    checkAnswer(
      sql("select distinct Make from cars"),
      "Ford"
    )
  }

  test("Custom quoted CSV with inner quotes") {
    val csvSchemaRDD = csvRDD(salesCSVWithDoubleQuotes, delimiter = "; ", quote = '|')
    csvSchemaRDD.registerAsTable("quotedSales")

    checkAnswer(
      sql("select distinct V0 from quotedSales"),
      "2003"
    )

    checkAnswer(
      sql("select distinct V2 from quotedSales where V2 like '%Mac%'"),
      """Mac "Power" Adapter"""
    )

    checkAnswer(
      sql("select distinct V2 from quotedSales where V2 like '%iPad%'"),
      """iPad |Power| Adapter"""
    )
   }
}

