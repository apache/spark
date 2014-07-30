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

import org.apache.spark.sql.test.TestSQLContext

object TestCsvData {

  val diamondCSVWithHeader = TestSQLContext.sparkContext.parallelize(Array(
    "carat,cut,color,clarity,depth,table,price,x,y,z",
    "0.23,Ideal,E,SI2,61.5,55,326,3.95,3.98,2.43",
    "0.21,Premium,E,SI1,59.8,61,326,3.89,3.84,2.31",
    "0.23,Good,E,VS1,56.9,65,327,4.05,4.07,2.31"), 1)

  val salesCSVWithoutHeader = TestSQLContext.sparkContext.parallelize(Array(
    "2003; USA; MacBook; 11580.401776",
    "2003; USA; Power Adapter; 4697.495022",
    "2003; FRA; MacBook;5910; 787393",
    "2003; FRA; Power Adapter; 2758.903949",
    "2003; RUS; MacBook; 6992; 729325"
  ))

  val carCSVWithQuotes = TestSQLContext.sparkContext.parallelize(Array(
  """Year, Make, Model, Desc""",
  """"1964", Ford, "Ford""",
"""Pampa", manufactured by Ford of Brazil""",
  """"1947", "Ford", "Ford Pilot", "The Pilot was the first large""",
"""post-war British Ford"""",
  """1997, Ford, Mustang, """
  ), 1)

  val salesCSVWithDoubleQuotes = TestSQLContext.sparkContext.parallelize(Array(
    """|2003|; USA; |Mac "Power" Adapter|; 4697.495022""",
    """|2003|; FRA; |iPhone "Power" Adapter|; 2758.903949""",
    """|2003|; FRA; |iPad ||Power|| Adapter|; 2758.903949"""
  ), 1)

}

