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

package org.apache.spark.sql.hbase

import org.apache.spark.sql.Row

class HBaseInsertTableSuite extends QueriesSuiteBase {

  var testnm = "Insert all rows to the table from other table"
  test("Insert all rows to the table from other table") {
    val createQuery = s"""CREATE TABLE insertTestTable(strcol STRING, bytecol BYTE, shortcol SHORT,
            intcol INTEGER, longcol LONG, floatcol FLOAT, doublecol DOUBLE, 
            PRIMARY KEY(doublecol, strcol, intcol)) 
            MAPPED BY (hinsertTestTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    runQuery(createQuery)

    val insertQuery =
      s"""insert into insertTestTable select * from $tabName"""
        .stripMargin
    runQuery(insertQuery)

    val testQuery = "select * from insertTestTable"
    val testResult = runQuery(testQuery)
    val targetResult = runQuery(s"select * from $tabName")
    assert(testResult.size == targetResult.size, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runQuery("Drop Table insertTestTable")
  }

  testnm = "Insert few rows to the table from other table after applying filter"
  test("Insert few rows to the table from other table after applying filter") {
    val createQuery = s"""CREATE TABLE insertTestTableFilter(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, longcol LONG, floatcol FLOAT, doublecol DOUBLE, 
            PRIMARY KEY(doublecol, strcol, intcol)) 
            MAPPED BY (hinsertTestTableFilter, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    runQuery(createQuery)

    val insertQuery =
      s"""insert into insertTestTableFilter select * from $tabName 
        where doublecol > 5678912.345681"""
        .stripMargin
    runQuery(insertQuery)

    val testQuery = "select * from insertTestTableFilter"
    val testResult = runQuery(testQuery)
    val targetResult = runQuery(s"select * from $tabName where doublecol > 5678912.345681")
    assert(testResult.size == targetResult.size, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runQuery("Drop Table insertTestTableFilter")
  }

  def compareResults(fetchResult: Array[Row], targetResult: Array[Row]) = {
    val res = {
      for (rx <- 0 until targetResult.size)
      yield compareWithTol(fetchResult(rx).toSeq, targetResult(rx).toSeq, s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")
  }

  testnm = "Insert few columns to the table from other table"
  test("Insert few columns to the table from other table") {
    val createQuery = s"""CREATE TABLE insertTestTableFewCols(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol, intcol)) 
            MAPPED BY (hinsertTestTableFewCols, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol])"""
      .stripMargin
    runQuery(createQuery)

    val insertQuery =
      s"""insert into insertTestTableFewCols select strcol, bytecol,
        shortcol, intcol from $tabName order by strcol"""
        .stripMargin
    runQuery(insertQuery)

    val testQuery =
      "select strcol, bytecol, shortcol, intcol from insertTestTableFewCols order by strcol"
    val testResult = runQuery(testQuery)
    val targetResult =
      runQuery(s"select strcol, bytecol, shortcol, intcol from $tabName order by strcol")
    assert(testResult.size == targetResult.size, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runQuery("Drop Table insertTestTableFewCols")
  }

  testnm = "Insert into values test"
  test("Insert into values test") {
    val createQuery = s"""CREATE TABLE insertValuesTest(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol, intcol)) 
            MAPPED BY (hinsertValuesTest, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol])"""
      .stripMargin
    runQuery(createQuery)

    val insertQuery1 = s"insert into insertValuesTest values('Row0','a',12340,23456780)"
    val insertQuery2 = s"insert into insertValuesTest values('Row1','b',12345,23456789)"
    val insertQuery3 = s"insert into insertValuesTest values('Row2','c',12342,23456782)"
    runQuery(insertQuery1)
    runQuery(insertQuery2)
    runQuery(insertQuery3)

    val testQuery = "select * from insertValuesTest order by strcol"
    val testResult = runQuery(testQuery)
    assert(testResult.size == 3, s"$testnm failed on size")

    val exparr = Array(Array("Row0", 'a', 12340, 23456780),
      Array("Row1", 'b', 12345, 23456789),
      Array("Row2", 'c', 12342, 23456782))

    val res = {
      for (rx <- 0 until 3)
      yield compareWithTol(testResult(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    runQuery("Drop Table insertValuesTest")
  }

  testnm = "Insert nullable values test"
  test("Insert nullable values test") {
    val createQuery = s"""CREATE TABLE insertNullValuesTest(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol))
            MAPPED BY (hinsertNullValuesTest, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, intcol=cf1.hintcol])"""
      .stripMargin
    runQuery(createQuery)

    val insertQuery1 = s"insert into insertNullValuesTest values('Row0', null,  12340, 23456780)"
    val insertQuery2 = s"insert into insertNullValuesTest values('Row1', 'b',   null, 23456789)"
    val insertQuery3 = s"insert into insertNullValuesTest values('Row2', 'c',  12342, null)"
    runQuery(insertQuery1)
    runQuery(insertQuery2)
    runQuery(insertQuery3)

    val selectAllQuery = "select * from insertNullValuesTest order by strcol"
    val selectAllResult = runQuery(selectAllQuery)

    assert(selectAllResult.size == 3, s"$testnm failed on size")

    var currentResultRow: Int = 0

    // check 1st result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row0", s"$testnm failed on returned Row0, key value")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned Row0, null col1 value")
    assert(selectAllResult(currentResultRow)(2) == 12340, s"$testnm failed on returned Row0, col2 value")
    assert(selectAllResult(currentResultRow)(3) == 23456780, s"$testnm failed on returned Row0, col3 value")

    currentResultRow += 1

    // check 2nd result row
    assert(selectAllResult(currentResultRow)(0) === s"Row1", s"$testnm failed on returned Row1, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectAllResult(currentResultRow)(2) == null, s"$testnm failed on returned Row1, null col2 value")
    assert(selectAllResult(currentResultRow)(3) == 23456789, s"$testnm failed on returned Row1, col3 value")

    currentResultRow += 1

    // check 3rd result row
    assert(selectAllResult(currentResultRow)(0) === s"Row2", s"$testnm failed on returned Row2, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectAllResult(currentResultRow)(2) == 12342, s"$testnm failed on returned Row2, col2 value")
    assert(selectAllResult(currentResultRow)(3) == null, s"$testnm failed on returned Row2, null col3 value")

    // test 'where col is not null'

    val selectWhereIsNotNullQuery = "select * from insertNullValuesTest where intcol is not null order by strcol"
    val selectWhereIsNotNullResult = runQuery(selectWhereIsNotNullQuery)
    assert(selectWhereIsNotNullResult.size == 2, s"$testnm failed on size")

    currentResultRow = 0
    // check 1st result row
    assert(selectWhereIsNotNullResult(currentResultRow)(0) === s"Row0", s"$testnm failed on returned Row0, key value")
    assert(selectWhereIsNotNullResult(currentResultRow)(1) == null, s"$testnm failed on returned Row0, null col1 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(2) == 12340, s"$testnm failed on returned Row0, col2 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(3) == 23456780, s"$testnm failed on returned Row0, col3 value")

    currentResultRow += 1
    // check 2nd result row
    assert(selectWhereIsNotNullResult(currentResultRow)(0) === s"Row1", s"$testnm failed on returned Row1, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectWhereIsNotNullResult(currentResultRow)(2) == null, s"$testnm failed on returned Row1, null col2 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(3) == 23456789, s"$testnm failed on returned Row1, col3 value")


    runQuery("Drop Table insertNullValuesTest")
  }


}
