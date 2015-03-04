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

class BasicQueriesSuite extends QueriesSuiteBase {

  var testnm = "StarOperator * with limit"
  test("StarOperator * with limit") {
    val query1 =
      s"""select * from $tabName limit 3"""
        .stripMargin

    val result1 = runQuery(query1)
    assert(result1.size == 3, s"$testnm failed on size")
    val exparr = Array(Array("Row1", 'a', 12345, 23456789, 3456789012345L, 45657.89F, 5678912.345678),
      Array("Row2", 'b', 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682),
      Array("Row3", 'c', 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683))

    var res = {
      for (rx <- 0 until 3)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    logInfo(s"$query1 came back with ${result1.size} results")
    logInfo(result1.mkString)

    val sql2 =
      s"""select * from $tabName limit 2"""
        .stripMargin

    val results = runQuery(sql2)
    logInfo(s"$sql2 came back with ${results.size} results")
    assert(results.size == 2, s"$testnm failed assertion on size")
    res = {
      for (rx <- 0 until 2)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    logInfo(results.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select all cols with filter"
  test("Select all cols with filter") {
    val query1 =
      s"""select * from $tabName where shortcol < 12345 limit 2"""
        .stripMargin

    val result1 = runQuery(query1)
    logInfo(s"$query1 came back with ${result1.size} results")
    assert(result1.size == 2, s"$testnm failed on size")
    val exparr = Array(
      Array("Row2", 'b', 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682),
      Array("Row3", 'c', 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683))

    val res = {
      for (rx <- 0 until 2)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select all cols with order by"
  test("Select all cols with order by") {
    val query1 =
      s"""select * from $tabName where shortcol < 12344 order by strcol desc limit 2"""
        .stripMargin

    val result1 = runQuery(query1)
    assert(result1.size == 2, s"$testnm failed on size")
    val exparr = Array(
      Array("Row3", 'c', 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683),
      Array("Row2", 'b', 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682))

    val res = {
      for (rx <- 0 until 2)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select same column twice"
  test("Select same column twice") {
    val query1 =
      s"""select doublecol as double1, doublecol as doublecol
             | from $tabName
     | where doublecol > 5678912.345681 and doublecol < 5678912.345683"""
        .stripMargin

    val result1 = runQuery(query1)
    logInfo(s"$query1 came back with ${result1.size} results")
    assert(result1.size == 1, s"$testnm failed on size")
    val exparr = Array(
      Array(5678912.345682, 5678912.345682))

    assert(result1.size == 1, s"$testnm failed assertion on size")
    val res = {
      for (rx <- 0 until 1)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select specific cols with filter"
  test("Select specific cols with filter") {
    val query1 =
      s"""select doublecol as double1, -1 * doublecol as minusdouble,
         | substr(strcol, 2) as substrcol, doublecol, strcol,
         | bytecol, shortcol, intcol, longcol, floatcol from $tabName where strcol like
         |  '%Row%' and shortcol < 12345
         |  and doublecol > 5678912.345681 and doublecol < 5678912.345683 limit 2"""
        .stripMargin

    val result1 = runQuery(query1)
    logInfo(s"$query1 came back with ${result1.size} results")
    assert(result1.size == 1, s"$testnm failed on size")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", 'b', 12342, 23456782, 3456789012342L, 45657.82F))

    assert(result1.size == 1, s"$testnm failed assertion on size")
    val res = {
      for (rx <- 0 until 1)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Mixed And/or predicates"
  test("Mixed And/or predicates") {
    val query1 = s"""select doublecol as double1, -1 * doublecol as minusdouble,
     substr(strcol, 2) as substrcol, doublecol, strcol,
     bytecol, shortcol, intcol, longcol, floatcol from $tabName
     where strcol like '%Row%'
     and shortcol < 12345
      and doublecol > 5678912.345681 and doublecol < 5678912.345683
       OR (doublecol = 5678912.345683 and strcol is not null)
       OR (doublecol = 5678912.345683 and strcol is not null or intcol > 12345 AND intcol < 0)
       OR (doublecol <> 5678912.345683 and (strcol is null or intcol > 12345 AND intcol < 0))
       AND floatcol is not null
       AND (intcol is not null and intcol > 0)
       AND (intcol < 0 OR intcol is not null)""".stripMargin

    val result1 = runQuery(query1)
    logInfo(s"$query1 came back with ${result1.size} results")
    assert(result1.size == 2, s"$testnm failed on size")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", 'b', 12342, 23456782, 3456789012342L, 45657.82F),
      Array(5678912.345683, -5678912.345683, "ow3", 5678912.345683,
        "Row3", -29, 12343, 23456783, 3456789012343L, 45657.83))

    val res = {
      for (rx <- 0 until 1)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }
}
