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

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.scalatest.ConfigMap

class QueriesSuiteBase() extends HBaseIntegrationTestBase
    with CreateTableAndLoadData with Logging {
  self: HBaseIntegrationTestBase =>
  val tabName = DefaultTableName

  override protected def beforeAll() = {
    createTableAndLoadData()
  }

  override protected def afterAll(): Unit = {
    TestHbase.sql("Drop Table " + DefaultStagingTableName)
    TestHbase.sql("Drop Table " + DefaultTableName)
  }

  def runQuery(sql: String) = {
    logInfo(sql)
    val execQuery1 = TestHbase.sql(sql)
    execQuery1.collect()
  }

  def run(sqlCtx: SQLContext, testName: String, sql: String, exparr: Seq[Seq[Any]]) = {
    val execQuery1 = sqlCtx.executeSql(sql)
    val result1 = runQuery(sql)
    assert(result1.size == exparr.length, s"$testName failed on size")
    verify(testName,
      sql,
      for (rx <- 0 until exparr.size)
      yield result1(rx).toSeq, exparr
    )
  }

  def verify(testName: String, sql: String, result1: Seq[Seq[Any]], exparr: Seq[Seq[Any]]) = {
    val res = {
      for (rx <- 0 until exparr.size)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}

    logInfo(s"$sql came back with ${result1.size} results")
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")
  }

  val CompareTol = 1e-6

  def compareWithTol(actarr: Seq[Any], exparr: Seq[Any], emsg: String): Boolean = {
    actarr.zip(exparr).forall { case (aa, ee) =>
      val eq = (aa, ee) match {
        case (a: Double, e: Double) =>
          Math.abs(a - e) <= CompareTol
        case (a: Float, e: Float) =>
          Math.abs(a - e) <= CompareTol
        case (a: Byte, e)  => true //For now, we assume it is ok
        case (a, e) =>
          if(a == null && e == null) {
            logDebug(s"a=null e=null")
          } else {
            logDebug(s"atype=${a.getClass.getName} etype=${e.getClass.getName}")
          }
          a == e
        case _ => throw new IllegalArgumentException("Expected tuple")
      }
      if (!eq) {
        logError(s"$emsg: Mismatch- act=$aa exp=$ee")
      }
      eq
    }
  }
}

