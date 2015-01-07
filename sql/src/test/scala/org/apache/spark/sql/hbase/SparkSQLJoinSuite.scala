package org.apache.spark.sql.hbase

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

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

/**
 * SparkSQLJoinSuite
 *
 */
class SparkSQLJoinSuite extends FunSuite {

  test("Basic Join on vanilla SparkSql: Simple Two Way  2 cols") {
    val testnm = "Basic Join on vanilla SparkSql: Simple Two Way 2 cols"
    import org.apache.spark.sql._
    //    val hbclocal = hbc.asInstanceOf[SQLContext]
    //    import hbclocal._
    val sc = new SparkContext("local[1]","BasicJoinTest")
    val ssc = new SQLContext(sc)
    import ssc._
    val rdd1 = sc.parallelize((1 to 2).map { ix => JoinTable2Cols(ix, s"valA$ix")})
    rdd1.registerTempTable("SparkJoinTable1")
    println("Table1 Contents:")
    val q1 = ssc.sql("select * from SparkJoinTable1").collect().foreach(println)
    val ids = Seq((1, 1), (1, 2), (2, 3), (2, 4))
    val rdd2 = sc.parallelize(ids.map { case (ix, is) => JoinTable2Cols(ix, s"valB$is")})
    val table2 = rdd2.registerTempTable("SparkJoinTable2")
    println("Table2 Contents:")
    val q2 = ssc.sql("select * from SparkJoinTable2").collect().foreach(println)
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol, t1.strcol t1strcol,
                t2.strcol t2strcol from SparkJoinTable1 t1 JOIN
                    SparkJoinTable2 t2 on t1.intcol = t2.intcol""".stripMargin

    println(query)
    val res = ssc.sql(query).sortBy(r =>
      s"${r.getInt(0)} ${r.getInt(1)} ${r.getString(2)} ${r.getString(3)}")
    //    res.collect.foreach(println)
    val exparr = Seq[Seq[Any]](
      Seq(1, 1, "valA1", "valB1"),
      Seq(1, 1, "valA1", "valB2"),
      Seq(2, 2, "valA2", "valB3"),
      Seq(2, 2, "valA2", "valB4"))
    run(ssc, testnm, query, exparr)
  }

  def run(sqlCtx: SQLContext, testName: String, sql: String, exparr: Seq[Seq[Any]]) = {
    val result1 = sqlCtx.sql(sql).collect()
    assert(result1.size == exparr.length, s"$testName failed on size")
    verify(testName,
      sql,
      for (rx <- 0 until exparr.size)
        yield result1(rx).toSeq, exparr
    )
  }

  def verify(testName: String, sql: String, result1: Seq[Seq[Any]], exparr: Seq[Seq[Any]]) = {
    println(s"$sql came back with ${result1.size} results")
    println(result1.mkString("Results\n","\n",""))
    val res = {
      for (rx <- 0 until exparr.size)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}

    assert(res, "One or more rows did not match expected")

    println(s"Test $testName completed successfully")
  }

  val CompareTol = 1e-6

  def compareWithTol(actarr: Seq[Any], exparr: Seq[Any], emsg: String): Boolean = {
    actarr.zip(exparr).forall { case (a, e) =>
      val eq = (a, e) match {
        case (a: Double, e: Double) =>
          Math.abs(a - e) <= CompareTol
        case (a: Float, e: Float) =>
          Math.abs(a - e) <= CompareTol
        case (a, e) =>
          a == e
        case _ => throw new IllegalArgumentException("Expected tuple")
      }
      if (!eq) {
        System.err.println(s"ERROR: $emsg: Mismatch- act=$a exp=$e")
      }
      eq
    }
  }
}
