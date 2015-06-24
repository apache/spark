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

package org.apache.spark.sql

import java.sql.Date

import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll

class DatetimeExpressionsSuite extends QueryTest with BeforeAndAfterAll {
  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  val df = Seq((1, Date.valueOf("2015-06-01")), (3, Date.valueOf("2015-06-02"))).toDF("num", "day")

  override def beforeAll() {
    df.registerTempTable("dttable")
  }

  override def afterAll() {
    ctx.dropTempTable("dttable")
  }

  test("function date_add") {
    checkAnswer(
      df.select(date_add("day", "num")),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(column("day"), lit(null))).limit(1), Row(null))

    checkAnswer(ctx.sql("""SELECT DATE_ADD("2015-06-12", -1)"""),
      Row(Date.valueOf("2015-06-11")))
    checkAnswer(ctx.sql("SELECT DATE_ADD(null, 1)"), Row(null))
    checkAnswer(
      ctx.sql("""SELECT DATE_ADD(day, 11) FROM dttable LIMIT 1"""),
      Row(Date.valueOf("2015-06-12")))
  }

  test("function date_sub") {
    checkAnswer(
      df.select(date_sub("day", "num")),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
    checkAnswer(
      df.select(date_sub(lit(null), column("num"))).limit(1), Row(null))

    checkAnswer(ctx.sql("""SELECT DATE_SUB("2015-06-12 14:00:00", 31)"""),
      Row(Date.valueOf("2015-05-12")))
    checkAnswer(ctx.sql("""SELECT DATE_SUB("2015-06-12", null)"""), Row(null))
    checkAnswer(
      ctx.sql("""SELECT DATE_SUB(day, num) FROM dttable LIMIT 1"""),
      Row(Date.valueOf("2015-05-31")))
  }

}
