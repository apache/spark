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

package org.apache.spark.sql.execution

import java.sql.{Date, Timestamp}

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf

class TypeCoercionModeSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  private def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  private var sparkSession: SparkSession = _

  private def withTypeCoercionMode[T](typeCoercionMode: String)(f: SparkSession => T): T = {
    try {
      val sparkConf = new SparkConf(false)
        .setMaster("local")
        .setAppName(this.getClass.getName)
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")
        .set(SQLConf.typeCoercionMode.key, typeCoercionMode)

      sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      f(sparkSession)
    } finally {
      if (sparkSession != null) {
        sparkSession.sql("DROP TABLE IF EXISTS v")
        sparkSession.stop()
      }
    }
  }

  test("CommonTypeForBinaryComparison: StringType vs NumericType") {
    val str1 = Long.MaxValue.toString + "1"
    val str2 = Int.MaxValue.toString + "1"
    val str3 = "10"
    val str4 = "0"
    val str5 = "-0.4"
    val str6 = "0.6"

    val data = Seq(str1, str2, str3, str4, str5, str6)

    val q1 = "SELECT c1 FROM v WHERE c1 > 0"
    val q2 = "SELECT c1 FROM v WHERE c1 > 0L"
    val q3 = "SELECT c1 FROM v WHERE c1 = 0"
    val q4 = "SELECT c1 FROM v WHERE c1 in (0)"

    withTypeCoercionMode("hive") { spark =>
      import spark.implicits._
      data.toDF("c1").createOrReplaceTempView("v")
      checkAnswer(spark.sql(q1), Row(str1) :: Row(str2) :: Row(str3) :: Row(str6) :: Nil)
      checkAnswer(spark.sql(q2), Row(str1) :: Row(str2) :: Row(str3) :: Row(str6) :: Nil)
      checkAnswer(spark.sql(q3), Row(str4) :: Nil)
      checkAnswer(spark.sql(q4), Row(str4) :: Nil)
    }

    withTypeCoercionMode("default") { spark =>
      import spark.implicits._
      data.toDF("c1").createOrReplaceTempView("v")
      checkAnswer(spark.sql(q1), Row(str3) :: Nil)
      checkAnswer(spark.sql(q2), Row(str2) :: Row(str3) :: Nil)
      checkAnswer(spark.sql(q3), Row(str4) :: Row(str5) :: Row(str6) :: Nil)
      checkAnswer(spark.sql(q4), Row(str4) :: Nil)
    }
  }

  test("CommonTypeForBinaryComparison: StringType vs DateType") {
    val v1 = Date.valueOf("2017-09-22")
    val v2 = Date.valueOf("2017-09-09")

    val data = Seq(v1, v2)

    val q1 = "SELECT c1 FROM v WHERE c1 > '2017-8-1'"
    val q2 = "SELECT c1 FROM v WHERE c1 > '2014'"
    val q3 = "SELECT c1 FROM v WHERE c1 > cast('2017-8-1' as date)"

    withTypeCoercionMode("hive") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql(q2), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql(q3), Row(v1) :: Row(v2) :: Nil)
    }

    withTypeCoercionMode("default") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Nil)
      checkAnswer(spark.sql(q2), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql(q3), Row(v1) :: Row(v2) :: Nil)
    }
  }

  test("CommonTypeForBinaryComparison: StringType vs TimestampType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")
    val v3 = Timestamp.valueOf("2017-08-21 23:42:12")

    val data = Seq(v1, v2, v3)

    val q1 = "SELECT c1 FROM v WHERE c1 > '2017-8-1'"
    val q2 = "SELECT c1 FROM v WHERE c1 < '2017-08-21 23:42:12.0'"
    val q3 = "SELECT c1 FROM v WHERE c1 > cast('2017-8-1' as timestamp)"

    withTypeCoercionMode("hive") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Row(v2) :: Row(v3) :: Nil)
      checkAnswer(spark.sql(q2), Row(v1) :: Nil)
      checkAnswer(spark.sql(q3), Row(v2) :: Row(v3) :: Nil)
    }

    withTypeCoercionMode("default") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Nil)
      checkAnswer(spark.sql(q2), Row(v1) :: Row(v3) :: Nil)
      checkAnswer(spark.sql(q3), Row(v2) :: Row(v3) :: Nil)
    }
  }

  test("CommonTypeForBinaryComparison: TimestampType vs DateType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")

    val data = Seq(v1, v2)

    val q1 = "SELECT c1 FROM v WHERE c1 > cast('2017-8-1' as date)"
    val q2 = "SELECT c1 FROM v WHERE c1 > cast('2017-8-1' as timestamp)"

    withTypeCoercionMode("hive") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Row(v2) :: Nil)
      checkAnswer(spark.sql(q2), Row(v2) :: Nil)
    }

    withTypeCoercionMode("default") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Row(v2) :: Nil)
      checkAnswer(spark.sql(q2), Row(v2) :: Nil)
    }
  }

  test("CommonTypeForBinaryComparison: TimestampType vs NumericType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")

    val data = Seq(v1, v2)

    val q1 = "SELECT c1 FROM v WHERE c1 > 1"
    val q2 = "SELECT c1 FROM v WHERE c1 > '2017-8-1'"
    val q3 = "SELECT c1 FROM v WHERE c1 > '2017-08-01'"
    val q4 = "SELECT c1 FROM v WHERE c1 > cast(cast('2017-08-01' as timestamp) as double)"

    withTypeCoercionMode("hive") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      checkAnswer(spark.sql(q1), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql(q2), Row(v2) :: Nil)
      checkAnswer(spark.sql(q3), Row(v2) :: Nil)
      checkAnswer(spark.sql(q4), Row(v2) :: Nil)
    }

    withTypeCoercionMode("default") { spark =>
      import spark.implicits._
      data.toDF("c1").createTempView("v")
      val e1 = intercept[AnalysisException] {
        spark.sql(q1)
      }
      assert(e1.getMessage.contains("data type mismatch"))
      checkAnswer(spark.sql(q2), Nil)
      checkAnswer(spark.sql(q3), Row(v2) :: Nil)
      val e2 = intercept[AnalysisException] {
        spark.sql(q4)
      }
      assert(e2.getMessage.contains("data type mismatch"))
    }
  }
}
