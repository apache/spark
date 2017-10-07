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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.internal.SQLConf

class TypeCoercionSuite extends QueryTest {

  val spark: SparkSession = null

  val hiveSparkConf = new SparkConf()
    .setMaster("local")
    .set(SQLConf.typeCoercionMode.key, "hive")

  val legacySparkConf = new SparkConf()
    .setMaster("local")
    .set(SQLConf.typeCoercionMode.key, "legacy")

  test("SPARK-21646: CommonTypeForBinaryComparison: StringType vs NumericType") {
    val str1 = Long.MaxValue.toString + "1"
    val str2 = Int.MaxValue.toString + "1"
    val str3 = "10"
    val str4 = "0"
    val str5 = "-0.4"
    val str6 = "0.6"

    withSparkSession(SparkSession.builder.config(hiveSparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(str1, str2, str3, str4, str5, str6).toDF("c1").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT c1 from v where c1 > 0"),
        Row(str1) :: Row(str2) :: Row(str3) :: Row(str6) :: Nil)
      checkAnswer(spark.sql("SELECT c1 from v where c1 > 0L"),
        Row(str1) :: Row(str2) :: Row(str3) :: Row(str6) :: Nil)
      checkAnswer(spark.sql("SELECT c1 FROM v WHERE c1 = 0"),
        Seq(Row("0")))
    }

    withSparkSession(SparkSession.builder.config(legacySparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(str1, str2, str3, str4, str5, str6).toDF("c1").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT c1 from v where c1 > 0"),
        Row(str3) :: Nil)
      checkAnswer(spark.sql("SELECT c1 from v where c1 > 0L"),
        Row(str2) :: Row(str3) :: Nil)
      checkAnswer(spark.sql("SELECT c1 FROM v WHERE c1 = 0"),
        Seq(Row("0"), Row("-0.4"), Row("0.6")))
    }
  }

  test("SPARK-21646: CommonTypeForBinaryComparison: StringType vs DateType") {
    val v1 = Date.valueOf("2017-09-22")
    val v2 = Date.valueOf("2017-09-09")

    withSparkSession(SparkSession.builder.config(hiveSparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2014'"), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as date)"),
        Row(v1) :: Row(v2) :: Nil)
    }

    withSparkSession(SparkSession.builder.config(legacySparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2014'"), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as date)"),
        Row(v1) :: Row(v2) :: Nil)
    }
  }

  test("SPARK-21646: CommonTypeForBinaryComparison: StringType vs TimestampType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")
    val v3 = Timestamp.valueOf("2017-08-21 23:42:12.0")

    withSparkSession(SparkSession.builder.config(hiveSparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2, v3).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Row(v2) :: Row(v3) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-08-21 23:42:12'"),
        Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as timestamp)"),
        Row(v2) :: Row(v3) :: Nil)
    }

    withSparkSession(SparkSession.builder.config(legacySparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2, v3).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Nil)
      checkAnswer(spark.sql("select c1 from v where c1 >= '2017-08-21 23:42:12'"),
        Row(v2) :: Row(v3) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as timestamp)"),
        Row(v2) :: Row(v3) :: Nil)
    }
  }

  test("SPARK-21646: CommonTypeForBinaryComparison: TimestampType vs DateType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")

    withSparkSession(SparkSession.builder.config(hiveSparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as date)"), Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as timestamp)"),
        Row(v2) :: Nil)
    }

    withSparkSession(SparkSession.builder.config(legacySparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as date)"), Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > cast('2017-8-1' as timestamp)"),
        Row(v2) :: Nil)
    }
  }

  test("SPARK-21646: CommonTypeForBinaryComparison: TimestampType vs NumericType") {
    val v1 = Timestamp.valueOf("2017-07-21 23:42:12.123")
    val v2 = Timestamp.valueOf("2017-08-21 23:42:12.123")

    withSparkSession(SparkSession.builder.config(hiveSparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      checkAnswer(spark.sql("select c1 from v where c1 > 1"), Row(v1) :: Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Row(v2) :: Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-08-01'"), Row(v2) :: Nil)
      checkAnswer(
        spark.sql("select * from v where c1 > cast(cast('2017-08-01' as timestamp) as double)"),
        Row(v2) :: Nil)
    }

    withSparkSession(SparkSession.builder.config(legacySparkConf).getOrCreate()) { spark =>
      import spark.implicits._
      Seq(v1, v2).toDF("c1").createTempView("v")
      val e1 = intercept[AnalysisException] {
        spark.sql("select * from v where c1 > 1")
      }
      assert(e1.getMessage.contains("data type mismatch"))
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-8-1'"), Nil)
      checkAnswer(spark.sql("select c1 from v where c1 > '2017-08-01'"), Row(v2) :: Nil)
      val e2 = intercept[AnalysisException] {
        spark.sql("select * from v where c1 > cast(cast('2017-08-01' as timestamp) as double)")
      }
      assert(e2.getMessage.contains("data type mismatch"))
    }
  }

}
