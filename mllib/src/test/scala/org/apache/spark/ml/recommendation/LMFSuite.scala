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

package org.apache.spark.ml.recommendation

import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class LMFSuite extends MLTest with DefaultReadWriteTest with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setCheckpointDir(tempDir.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("LMF validate input dataset") {
    import testImplicits._

    withClue("Valid Integer Ids") {
      val df = sc.parallelize(Seq(
        (123, 1),
        (111, 2)
      )).toDF("item", "user")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Long Ids") {
      val df = sc.parallelize(Seq(
        (1231L, 12L),
        (1112L, 21L)
      )).toDF("item", "user")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        new LMF().setMaxIter(1).fit(df)
      }
    }

    withClue("Valid Double Ids") {
      val df = sc.parallelize(Seq(
        (123.0, 12.0),
        (111.0, 21.0)
      )).toDF("item", "user")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Decimal Ids") {
      val df = sc.parallelize(Seq(
          (1231L, 12L),
          (1112L, 21L)
        )).toDF("item", "user")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"))
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Invalid Double: fractional part") {
      val df = sc.parallelize(Seq(
        (123.1, 12.0),
        (111.0, 21.0)
      )).toDF("item", "user")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Decimal: fractional part") {
      val df = sc.parallelize(Seq(
          (123.1, 12L),
          (1112.0, 21L)
        )).toDF("item", "user")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user")
        )
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Type") {
      val df = sc.parallelize(Seq(
        ("123.0", 12.0),
        ("111", 21.0)
      )).toDF("item", "user")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Column item must be of type numeric"))
    }

    withClue("Valid implicit with weights") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 1.0),
        (1112L, 21L, 2.0)
      )).toDF("item", "user", "weight")
      new LMF().setWeightCol("weight").setMaxIter(1).fit(df)
    }

    withClue("Invalid implicit with weights") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, -1f),
        (1112L, 21L, 2f)
      )).toDF("item", "user", "weight")
      val e = intercept[Exception] { new LMF().setWeightCol("weight").setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Weights MUST NOT be Negative or Infinity"))
    }

    withClue("Invalid implicit with labels") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 1f),
        (1112L, 21L, 0f)
      )).toDF("item", "user", "label")
      val e = intercept[Exception] { new LMF().setLabelCol("label").setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF does not support the labelCol in implicitPrefs mode."))
    }

    withClue("Valid explicit labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.0),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "label")
      new LMF().setLabelCol("label").setImplicitPrefs(false).setMaxIter(1).fit(df)
    }

    withClue("Invalid explicit labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.5),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "label")
      val e = intercept[Exception] { new LMF().setLabelCol("label")
        .setImplicitPrefs(false).setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Labels MUST be in {0, 1}"))
    }

    withClue("Invalid explicit without labels") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.5),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "label")
      val e = intercept[Exception] { new LMF().setLabelCol("label")
        .setImplicitPrefs(false).setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("The labelCol must be set in explicit mode."))
    }
  }
  
}