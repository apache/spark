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
        (123, 1, 0.0),
        (111, 2, 1.0)
      )).toDF("item", "user", "rating")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Long Ids") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 0.0),
        (1112L, 21L, 1.0)
      )).toDF("item", "user", "rating")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        new LMF().setMaxIter(1).fit(df)
      }
    }

    withClue("Valid Double Ids") {
      val df = sc.parallelize(Seq(
        (123.0, 12.0, 0.0),
        (111.0, 21.0, 1.0)
      )).toDF("item", "user", "rating")
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Valid Decimal Ids") {
      val df = sc.parallelize(Seq(
          (1231L, 12L, 0.0),
          (1112L, 21L, 1.0)
        )).toDF("item", "user", "rating")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"),
          col("rating")
        )
      new LMF().setMaxIter(1).fit(df)
    }

    withClue("Invalid Double: fractional part") {
      val df = sc.parallelize(Seq(
        (123.1, 12.0, 0.0),
        (111.0, 21.0, 1.0)
      )).toDF("item", "user", "rating")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Decimal: fractional part") {
      val df = sc.parallelize(Seq(
          (123.1, 12L, 0.0),
          (1112.0, 21L, 1.0)
        )).toDF("item", "user", "rating")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"),
          col("rating")
        )
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("LMF only supports non-Null values"))
    }

    withClue("Invalid Type") {
      val df = sc.parallelize(Seq(
        ("123.0", 12.0, 0.0),
        ("111", 21.0, 1.0)
      )).toDF("item", "user", "rating")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Column item must be of type numeric"))
    }

    withClue("Invalid rating") {
      val df = sc.parallelize(Seq(
        (123L, 321L, 0.5),
        (1234L, 4321L, 1.0)
      )).toDF("item", "user", "rating")
      val e = intercept[Exception] { new LMF().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Labels MUST be in {0, 1}"))
    }
  }
  
}