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

import java.io.File

import org.apache.spark.sql.classic
import org.apache.spark.sql.test.SharedSparkSession

class SparkSessionGetOrCreateSuite extends QueryTest with SharedSparkSession {

  private val threeRows = Seq(Row(0L), Row(1L), Row(2L))

  test("SPARK-60775: computes and stores the dataset on the first call") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      checkAnswer(spark.getOrCreate(path, () => spark.range(3).toDF()), threeRows)
      assert(new File(dir, classic.SparkSession.GET_OR_CREATE_MARKER).exists())
      checkAnswer(spark.read.parquet(path), threeRows)
    }
  }

  test("SPARK-60775: loads existing dataset instead of recomputing") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      var calls = 0
      def create(): classic.DataFrame = {
        calls += 1
        spark.range(3).toDF()
      }

      checkAnswer(spark.getOrCreate(path, () => create()), threeRows)
      checkAnswer(spark.getOrCreate(path, () => create()), threeRows)
      assert(calls === 1)
    }
  }

  test("SPARK-60775: recomputes when the stored dataset has no marker") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(100).toDF().write.parquet(path)

      checkAnswer(spark.getOrCreate(path, () => spark.range(3).toDF()), threeRows)
    }
  }
}
