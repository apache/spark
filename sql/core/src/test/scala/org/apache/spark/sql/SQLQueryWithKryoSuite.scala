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

import org.apache.spark.internal.config
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.SERIALIZER
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class SQLQueryWithKryoSuite extends QueryTest with SharedSQLContext {

  override protected def sparkConf = super.sparkConf
    .set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    .set(KRYO_USE_UNSAFE, true)

  test("kryo unsafe data quality issue") {
    // This issue can be reproduced when
    // 1. Enable KryoSerializer
    // 2. Set spark.kryo.unsafe to true
    // 3. Use HighlyCompressedMapStatus since it uses RoaringBitmap
    // 4. Set spark.sql.shuffle.partitions to 6000, 6000 can trigger issue based the supplied data
    // 5. Comment the zero-size blocks fetch fail exception in ShuffleBlockFetcherIterator
    //    or this job will failed with FetchFailedException.
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "6000",
      config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.key -> "-1") {
      withTempView("t") {
        val df = spark.read.parquet(testFile("test-data/dates.parquet")).toDF("date")
        df.createOrReplaceTempView("t")
        checkAnswer(
          sql("SELECT COUNT(*) FROM t"),
          sql(
            """
              |SELECT SUM(a) FROM
              |(
              |SELECT COUNT(*) a, date
              |FROM t
              |GROUP BY date
              |)
            """.stripMargin))
      }
    }
  }
}
