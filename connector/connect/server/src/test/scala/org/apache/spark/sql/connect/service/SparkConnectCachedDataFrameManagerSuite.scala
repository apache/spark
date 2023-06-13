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

package org.apache.spark.sql.connect.service

import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectCachedDataFrameManagerSuite extends SharedSparkSession {

  test("Successful put and get") {
    val spark = this.spark
    import spark.implicits._

    val cachedDataFrameManager = new SparkConnectCachedDataFrameManager()

    val key1 = "key_1"
    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    cachedDataFrameManager.put(spark, key1, df1)

    val expectedDf1 = cachedDataFrameManager.get(spark, key1)
    assert(expectedDf1 == df1)

    val key2 = "key_2"
    val data2 = Seq(("k4", "v4"), ("k5", "v5"))
    val df2 = data2.toDF()
    cachedDataFrameManager.put(spark, key2, df2)

    val expectedDf2 = cachedDataFrameManager.get(spark, key2)
    assert(expectedDf2 == df2)
  }

  test("Get cache that does not exist should fail") {
    val spark = this.spark
    import spark.implicits._

    val cachedDataFrameManager = new SparkConnectCachedDataFrameManager()

    val key1 = "key_1"

    assertThrows[InvalidPlanInput] {
      cachedDataFrameManager.get(spark, key1)
    }

    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    cachedDataFrameManager.put(spark, key1, df1)
    cachedDataFrameManager.get(spark, key1)

    val key2 = "key_2"
    assertThrows[InvalidPlanInput] {
      cachedDataFrameManager.get(spark, key2)
    }
  }

  test("Remove cache and then get should fail") {
    val spark = this.spark
    import spark.implicits._

    val cachedDataFrameManager = new SparkConnectCachedDataFrameManager()

    val key1 = "key_1"
    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    cachedDataFrameManager.put(spark, key1, df1)
    cachedDataFrameManager.get(spark, key1)

    cachedDataFrameManager.remove(spark)
    assertThrows[InvalidPlanInput] {
      cachedDataFrameManager.get(spark, key1)
    }
  }
}
