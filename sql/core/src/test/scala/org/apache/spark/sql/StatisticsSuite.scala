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

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class StatisticsSuite extends QueryTest with SharedSQLContext {

  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

}
