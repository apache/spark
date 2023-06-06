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

import org.apache.spark.sql.connect.client.util.QueryTest
import org.apache.spark.sql.functions.expr

/**
 * All tests in this class requires client UDF artifacts synced with the server.
 */
class MyE2ETestSuite extends QueryTest with SQLHelper {

  lazy val session: SparkSession = spark
  import session.implicits._

  test("groupby") { // TODO: how to fix this then?
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"key").as[String, (String, Int, Int)]
    val aggregated = grouped
      .flatMapSortedGroups($"seq", expr("length(key)"), $"value") { (g, iter) =>
        Iterator(g, iter.mkString(", "))
      }

    checkDatasetUnorderly(
      aggregated,
      "a",
      "(a,1,10), (a,2,20)",
      "b",
      "(b,1,2), (b,2,1)",
      "c",
      "(c,1,1)"
    )
  }
}
