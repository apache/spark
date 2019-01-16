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

package org.apache.spark.sql.execution.datasources.noop

import org.apache.spark.sql.test.SharedSQLContext

class NoopSuite extends SharedSQLContext {
  import testImplicits._

  test("materialisation of all rows") {
    val numElems = 10
    val accum = spark.sparkContext.longAccumulator
    spark.range(numElems)
      .map { x =>
        accum.add(1)
        x
      }
      .write
      .format("noop")
      .save()
    assert(accum.value == numElems)
  }

  test("read partitioned data") {
    val numElems = 100
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(numElems)
        .select('id mod 10 as "key", 'id as "value")
        .write
        .partitionBy("key")
        .parquet(path)

      val accum = spark.sparkContext.longAccumulator
      spark.read
        .parquet(path)
        .as[(Long, Long)]
        .map { x =>
          accum.add(1)
          x
        }
        .write.format("noop").save()
      assert(accum.value == numElems)
    }
  }
}

