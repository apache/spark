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

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.VariantVal

class VariantSuite extends QueryTest with SharedSparkSession {
  test("basic tests") {
    def verifyResult(df: DataFrame): Unit = {
      val result = df.collect()
        .map(_.get(0).asInstanceOf[VariantVal].toString)
        .sorted
        .toSeq
      assert(result == (1 until 10).map(id => "1" * id))
    }

    // At this point, JSON parsing logic is not really implemented. We just construct some number
    // inputs that are also valid JSON. This exercises passing VariantVal throughout the system.
    val query = spark.sql("select parse_json(repeat('1', id)) as v from range(1, 10)")
    verifyResult(query)

    // Write into and read from Parquet.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      query.write.parquet(tempDir)
      verifyResult(spark.read.parquet(tempDir))
    }
  }
}
