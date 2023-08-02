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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.sql.types.ByteType

class VectorReservePolicySuite extends SparkFunSuite with SharedSparkSessionBase {

  test("Test column vector reserve policy") {
    withSQLConf(
      SQLConf.VECTORIZED_HUGE_VECTOR_THRESHOLD.key -> "300",
      SQLConf.VECTORIZED_HUGE_VECTOR_RESERVE_RATIO.key -> "1.2") {
      val dataType = ByteType

      Array(new OnHeapColumnVector(80, dataType),
        new OffHeapColumnVector(80, dataType)).foreach { vector =>
        try {
          // The new capacity of small vector = request capacity * 2 and will not be reset
          vector.appendBytes(100, 0)
          assert(vector.getCapacity == 200)
          vector.reset()
          assert(vector.getCapacity == 200)

          // The new capacity of huge vector = (request capacity - HUGE_VECTOR_THRESHOLD) * 1.2 +
          // HUGE_VECTOR_THRESHOLD * 2 = (300 - 300) * 1.2 + 300 * 2 and will be reset.
          vector.appendBytes(300, 0)
          assert(vector.getCapacity == 600)
          vector.reset()
          assert(vector.getCapacity == 80)
        } finally {
          vector.close()
        }
      }
    }
  }
}
