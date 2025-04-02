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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}

class UnsafeArrayWriterSuite extends SparkFunSuite {
  test("SPARK-40403: don't print negative number when array is too big") {
    val numElements = 268271216
    val elementSize = 8
    val rowWriter = new UnsafeRowWriter(1)
    rowWriter.resetRowWriter()
    val arrayWriter = new UnsafeArrayWriter(rowWriter, elementSize)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        arrayWriter.initialize(numElements)
      },
      condition = "COLLECTION_SIZE_LIMIT_EXCEEDED.INITIALIZE",
      parameters = Map(
        "numberOfElements" -> (numElements * elementSize).toString,
        "maxRoundedArrayLength" -> Int.MaxValue.toString
      )
    )
  }
}
