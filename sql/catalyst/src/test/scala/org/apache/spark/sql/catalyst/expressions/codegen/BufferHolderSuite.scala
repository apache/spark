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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class BufferHolderSuite extends SparkFunSuite {

  test("SPARK-16071 Check the size limit to avoid integer overflow") {
    assert(intercept[UnsupportedOperationException] {
      new BufferHolder(new UnsafeRow(Int.MaxValue / 8))
    }.getMessage.contains("too many fields"))

    val holder = new BufferHolder(new UnsafeRow(1000))
    holder.reset()
    holder.grow(1000)
    assert(intercept[IllegalArgumentException] {
      holder.grow(Integer.MAX_VALUE)
    }.getMessage.contains("exceeds size limitation"))
  }
}
