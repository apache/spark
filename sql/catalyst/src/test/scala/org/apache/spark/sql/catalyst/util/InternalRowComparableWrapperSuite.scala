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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.types.{IntegerType, LongType}

class InternalRowComparableWrapperSuite extends SparkFunSuite {

  test("SPARK-59249: the grouped key layout and wrapper equality hold one ordering instance") {
    // Identity is the property to assert, because behaviour is not what changes here: both sides
    // were already built by the same function, so they already compared the same way. What one
    // instance buys is that neither side can later be given a definition the other does not have.
    // `InternalRowComparableWrapper.equals` compares its rows with the instance below, and
    // `KeyedPartitioning` sorts and groups partition keys with it. The two type lists are built
    // separately, so this pins the shared cache as well.
    val wrapper = InternalRowComparableWrapper
      .getInternalRowComparableWrapperFactory(Seq(IntegerType, LongType))(InternalRow(1, 2L))

    assert(KeyedPartitioning.groupedKeyRowOrdering(Seq(IntegerType, LongType)) eq wrapper.ordering)
  }
}
