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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite

class RandomBasedUuidSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("uuid length") {
    checkEvaluation(Length(RandomBasedUuid(Some(0))), 36)
  }

  test("uuid equals") {
    val seed1 = Some(5L)
    val seed2 = Some(10L)
    val uuid = RandomBasedUuid(seed1)
    assert(uuid.fastEquals(uuid))
    assert(!uuid.fastEquals(RandomBasedUuid(seed1)))
    assert(!uuid.fastEquals(RandomBasedUuid(seed2)))
    assert(!uuid.fastEquals(uuid.freshCopy()))
  }

  test("uuid evaluate") {
    val seed1 = Some(5L)
    assert(evaluateWithoutCodegen(RandomBasedUuid(seed1)) ===
      evaluateWithoutCodegen(RandomBasedUuid(seed1)))
    assert(evaluateWithGeneratedMutableProjection(RandomBasedUuid(seed1)) ===
      evaluateWithGeneratedMutableProjection(RandomBasedUuid(seed1)))
    assert(evaluateWithUnsafeProjection(RandomBasedUuid(seed1)) ===
      evaluateWithUnsafeProjection(RandomBasedUuid(seed1)))

    val seed2 = Some(10L)
    assert(evaluateWithoutCodegen(RandomBasedUuid(seed1)) !==
      evaluateWithoutCodegen(RandomBasedUuid(seed2)))
    assert(evaluateWithGeneratedMutableProjection(RandomBasedUuid(seed1)) !==
      evaluateWithGeneratedMutableProjection(RandomBasedUuid(seed2)))
    assert(evaluateWithUnsafeProjection(RandomBasedUuid(seed1)) !==
      evaluateWithUnsafeProjection(RandomBasedUuid(seed2)))
  }
}
