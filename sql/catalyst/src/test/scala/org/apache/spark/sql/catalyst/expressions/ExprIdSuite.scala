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

import java.util.UUID

import org.apache.spark.SparkFunSuite

class ExprIdSuite extends SparkFunSuite {

  private val jvmId = UUID.randomUUID()
  private val otherJvmId = UUID.randomUUID()

  test("hashcode independent of jvmId") {
    val exprId1 = ExprId(12, jvmId)
    val exprId2 = ExprId(12, otherJvmId)
    assert(exprId1 != exprId2)
    assert(exprId1.hashCode() == exprId2.hashCode())
  }

  test("equality should depend on both id and jvmId") {
    val exprId1 = ExprId(1, jvmId)
    val exprId2 = ExprId(1, jvmId)
    assert(exprId1 == exprId2)

    val exprId3 = ExprId(1, jvmId)
    val exprId4 = ExprId(2, jvmId)
    assert(exprId3 != exprId4)

    val exprId5 = ExprId(1, jvmId)
    val exprId6 = ExprId(1, otherJvmId)
    assert(exprId5 != exprId6)
  }

}
