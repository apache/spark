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

package org.apache.spark.sql.internal.connector

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpressionInfo, Literal}

class V1FunctionSuite extends SparkFunSuite {
  test("metadata and invocation factories are independent and evaluated once") {
    var infoLoads = 0
    var builderLoads = 0
    val info = new ExpressionInfo("test.Class", "f")
    val function = V1Function.lazyInfo(
      () => {
        infoLoads += 1
        info
      },
      () => {
        builderLoads += 1
        arguments => arguments.head
      })

    assert(infoLoads == 0 && builderLoads == 0)
    assert(function.invoke(Seq(Literal(1))) == Literal(1))
    assert(function.invoke(Seq(Literal(2))) == Literal(2))
    assert(infoLoads == 0 && builderLoads == 1)
    assert(function.info eq info)
    assert(function.name() == "f")
    assert(function.description() == "N/A.")
    assert(infoLoads == 1 && builderLoads == 1)

    val metadataFirst = V1Function.lazyInfo(() => info, () => {
      builderLoads += 1
      arguments => arguments.head
    })
    assert(metadataFirst.info eq info)
    assert(builderLoads == 1)
  }

  test("eager overloads and metadata-only wrappers preserve their supplied info") {
    val info = new ExpressionInfo("test.Class", "f")
    val builder: FunctionBuilder = arguments => arguments.head
    assert(V1Function(info, builder).info eq info)
    assert(V1Function(info, () => builder).info eq info)
    val metadataOnly = V1Function.metadataOnly(info)
    assert(metadataOnly.info eq info)
    intercept[SparkException](metadataOnly.invoke(Seq(Literal(1))))
  }
}
