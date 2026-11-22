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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Cast, EvalMode, IsNull, Literal, RaiseError}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.IntegerType

class ExtractEquiJoinKeysSuite extends SparkFunSuite {

  test("SPARK-62241: an equality that can fail after a throwable conjunct is not a join key") {
    val left = LocalRelation($"a".int, $"s".string)
    val right = LocalRelation($"z".int)
    val Seq(a, s) = left.output
    val z = right.output.head
    // The cast is built in ANSI mode, where it can fail. A join computes its keys on every row,
    // so after the throwable conjunct the equality on the cast is not a key, and stays after the
    // throwable conjunct in the other condition. `a = z`, which can not fail, is a key.
    val throwing = IsNull(RaiseError(Literal("boom")))
    val cast = Cast(s, IntegerType, evalMode = EvalMode.ANSI)
    left.join(right, Inner, Some(throwing && cast === z && a === z)) match {
      case ExtractEquiJoinKeys(_, leftKeys, rightKeys, otherCondition, keyCondition, _, _, _) =>
        assert(leftKeys === Seq(a))
        assert(rightKeys === Seq(z))
        assert(otherCondition === Some(throwing && cast === z))
        assert(keyCondition === Some(a === z))
      case other => fail(s"Expected an equi-join, got $other")
    }
  }
}
