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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.sql.types.{TimestampNTZType, TimestampType}

class GeneratedColumnExpressionSuite extends SparkFunSuite {

  private def genExpr(childType: DataType): GeneratedColumnExpression =
    GeneratedColumnExpression(Literal.create(null, childType), "<gen expr>")

  test("SPARK-57303: validate accepts a lossless widening to a nanosecond timestamp column") {
    // The generation expression's type is up-castable to the column type, so validate() succeeds:
    // micros -> nanos is a lossless widening up-cast (Cast.canUpCast).
    (TimestampNTZNanosType.MIN_PRECISION to TimestampNTZNanosType.MAX_PRECISION).foreach { p =>
      genExpr(TimestampNTZType).validate("c", TimestampNTZNanosType(p), allColumns = Seq.empty)
      genExpr(TimestampType).validate("c", TimestampLTZNanosType(p), allColumns = Seq.empty)
    }
  }

  test("SPARK-57303: validate rejects a lossy narrowing from a nanosecond timestamp column") {
    // nanos -> micros drops sub-microsecond digits and is not an up-cast, so validate() rejects it.
    (TimestampNTZNanosType.MIN_PRECISION to TimestampNTZNanosType.MAX_PRECISION).foreach { p =>
      val ex = intercept[AnalysisException] {
        genExpr(TimestampNTZNanosType(p)).validate("c", TimestampNTZType, allColumns = Seq.empty)
      }
      assert(ex.getCondition == "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN")
      assert(ex.getMessageParameters.get("reason").contains("incompatible with column data type"))
    }
  }
}
