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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, UpCast}
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, DataType, DecimalType, StringType}

object UpCastResolution extends SQLConfHelper {
  def resolve(unresolvedUpCast: UpCast): Expression = unresolvedUpCast match {
    case UpCast(_, target, _) if target != DecimalType && !target.isInstanceOf[DataType] =>
      throw SparkException.internalError(
        s"UpCast only supports DecimalType as AbstractDataType yet, but got: $target"
      )

    case UpCast(child, target, walkedTypePath)
        if target == DecimalType
        && child.dataType.isInstanceOf[DecimalType] =>
      assert(
        walkedTypePath.nonEmpty,
        "object DecimalType should only be used inside ExpressionEncoder"
      )

      // SPARK-31750: if we want to upcast to the general decimal type, and the `child` is
      // already decimal type, we can remove the `Upcast` and accept any precision/scale.
      // This can happen for cases like `spark.read.parquet("/tmp/file").as[BigDecimal]`.
      child

    case UpCast(child, target: AtomicType, _)
        if conf.getConf(SQLConf.LEGACY_LOOSE_UPCAST) &&
        child.dataType == StringType =>
      Cast(child, target.asNullable)

    case unresolvedUpCast @ UpCast(child, _, walkedTypePath)
        if !Cast.canUpCast(child.dataType, unresolvedUpCast.dataType) =>
      fail(child, unresolvedUpCast.dataType, walkedTypePath)

    case unresolvedUpCast @ UpCast(child, _, _) =>
      Cast(child, unresolvedUpCast.dataType)
  }

  private def fail(from: Expression, to: DataType, walkedTypePath: Seq[String]) = {
    val fromStr = from match {
      case l: LambdaVariable => "array element"
      case e => e.sql
    }

    throw QueryCompilationErrors.upCastFailureError(fromStr, from, to, walkedTypePath)
  }
}
