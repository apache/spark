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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.catalyst.plans.logical.Unpivot

/**
 * Singleton object performing the type coercion on the given [[Unpivot]] node. Do that by:
 *  1. Get wider data type of inner values at same index.
 *  2. Cast inner values to type according to their index.
 */
object UnpivotTypeCoercion extends SQLConfHelper {
  def apply(unpivot: Unpivot): Unpivot = {
    val valueDataTypes = unpivot.values.get.head.zipWithIndex.map {
      case (_, index) =>
        TypeCoercion.findWiderTypeWithoutStringPromotion(
          unpivot.values.get.map(_(index).dataType)
        )
    }

    val values = unpivot.values.get.map(
      values =>
        values.zipWithIndex.map {
          case (value, index) => (value, valueDataTypes(index))
        } map {
          case (value, Some(valueType)) if value.dataType != valueType =>
            val cast = Cast(value, valueType)
            Alias(cast.withTimeZone(conf.sessionLocalTimeZone), value.name)()
          case (value, _) => value
        }
    )

    unpivot.copy(values = Some(values))
  }
}
