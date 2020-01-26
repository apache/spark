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

package org.apache.spark.sql.catalyst.json

import org.apache.spark.sql.catalyst.{InternalRow, StructFilters}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

class JsonFilters(filters: Seq[sources.Filter], schema: StructType)
  extends StructFilters(filters, schema) {

  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int, var refCount: Int) {
    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  private val predicates: Array[Array[JsonPredicate]] = {
    val literals = filters.filter(_.references.isEmpty)
    val groupedByRefSet = filters
      .groupBy(_.references.toSet)
      .map { case (refSet, refsFilters) =>
        val reducedExpr = (literals ++ refsFilters)
          .flatMap(StructFilters.filterToExpression(_, toRef))
          .reduce(And)
        (refSet, JsonPredicate(Predicate.create(reducedExpr), refSet.size, 0))
      }
    val groupedByFields = groupedByRefSet.toSeq
      .flatMap { case (refSet, pred) => refSet.map((_, pred)) }
      .groupBy(_._1)
    val groupedPredicates = Array.fill(schema.length)(Array.empty[JsonPredicate])
    groupedByFields.foreach { case (fieldName, fieldPredicates) =>
      val fieldIndex = schema.fieldIndex(fieldName)
      groupedPredicates(fieldIndex) = fieldPredicates.map(_._2).toArray
    }
    groupedPredicates
  }

  def skipRow(row: InternalRow, index: Int): Boolean = {
    var skip = false
    predicates(index).foreach { pred =>
      pred.refCount -= 1
      if (!skip && pred.refCount == 0) {
        skip = !pred.predicate.eval(row)
      }
    }
    skip
  }

  def reset(): Unit = predicates.foreach(_.foreach(_.reset))
}
