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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BasePredicate
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, StructType}

class JsonFilters(filters: Seq[sources.Filter], schema: DataType) {
  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int, var refCount: Int) {
    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  private val indexedPredicates: Array[Array[JsonPredicate]] = schema match {
    case st: StructType =>
      Array.fill(st.length)(null)
    case _ => null
  }

  def skipRow(row: InternalRow, index: Int): Boolean = {
    var skip = false
    assert(indexedPredicates != null, "skipRow() can be called only for structs")
    val predicates = indexedPredicates(index)
    if (predicates != null) {
      val len = predicates.length
      var i = 0

      while (i < len) {
        val pred = predicates(i)
        pred.refCount -= 1
        if (!skip && pred.refCount == 0) {
          skip = !pred.predicate.eval(row)
        }
        i += 1
      }
    }

    skip
  }

  private val allPredicates: Array[JsonPredicate] = {
    val predicates = new ArrayBuffer[JsonPredicate]()
    if (indexedPredicates != null) {
      for {
        groupedPredicates <- indexedPredicates
        predicate <- groupedPredicates if groupedPredicates != null
      } {
        predicates += predicate
      }
    }
    predicates.toArray
  }

  def reset(): Unit = {
    val len = allPredicates.length
    var i = 0
    while (i < len) {
      val pred = allPredicates(i)
      pred.refCount = pred.totalRefs
      i += 1
    }
  }
}

object JsonFilters {
  private def checkFilterRefs(filter: sources.Filter, schema: StructType): Boolean = {
    val fieldNames = schema.fields.map(_.name).toSet
    filter.references.forall(fieldNames.contains(_))
  }

  /**
   * Returns the filters currently supported by JSON datasource.
   * @param filters The filters pushed down to JSON datasource.
   * @param schema data schema of JSON files.
   * @return a sub-set of `filters` that can be handled by JSON datasource.
   */
  def pushedFilters(filters: Array[sources.Filter], schema: StructType): Array[sources.Filter] = {
    filters.filter(checkFilterRefs(_, schema))
  }
}
