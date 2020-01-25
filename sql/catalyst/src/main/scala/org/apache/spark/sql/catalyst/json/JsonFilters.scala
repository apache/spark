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

  private var allPredicates: List[JsonPredicate] = List.empty

  def skipRow(row: InternalRow, index: Int): Boolean = {
    false
  }

  def reset(): Unit = allPredicates.foreach(_.reset)
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
