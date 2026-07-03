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

package org.apache.spark.sql.execution.datasources.orc.types.ops

import java.time.LocalTime

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf
import org.apache.hadoop.io.{LongWritable, WritableComparable}
import org.apache.orc.TypeDescription

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils.localTimeToNanos
import org.apache.spark.sql.types.{LongType, TimeType}

/**
 * ORC operations for TimeType.
 *
 * TimeType is a primitive Long-backed type (nanoseconds since midnight). ORC has no TIME category,
 * so it is stored in the LONG physical category with the true Spark type recovered on read from
 * the CATALYST_TYPE_ATTRIBUTE_NAME attribute. Read and write are pure Long pass-through: the
 * internal nanos-of-day value IS the stored LONG (no unit conversion, unlike Parquet which may
 * store MICROS). Precision affects only display, so nothing is truncated here.
 *
 * @param t the TimeType (precision is unused on the ORC path; kept for symmetry with the other
 *          storage ops and possible future precision-aware behavior).
 * @since 4.4.0
 */
case class TimeTypeOrcOps(t: TimeType) extends OrcTypeOps {

  // ==================== Schema Mapping ====================

  // Was: OrcUtils.getOrcSchemaString  `case ... | _: TimeType => LongType.catalogString`
  override def orcSchemaString: String = LongType.catalogString

  // Was: OrcUtils.orcTypeDescription  `case tm: TimeType => new TypeDescription(LONG) ...`
  override def orcCategory: TypeDescription.Category = TypeDescription.Category.LONG

  // ==================== Value Write (serialize) ====================

  // Was: OrcSerializer.newConverter  `case ... | _: TimeType => ... LongWritable ...`
  override def makeSerializer(
      reuseObj: Boolean): (SpecializedGetters, Int) => WritableComparable[_] =
    if (reuseObj) {
      val result = new LongWritable()
      (getter: SpecializedGetters, ordinal: Int) => {
        result.set(getter.getLong(ordinal))
        result
      }
    } else {
      (getter: SpecializedGetters, ordinal: Int) => new LongWritable(getter.getLong(ordinal))
    }

  // ==================== Row-Based Read (deserialize) ====================

  // Was: OrcDeserializer.newWriter  `case ... | _: TimeType => ... setLong ...`
  override def makeDeserializer(
      setLong: (Int, Long) => Unit,
      set: (Int, Any) => Unit): (Int, WritableComparable[_]) => Unit =
    (ordinal: Int, value: WritableComparable[_]) =>
      setLong(ordinal, value.asInstanceOf[LongWritable].get)

  // ==================== Predicate Pushdown ====================

  // Was: OrcFilters.getPredicateLeafType  `case ... | _: TimeType => PredicateLeaf.Type.LONG`
  override def predicateLeafType: Option[PredicateLeaf.Type] = Some(PredicateLeaf.Type.LONG)

  // Was: OrcFilters.castLiteralValue  `case _: TimeType if value.isInstanceOf[LocalTime] => ...`
  override def castFilterLiteral(value: Any): Any = value match {
    case lt: LocalTime => localTimeToNanos(lt)
    case other => other
  }
}
