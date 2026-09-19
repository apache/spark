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

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf
import org.apache.hadoop.io.WritableComparable
import org.apache.orc.TypeDescription
import org.apache.orc.mapred.OrcTimestamp

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * ORC operations for TimestampLTZNanosType (nanosecond precision, with time zone).
 *
 * Stored in ORC as the TIMESTAMP_INSTANT category (matching TIMESTAMP with local time zone). The
 * internal TimestampNanosVal is converted through java.time.Instant in both directions; the true
 * Spark type and precision are recovered on read from the CATALYST_TYPE_ATTRIBUTE_NAME attribute.
 * ORC does not reuse the OrcTimestamp object (the conversion is already expensive), so the
 * serializer runs with reuse disabled regardless of the reuseObj hint.
 *
 * Predicate pushdown uses PredicateLeaf.Type.TIMESTAMP (matching the physical category). ORC's
 * SearchArgument builder requires the literal's class to be exactly java.sql.Timestamp (it rejects
 * the OrcTimestamp subclass), and ORC compares against a TIMESTAMP_INSTANT column by first shifting
 * the stored UTC value into the JVM default zone (the default useUTCTimestamp=false read mode Spark
 * uses). So the filter literal (an external java.time.Instant, already truncated to the column
 * precision upstream) is cast to a java.sql.Timestamp whose *local* wall clock equals the instant's
 * UTC wall clock, via Timestamp.valueOf(LocalDateTime.ofInstant(instant, UTC)). This keeps the
 * pushed predicate consistent with the ORC min/max statistics regardless of the JVM time zone.
 *
 * @since 5.0.0
 */
case class TimestampLTZNanosOrcOps(t: TimestampLTZNanosType) extends OrcTypeOps {

  // Was: OrcUtils.getOrcSchemaString
  // `case _: TimestampLTZNanosType => "timestamp with local time zone"`
  override def orcSchemaString: String = "timestamp with local time zone"

  // Was: OrcUtils.orcTypeDescription  `case t: TimestampLTZNanosType => ... TIMESTAMP_INSTANT ...`
  override def orcCategory: TypeDescription.Category = TypeDescription.Category.TIMESTAMP_INSTANT

  // Was: OrcSerializer.newConverter  `case t: TimestampLTZNanosType => ...`
  override def makeSerializer(
      reuseObj: Boolean): (SpecializedGetters, Int) => WritableComparable[_] =
    (getter: SpecializedGetters, ordinal: Int) => {
      val v = getter.get(ordinal, t).asInstanceOf[TimestampNanosVal]
      val instant = DateTimeUtils.timestampNanosToInstant(v)
      val result = new OrcTimestamp(instant.toEpochMilli)
      result.setNanos(instant.getNano)
      result
    }

  // Was: OrcDeserializer.newWriter  `case t: TimestampLTZNanosType => ...`
  override def makeDeserializer(
      setLong: (Int, Long) => Unit,
      set: (Int, Any) => Unit): (Int, WritableComparable[_]) => Unit =
    (ordinal: Int, value: WritableComparable[_]) => {
      val ts = value.asInstanceOf[OrcTimestamp]
      val instant = ts.toInstant
      set(ordinal, DateTimeUtils.instantToTimestampNanos(instant, t.precision))
    }

  // The physical ORC category is a timestamp, so the search argument uses the TIMESTAMP leaf type.
  override def predicateLeafType: Option[PredicateLeaf.Type] = Some(PredicateLeaf.Type.TIMESTAMP)

  // The filter literal is an external java.time.Instant (see CatalystTypeConverters). ORC evaluates
  // a TIMESTAMP_INSTANT predicate against the stored UTC value shifted into the JVM default zone,
  // so the literal must be a java.sql.Timestamp whose local wall clock equals the instant's UTC
  // wall clock; Timestamp.valueOf(LocalDateTime at UTC) produces exactly that. Any non-Instant
  // value is passed through unchanged.
  override def castFilterLiteral(value: Any): Any = value match {
    case i: Instant => Timestamp.valueOf(LocalDateTime.ofInstant(i, ZoneOffset.UTC))
    case other => other
  }
}

/**
 * ORC operations for TimestampNTZNanosType (nanosecond precision, without time zone).
 *
 * Stored in ORC as the TIMESTAMP category. The internal TimestampNanosVal is converted through
 * java.time.LocalDateTime in both directions; the true Spark type and precision are recovered on
 * read from the CATALYST_TYPE_ATTRIBUTE_NAME attribute. As with the LTZ variant, the OrcTimestamp
 * object is not reused.
 *
 * Predicate pushdown uses PredicateLeaf.Type.TIMESTAMP (matching the physical category). The filter
 * literal is an external java.time.LocalDateTime (already truncated to the column precision
 * upstream); it is cast to a plain java.sql.Timestamp via Timestamp.valueOf, the same conversion
 * the write path applies before wrapping the result in an OrcTimestamp. ORC's SearchArgument
 * builder requires the literal's class to be exactly java.sql.Timestamp (it rejects the
 * OrcTimestamp subclass), while the stored OrcTimestamp compares as its java.sql.Timestamp
 * super-value, so the two are value-equal and the pushed predicate is consistent with the ORC
 * min/max statistics.
 *
 * @since 5.0.0
 */
case class TimestampNTZNanosOrcOps(t: TimestampNTZNanosType) extends OrcTypeOps {

  // Was: OrcUtils.getOrcSchemaString  `case _: TimestampNTZNanosType => "timestamp"`
  override def orcSchemaString: String = "timestamp"

  // Was: OrcUtils.orcTypeDescription  `case t: TimestampNTZNanosType => ... TIMESTAMP ...`
  override def orcCategory: TypeDescription.Category = TypeDescription.Category.TIMESTAMP

  // Was: OrcSerializer.newConverter  `case t: TimestampNTZNanosType => ...`
  override def makeSerializer(
      reuseObj: Boolean): (SpecializedGetters, Int) => WritableComparable[_] =
    (getter: SpecializedGetters, ordinal: Int) => {
      val v = getter.get(ordinal, t).asInstanceOf[TimestampNanosVal]
      val localDateTime = DateTimeUtils.timestampNanosToLocalDateTime(v)
      val ts = Timestamp.valueOf(localDateTime)
      val result = new OrcTimestamp(ts.getTime)
      result.setNanos(ts.getNanos)
      result
    }

  // Was: OrcDeserializer.newWriter  `case t: TimestampNTZNanosType => ...`
  override def makeDeserializer(
      setLong: (Int, Long) => Unit,
      set: (Int, Any) => Unit): (Int, WritableComparable[_]) => Unit =
    (ordinal: Int, value: WritableComparable[_]) => {
      val ts = value.asInstanceOf[OrcTimestamp]
      val localDateTime = ts.toLocalDateTime
      set(ordinal, DateTimeUtils.localDateTimeToTimestampNanos(localDateTime, t.precision))
    }

  // The physical ORC category is a timestamp, so the search argument uses the TIMESTAMP leaf type.
  override def predicateLeafType: Option[PredicateLeaf.Type] = Some(PredicateLeaf.Type.TIMESTAMP)

  // The filter literal is an external java.time.LocalDateTime (see CatalystTypeConverters); convert
  // it to a plain java.sql.Timestamp via the same Timestamp.valueOf the serializer uses, so it is
  // value-equal to the written OrcTimestamp. Any non-LocalDateTime value is passed through
  // unchanged.
  override def castFilterLiteral(value: Any): Any = value match {
    case ldt: LocalDateTime => Timestamp.valueOf(ldt)
    case other => other
  }
}
