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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_LOOKUP_JOIN, TreePattern}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CharType, DataType,
  DateType, DecimalType, IntegerType, LongType, ShortType, StringType, TimestampNTZType,
  TimestampType, VarcharType}

/**
 * A lookup join retained through ordinary expression and operator optimization.
 *
 * @param requiredLookupOutput Every output attribute of the lookup side. They are all part of this
 *   node's references, so column pruning never puts a projection over the lookup relation, which
 *   preparation needs directly under the join. A narrower list would let it do so.
 */
case class ExternalLookupJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    requiredLookupOutput: Seq[Attribute]) extends BinaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_LOOKUP_JOIN)

  override def output: Seq[Attribute] = joinType match {
    case Inner => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): ExternalLookupJoin = copy(left = newLeft, right = newRight)
}

/** A lookup join after its optimized lookup side has been converted to an external operation. */
case class PreparedExternalLookupJoin(
    child: LogicalPlan,
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupParameters: Seq[Expression],
    nullSafeParameters: Seq[Boolean],
    joinType: JoinType,
    residualCondition: Option[Expression]) extends UnaryNode {

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet))
        .getOrElse(AttributeSet.empty)

  override protected def withNewChildInternal(newChild: LogicalPlan): PreparedExternalLookupJoin =
    copy(child = newChild)

  // Connector descriptions may contain secrets, so the scan and the reader factory are not shown.
  // The physical node renders a redacted description of them.
  override protected def stringArgs: Iterator[Any] =
    Iterator(joinType, lookupParameters, residualCondition, child)

  // The join keeps the size estimate it had before preparation, as an ordinary join's: the product
  // of the stream side's size and the lookup side's, Spark's default size for a table it cannot
  // scan. The stream side's size alone would ignore the lookup columns and the rows that a stream
  // row can match.
  override def stats: Statistics = statsCache.getOrElse {
    statsCache = Some(Statistics(sizeInBytes =
      Seq(child.stats.sizeInBytes, BigInt(conf.defaultSizeInBytes)).filter(_ > 0L).product))
    statsCache.get
  }
}

/**
 * The types of lookup columns on which Spark offers a comparison to the connector as a lookup
 * candidate. The connector evaluates a selected candidate in its store, so Spark offers only types
 * whose values a store compares as Spark does. A comparison on any other type stays in Spark's
 * residual condition.
 */
object ExternalLookupKeyTypes {
  /**
   * Whether Spark offers `=` on the lookup column `column`, by its declared type: integral types,
   * BOOLEAN, DECIMAL, DATE, TIMESTAMP, TIMESTAMP_NTZ, BINARY, and STRING and VARCHAR whose
   * collation compares bytes, such as UTF8_BINARY. FLOAT and DOUBLE are not offered, because
   * stores treat NaN and -0.0 in different ways, nor CHAR, which Spark pads on read, nor strings
   * with another collation, nor complex types, nor any other type. VARCHAR is not offered under
   * `spark.sql.charVarchar.standardSemantics.enabled` either, because Spark then trims trailing
   * blanks on read, so a store that compares the stored value could miss a row.
   *
   * Analysis reads a CHAR or VARCHAR column as a STRING and keeps its declared type in the
   * metadata, so the column's data type alone does not tell.
   */
  def supportsEquality(column: Attribute): Boolean =
    supportsEquality(CharVarcharUtils.getRawType(column.metadata).getOrElse(column.dataType))

  private def supportsEquality(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | BooleanType | DateType | TimestampType |
        TimestampNTZType | BinaryType | _: DecimalType => true
    case _: CharType => false
    case varchar: VarcharType =>
      !SQLConf.get.charVarcharStandardSemantics && varchar.supportsBinaryEquality
    case stringType: StringType => stringType.supportsBinaryEquality
    case _ => false
  }
}
