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

package org.apache.spark.sql.catalog.v2.expressions

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
 * Helper methods for working with the logical expressions API.
 *
 * Factory methods can be used when referencing the logical expression nodes is ambiguous because
 * logical and internal expressions are used.
 */
private[sql] object LogicalExpressions {
  // a generic parser that is only used for parsing multi-part field names.
  // because this is only used for field names, the SQL conf passed in does not matter.
  private lazy val parser = new CatalystSqlParser(SQLConf.get)

  def literal[T](value: T): LiteralValue[T] = {
    val internalLit = catalyst.expressions.Literal(value)
    literal(value, internalLit.dataType)
  }

  def literal[T](value: T, dataType: DataType): LiteralValue[T] = LiteralValue(value, dataType)

  def reference(name: String): NamedReference =
    FieldReference(parser.parseMultipartIdentifier(name))

  def apply(name: String, arguments: Expression*): Transform = ApplyTransform(name, arguments)

  def bucket(numBuckets: Int, columns: String*): BucketTransform =
    BucketTransform(literal(numBuckets, IntegerType), columns.map(reference))

  def identity(column: String): IdentityTransform = IdentityTransform(reference(column))

  def years(column: String): YearsTransform = YearsTransform(reference(column))

  def months(column: String): MonthsTransform = MonthsTransform(reference(column))

  def days(column: String): DaysTransform = DaysTransform(reference(column))

  def hours(column: String): HoursTransform = HoursTransform(reference(column))
}

/**
 * Base class for simple transforms of a single column.
 */
private[sql] abstract class SingleColumnTransform(ref: NamedReference) extends Transform {

  def reference: NamedReference = ref

  override def references: Array[NamedReference] = Array(ref)

  override def arguments: Array[Expression] = Array(ref)

  override def describe: String = name + "(" + reference.describe + ")"

  override def toString: String = describe
}

private[sql] final case class BucketTransform(
    numBuckets: Literal[Int],
    columns: Seq[NamedReference]) extends Transform {

  override val name: String = "bucket"

  override def references: Array[NamedReference] = {
    arguments
        .filter(_.isInstanceOf[NamedReference])
        .map(_.asInstanceOf[NamedReference])
  }

  override def arguments: Array[Expression] = numBuckets +: columns.toArray

  override def describe: String = s"bucket(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe
}

private[sql] final case class ApplyTransform(
    name: String,
    args: Seq[Expression]) extends Transform {

  override def arguments: Array[Expression] = args.toArray

  override def references: Array[NamedReference] = {
    arguments
        .filter(_.isInstanceOf[NamedReference])
        .map(_.asInstanceOf[NamedReference])
  }

  override def describe: String = s"$name(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe
}

private[sql] final case class IdentityTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "identity"
  override def describe: String = ref.describe
}

private[sql] final case class YearsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "years"
}

private[sql] final case class MonthsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "months"
}

private[sql] final case class DaysTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "days"
}

private[sql] final case class HoursTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "hours"
}

private[sql] final case class LiteralValue[T](value: T, dataType: DataType) extends Literal[T] {
  override def describe: String = {
    if (dataType.isInstanceOf[StringType]) {
      s"'$value'"
    } else {
      s"$value"
    }
  }
  override def toString: String = describe
}

private[sql] final case class FieldReference(parts: Seq[String]) extends NamedReference {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.MultipartIdentifierHelper
  override def fieldNames: Array[String] = parts.toArray
  override def describe: String = parts.quoted
  override def toString: String = describe
}

private[sql] object FieldReference {
  def apply(column: String): NamedReference = {
    LogicalExpressions.reference(column)
  }
}
