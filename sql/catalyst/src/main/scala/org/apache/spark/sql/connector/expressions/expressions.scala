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

package org.apache.spark.sql.connector.expressions

import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.util.ArrayImplicits._

/**
 * Helper methods for working with the logical expressions API.
 *
 * Factory methods can be used when referencing the logical expression nodes is ambiguous because
 * logical and internal expressions are used.
 */
private[sql] object LogicalExpressions {
  def literal[T](value: T): LiteralValue[T] = {
    val internalLit = catalyst.expressions.Literal(value)
    literal(value, internalLit.dataType)
  }

  def literal[T](value: T, dataType: DataType): LiteralValue[T] = LiteralValue(value, dataType)

  def parseReference(name: String): NamedReference =
    FieldReference(CatalystSqlParser.parseMultipartIdentifier(name))

  def reference(nameParts: Seq[String]): NamedReference = FieldReference(nameParts)

  def apply(name: String, arguments: Expression*): Transform = ApplyTransform(name, arguments)

  def bucket(numBuckets: Int, references: Array[NamedReference]): BucketTransform =
    BucketTransform(literal(numBuckets, IntegerType), references.toImmutableArraySeq)

  def bucket(
      numBuckets: Int,
      references: Array[NamedReference],
      sortedCols: Array[NamedReference]): SortedBucketTransform =
    SortedBucketTransform(literal(numBuckets, IntegerType),
      references.toImmutableArraySeq, sortedCols.toImmutableArraySeq)

  def clusterBy(references: Array[NamedReference]): ClusterByTransform =
    ClusterByTransform(references.toImmutableArraySeq)

  def identity(reference: NamedReference): IdentityTransform = IdentityTransform(reference)

  def years(reference: NamedReference): YearsTransform = YearsTransform(reference)

  def months(reference: NamedReference): MonthsTransform = MonthsTransform(reference)

  def days(reference: NamedReference): DaysTransform = DaysTransform(reference)

  def hours(reference: NamedReference): HoursTransform = HoursTransform(reference)

  def sort(
      reference: Expression,
      direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = {
    SortValue(reference, direction, nullOrdering)
  }
}

/**
 * Allows Spark to rewrite the given references of the transform during analysis.
 */
private[sql] sealed trait RewritableTransform extends Transform {
  /** Creates a copy of this transform with the new analyzed references. */
  def withReferences(newReferences: Seq[NamedReference]): Transform
}

/**
 * Base class for simple transforms of a single column.
 */
private[sql] abstract class SingleColumnTransform(ref: NamedReference) extends RewritableTransform {

  def reference: NamedReference = ref

  override def references: Array[NamedReference] = Array(ref)

  override def arguments: Array[Expression] = Array(ref)

  override def toString: String = name + "(" + reference.describe + ")"

  protected def withNewRef(ref: NamedReference): Transform

  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    assert(newReferences.length == 1,
      s"Tried rewriting a single column transform (${this}) with multiple references.")
    withNewRef(newReferences.head)
  }
}

private[sql] final case class BucketTransform(
    numBuckets: Literal[Int],
    columns: Seq[NamedReference]) extends RewritableTransform {

  override val name: String = "bucket"

  override def references: Array[NamedReference] = {
    arguments.collect { case named: NamedReference => named }
  }

  override def arguments: Array[Expression] = numBuckets +: columns.toArray

  override def describe: String = s"bucket(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe

  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    this.copy(columns = newReferences)
  }
}

private[sql] object BucketTransform {
  def unapply(transform: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] =
      transform match {
    case NamedTransform("sorted_bucket", arguments) =>
      var posOfLit: Int = -1
      var numOfBucket: Int = -1
      arguments.zipWithIndex.foreach {
        case (Lit(value: Int, IntegerType), i) =>
          numOfBucket = value
          posOfLit = i
        case _ =>
      }
      Some(numOfBucket, arguments.take(posOfLit).map(_.asInstanceOf[NamedReference]),
        arguments.drop(posOfLit + 1).map(_.asInstanceOf[NamedReference]))
    case NamedTransform("bucket", arguments) =>
      var numOfBucket: Int = -1
      arguments(0) match {
        case Lit(value: Int, IntegerType) =>
          numOfBucket = value
        case _ => throw new SparkException("The first element in BucketTransform arguments " +
          "should be an Integer Literal.")
      }
      Some(numOfBucket, arguments.drop(1).map(_.asInstanceOf[NamedReference]),
        Seq.empty[FieldReference])
    case _ =>
      None
  }
}

/**
 * This class represents a transform for `ClusterBySpec`. This is used to bundle
 * ClusterBySpec in CreateTable's partitioning transforms to pass it down to analyzer.
 */
final case class ClusterByTransform(
    columnNames: Seq[NamedReference]) extends RewritableTransform {

  override val name: String = "cluster_by"

  override def references: Array[NamedReference] = columnNames.toArray

  override def arguments: Array[Expression] = columnNames.toArray

  override def toString: String = s"$name(${arguments.map(_.describe).mkString(", ")})"

  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    this.copy(columnNames = newReferences)
  }
}

/**
 * Convenience extractor for ClusterByTransform.
 */
object ClusterByTransform {
  def unapply(transform: Transform): Option[Seq[NamedReference]] =
    transform match {
      case NamedTransform("cluster_by", arguments) =>
        Some(arguments.map(_.asInstanceOf[NamedReference]))
      case _ =>
        None
    }
}

private[sql] final case class SortedBucketTransform(
    numBuckets: Literal[Int],
    columns: Seq[NamedReference],
    sortedColumns: Seq[NamedReference] = Seq.empty[NamedReference]) extends RewritableTransform {

  override val name: String = "sorted_bucket"

  override def references: Array[NamedReference] = {
    arguments.collect { case named: NamedReference => named }
  }

  override def arguments: Array[Expression] = (columns.toArray :+ numBuckets) ++ sortedColumns

  override def toString: String = s"$name(${arguments.map(_.describe).mkString(", ")})"

  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    this.copy(columns = newReferences.take(columns.length),
      sortedColumns = newReferences.drop(columns.length))
  }
}

private[sql] final case class ApplyTransform(
    name: String,
    args: Seq[Expression]) extends Transform {

  override def arguments: Array[Expression] = args.toArray

  override def references: Array[NamedReference] = {
    arguments.collect { case named: NamedReference => named }
  }

  override def toString: String = s"$name(${arguments.map(_.describe).mkString(", ")})"
}

/**
 * Convenience extractor for any Literal.
 */
private object Lit {
  def unapply[T](literal: Literal[T]): Some[(T, DataType)] = {
    Some((literal.value, literal.dataType))
  }
}

/**
 * Convenience extractor for any NamedReference.
 */
private object Ref {
  def unapply(named: NamedReference): Some[Seq[String]] = {
    Some(named.fieldNames.toImmutableArraySeq)
  }
}

/**
 * Convenience extractor for any Transform.
 */
private[sql] object NamedTransform {
  def unapply(transform: Transform): Some[(String, Seq[Expression])] = {
    Some((transform.name, transform.arguments.toImmutableArraySeq))
  }
}

private[sql] final case class IdentityTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "identity"
  override def describe: String = ref.describe
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object IdentityTransform {
  def unapply(expr: Expression): Option[FieldReference] = expr match {
    case transform: Transform =>
      transform match {
        case IdentityTransform(ref) =>
          Some(ref)
        case _ =>
          None
      }
    case _ =>
      None
  }

  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("identity", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class YearsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "years"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object YearsTransform {
  def unapply(expr: Expression): Option[FieldReference] = expr match {
    case transform: Transform =>
      transform match {
        case YearsTransform(ref) =>
          Some(ref)
        case _ =>
          None
      }
    case _ =>
      None
  }

  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("years", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class MonthsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "months"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object MonthsTransform {
  def unapply(expr: Expression): Option[FieldReference] = expr match {
    case transform: Transform =>
      transform match {
        case MonthsTransform(ref) =>
          Some(ref)
        case _ =>
          None
      }
    case _ =>
      None
  }

  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("months", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class DaysTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "days"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object DaysTransform {
  def unapply(expr: Expression): Option[FieldReference] = expr match {
    case transform: Transform =>
      transform match {
        case DaysTransform(ref) =>
          Some(ref)
        case _ =>
          None
      }
    case _ =>
      None
  }

  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("days", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class HoursTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "hours"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object HoursTransform {
  def unapply(expr: Expression): Option[FieldReference] = expr match {
    case transform: Transform =>
      transform match {
        case HoursTransform(ref) =>
          Some(ref)
        case _ =>
          None
      }
    case _ =>
      None
  }

  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("hours", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class LiteralValue[T](value: T, dataType: DataType) extends Literal[T] {
  override def toString: String = {
    if (dataType.isInstanceOf[StringType]) {
      s"'${StringUtils.replace(s"$value", "'", "''")}'"
    } else {
      s"$value"
    }
  }
}

private[sql] final case class FieldReference(parts: Seq[String]) extends NamedReference {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
  override def fieldNames: Array[String] = parts.toArray
  override def toString: String = parts.quoted
}

private[sql] object FieldReference {
  def apply(column: String): NamedReference = {
    LogicalExpressions.parseReference(column)
  }

  def column(name: String) : NamedReference = {
    FieldReference(Seq(name))
  }
}

private[sql] final case class SortValue(
    expression: Expression,
    direction: SortDirection,
    nullOrdering: NullOrdering) extends SortOrder {

  override def toString(): String = s"$expression $direction $nullOrdering"
}

private[sql] object SortValue {
  def unapply(expr: Expression): Option[(Expression, SortDirection, NullOrdering)] = expr match {
    case sort: SortOrder =>
      Some((sort.expression, sort.direction, sort.nullOrdering))
    case _ =>
      None
  }
}
