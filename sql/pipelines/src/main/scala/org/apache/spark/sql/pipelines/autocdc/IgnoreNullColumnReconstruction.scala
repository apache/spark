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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.SparkException
import org.apache.spark.sql.{functions => F, Column}
import org.apache.spark.sql.catalyst.expressions.KnownNullable
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/** Rebuilds AutoCDC user-data columns selected for ignore-null from the values of their leaves. */
private[autocdc] object IgnoreNullColumnReconstruction {

  /**
   * Rebuilds the column at `path` as `field` from the values of its leaves.
   *
   * If `field` is a struct, it is rebuilt from the values of the leaves beneath each of its
   * fields. Otherwise, `field` is itself a leaf and takes its own value.
   *
   * A struct whose leaves are all null is rebuilt as a struct of nulls, which ignore-null treats
   * as equivalent to a null struct. A struct without leaves is rebuilt as an empty struct. A
   * nullable struct with a non-nullable leaf reachable through only non-nullable structs cannot
   * be rebuilt with that leaf null, so it is instead null unless a leaf beneath it is non-null.
   *
   * @param path The name parts of `field`, from its top-level column down. Each part is compared
   *             exactly to a field name, so it is not quoted.
   * @param field The schema to rebuild the column as: its name, type, nullability, and metadata.
   * @param leafValues The path, in the same form as `path`, and value of each leaf at or beneath
   *                   `path`. A leaf of `field` without a value raises an internal error.
   * @return The rebuilt value, cast to the type of `field` and aliased to its name and metadata.
   *         Evaluating it raises `NOT_NULL_ASSERT_VIOLATION` if `field` is non-nullable and its
   *         value is null.
   */
  def reconstructColumn(
      path: Seq[String],
      field: StructField,
      leafValues: Seq[(Seq[String], Column)]): Column = {
    val reconstructed = field.dataType match {
      case struct: StructType =>
        val leafValuesByChildName = leafValues.groupBy { case (leafPath, _) =>
          leafPath(path.length)
        }
        val rebuilt = F.struct(struct.fields.toImmutableArraySeq.map { childField =>
          reconstructColumn(
            path = path :+ childField.name,
            field = childField,
            leafValues = leafValuesByChildName.getOrElse(childField.name, Seq.empty))
        }: _*)

        if (field.nullable && cannotRebuildAsStructOfNulls(struct)) {
          // The schema forbids a struct of nulls here, so the equivalent null struct is used when
          // every leaf is null. If any leaf is non-null, the struct is rebuilt and a null
          // non-nullable leaf fails its assertion rather than the struct dropping non-null leaves.
          val everyLeafIsNull = leafValues.map { case (_, value) => value.isNull }.reduce(_ && _)
          F.when(everyLeafIsNull, F.lit(null).cast(field.dataType)).otherwise(rebuilt)
        } else {
          rebuilt
        }
      case _ =>
        leafValues.collectFirst { case (`path`, value) => value }.getOrElse {
          throw SparkException.internalError(
            s"Cannot reconstruct ${QuotingUtils.quoteNameParts(path)} because it has no value.")
        }
    }
    conformToField(reconstructed, path, field)
  }

  /**
   * Gives `value` the schema of `field`: its type, nullability, name, and metadata.
   *
   * @param path The name parts of `field`, from its top-level column down, reported if `field`
   *             is non-nullable and `value` is null.
   * @return `value`, cast to the type of `field` and aliased to its name and metadata. Evaluating
   *         it raises `NOT_NULL_ASSERT_VIOLATION` if `field` is non-nullable and `value` is null.
   */
  private[autocdc] def conformToField(
      value: Column,
      path: Seq[String],
      field: StructField): Column = {
    val withDeclaredNullability =
      if (field.nullable) {
        // Spark may report `value`, such as a rebuilt struct, as non-nullable; only
        // `KnownNullable` can widen that.
        ExpressionUtils.column(KnownNullable(ExpressionUtils.expression(value)))
      } else {
        ExpressionUtils.column(AssertNotNull(ExpressionUtils.expression(value), path))
      }
    withDeclaredNullability.cast(field.dataType).as(field.name, field.metadata)
  }

  /**
   * Whether `struct` has a non-nullable leaf whose enclosing structs beneath `struct` are all also
   * non-nullable. If such a leaf has no value, neither it nor any of those enclosing structs can
   * be null, so `struct` itself cannot be non-null.
   */
  private def cannotRebuildAsStructOfNulls(struct: StructType): Boolean =
    struct.fields.exists { field =>
      !field.nullable && (field.dataType match {
        case nested: StructType => cannotRebuildAsStructOfNulls(nested)
        case _ => true
      })
    }
}
