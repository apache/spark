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
package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.SortOrder.NullOrdering
import org.apache.spark.connect.proto.Expression.SortOrder.SortDirection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column.fn
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.functions.lit

/**
 * A column that will be computed based on the data in a `DataFrame`.
 *
 * A new column can be constructed based on the input columns present in a DataFrame:
 *
 * {{{
 *   df("columnName")            // On a specific `df` DataFrame.
 *   col("columnName")           // A generic column not yet associated with a DataFrame.
 *   col("columnName.field")     // Extracting a struct field
 *   col("`a.column.with.dots`") // Escape `.` in column names.
 *   $"columnName"               // Scala short hand for a named column.
 * }}}
 *
 * [[Column]] objects can be composed to form complex expressions:
 *
 * {{{
 *   $"a" + 1
 * }}}
 *
 * @since 3.4.0
 */
class Column private[sql] (private[sql] val expr: proto.Expression) extends Logging {

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").plus(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def +(other: Any): Column = fn("+", this, lit(other))

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def ===(other: Any): Column = fn("=", this, lit(other))

  /**
   * Gives the column a name (alias).
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".name("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated to
   * the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)` with
   * explicit metadata.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def name(alias: String): Column = Column { builder =>
    builder.getAliasBuilder.addName(alias).setExpr(expr)
  }

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   // Scala
   *   df.sort(df("age").desc)
   *
   *   // Java
   *   df.sort(df.col("age").desc());
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def desc: Column = desc_nulls_last

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing first.
   *   df.sort(df("age").desc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_first: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_FIRST)

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing last.
   *   df.sort(df("age").desc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_last: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_LAST)

  /**
   * Returns a sort expression based on ascending order of the column.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order.
   *   df.sort(df("age").asc)
   *
   *   // Java
   *   df.sort(df.col("age").asc());
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def asc: Column = asc_nulls_first

  /**
   * Returns a sort expression based on ascending order of the column, and null values return
   * before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing first.
   *   df.sort(df("age").asc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_first: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_FIRST)

  /**
   * Returns a sort expression based on ascending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing last.
   *   df.sort(df("age").asc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_last: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_LAST)

  private def buildSortOrder(sortDirection: SortDirection, nullOrdering: NullOrdering): Column =
    Column { builder =>
      builder.getSortOrderBuilder
        .setChild(expr)
        .setDirection(sortDirection)
        .setNullOrdering(nullOrdering)
    }

  private[sql] def sortOrder: proto.Expression.SortOrder = {
    val base = if (expr.hasSortOrder) {
      expr
    } else {
      asc.expr
    }
    base.getSortOrder
  }
}

private[sql] object Column {

  def apply(name: String): Column = Column { builder =>
    name match {
      case "*" =>
        builder.getUnresolvedStarBuilder
      case _ if name.endsWith(".*") =>
        unsupported("* with prefix is not supported yet.")
      case _ =>
        builder.getUnresolvedAttributeBuilder.setUnparsedIdentifier(name)
    }
  }

  private[sql] def apply(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    new Column(builder.build())
  }

  private[sql] def fn(name: String, inputs: Column*): Column = Column { builder =>
    builder.getUnresolvedFunctionBuilder
      .setFunctionName(name)
      .addAllArguments(inputs.map(_.expr).asJava)
  }
}
