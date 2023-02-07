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
