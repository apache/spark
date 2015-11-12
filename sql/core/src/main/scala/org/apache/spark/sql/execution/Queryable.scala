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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

/** A trait that holds shared code between DataFrames and Datasets. */
private[sql] trait Queryable {
  def schema: StructType
  def queryExecution: QueryExecution

  override def toString: String = {
    try {
      schema.map(f => s"${f.name}: ${f.dataType.simpleString}").mkString("[", ", ", "]")
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  protected def nameColumns(columns: Seq[Column]): Seq[NamedExpression] = {
    columns.map {
      // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
      // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
      // make it a NamedExpression.
      case Column(u: UnresolvedAttribute) => UnresolvedAlias(u)

      case Column(expr: NamedExpression) => expr

      // Leave an unaliased generator with an empty list of names since the analyzer will generate
      // the correct defaults after the nested expression's type has been resolved.
      case Column(explode: Explode) => MultiAlias(explode, Nil)
      case Column(jt: JsonTuple) => MultiAlias(jt, Nil)

      case Column(expr: Expression) => Alias(expr, expr.prettyString)()
    }
  }
}
