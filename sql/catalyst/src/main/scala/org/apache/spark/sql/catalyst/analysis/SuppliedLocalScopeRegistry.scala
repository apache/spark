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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.types.DataType
import org.apache.spark.util.LexicalThreadLocal

/**
 * One lexical SQL script scope supplied by an analysis caller: the scope label
 * and the local variables declared in it so far, in declaration order.
 */
private[sql] case class SuppliedLocalScope(label: String, variables: Seq[(String, DataType)])

/**
 * A lexical registry of SQL script local variable scopes supplied by an
 * analysis caller that analyzes a script without executing it.
 *
 * Scopes are ordered outermost first. Lookup mirrors the SQL script
 * interpreter: an unqualified name binds to the innermost scope declaring it,
 * and `label.name` binds to the innermost scope with that label.
 */
private[sql] object SuppliedLocalScopeRegistry
  extends LexicalThreadLocal[Seq[SuppliedLocalScope]] {

  def create(scopes: Seq[SuppliedLocalScope]): Handle = createHandle(Some(scopes))

  /** The scope and type bound by `nameParts` (one or two parts), if any. */
  def lookup(
      nameParts: Seq[String],
      resolver: (String, String) => Boolean): Option[(SuppliedLocalScope, DataType)] = {
    get().flatMap { scopes =>
      def typeIn(scope: SuppliedLocalScope, name: String): Option[DataType] =
        scope.variables.reverseIterator.collectFirst {
          case (declared, dataType) if resolver(declared, name) => dataType
        }
      nameParts match {
        case Seq(name) =>
          scopes.reverseIterator
            .map(scope => typeIn(scope, name).map(scope -> _))
            .collectFirst { case Some(found) => found }
        case Seq(label, name) =>
          scopes.reverseIterator.find(scope => resolver(scope.label, label))
            .flatMap(scope => typeIn(scope, name).map(scope -> _))
        case _ => None
      }
    }
  }
}
