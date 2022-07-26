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

package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.connector.expressions.{LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.sources.{Filter, In}

private[sql] object PredicateUtils {

  def toV1(predicate: Predicate): Option[Filter] = {
    predicate.name() match {
      // Todo: add conversion for other V2 Predicate
      case "IN" =>
        val attribute = predicate.children()(0)
          .asInstanceOf[NamedReference].fieldNames().mkString(".")
        val values = predicate.children().drop(1)
        if (values.length > 0) {
          val dataType = values(0).asInstanceOf[LiteralValue[_]].dataType
          assert(values.forall(_.asInstanceOf[LiteralValue[_]].dataType.sameType(dataType)))
          val inValues = values.map(v =>
            CatalystTypeConverters.convertToScala(v.asInstanceOf[LiteralValue[_]].value, dataType))
          Some(In(attribute, inValues))
        } else {
          Some(In(attribute, Array.empty[Any]))
        }

      case _ =>
        throw QueryExecutionErrors.unsupportedPredicateToFilterConversionError(predicate.name())
    }
  }
}
