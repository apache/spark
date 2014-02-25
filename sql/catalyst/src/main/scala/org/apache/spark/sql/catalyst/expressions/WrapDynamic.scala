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
package catalyst
package expressions

import scala.language.dynamics

import types._

case object DynamicType extends DataType

case class WrapDynamic(children: Seq[Attribute]) extends Expression with ImplementedUdf {
  def nullable = false
  def references = children.toSet
  def dataType = DynamicType

  def evaluate(evaluatedChildren: Seq[Any]): Any =
    new DynamicRow(children, evaluatedChildren)
}

class DynamicRow(val schema: Seq[Attribute], values: Seq[Any])
  extends GenericRow(values) with Dynamic {

  def selectDynamic(attributeName: String): String = {
    val ordinal = schema.indexWhere(_.name == attributeName)
    values(ordinal).toString
  }
}
