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

package org.apache.spark.sql.catalyst.expressions.variant

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr) - Parse a JSON string as an Variant value. Throw an exception when the string is not valid JSON value.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1,"b":0.8}');
       {"a":1,"b":0.8}
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
case class ParseJson(child: Expression)
  extends UnaryExpression with NullIntolerant with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    VariantExpressionEvalUtils.getClass,
    VariantType,
    "parseJson",
    Seq(child),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = VariantType

  override def prettyName: String = "parse_json"

  override protected def withNewChildInternal(newChild: Expression): ParseJson =
    copy(child = newChild)
}
