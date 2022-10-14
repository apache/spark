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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(str) - Returns hash of the given value. This can be useful for creating copies of tables with sensitive information removed.
           The hash is consistent and can be used to join masked values together across tables. This function returns null for non-string types.
            Error behavior: there are no error cases for this expression, it always returns a result string for every input string.""",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
      > SELECT _FUNC_('Spark', FALSE);
       529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
      > SELECT _FUNC_('Spark', TRUE);
       44844a586c54c9a212da1dbfe05c5f1705de1af5fda1f0d36297623249b279fd8f0ccec03f888f4fb13bf7cd83fdad58591c797f81121a23cfdd5e0897795238
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class MaskHash(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(left: Expression, right: Expression) = this(left, right,
    {
      val alg = Literal(if (right.eval().asInstanceOf[Boolean]) 512 else 256)
      (left.dataType, right.dataType) match {
        case (_: BinaryType, _: IntegerType) => Sha2(left, alg)
        case _ => Sha2(Cast(left, StringType), alg)
      }
    }
  )

  def this(left: Expression) = this(left, Literal(false))

  override def prettyName: String = "mask_hash"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}
