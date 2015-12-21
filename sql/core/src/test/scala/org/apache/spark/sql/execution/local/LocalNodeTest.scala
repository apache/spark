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

package org.apache.spark.sql.execution.local

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, AttributeReference}
import org.apache.spark.sql.types.{IntegerType, StringType}


class LocalNodeTest extends SparkFunSuite {

  protected val conf: SQLConf = new SQLConf
  protected val kvIntAttributes = Seq(
    AttributeReference("k", IntegerType)(),
    AttributeReference("v", IntegerType)())
  protected val joinNameAttributes = Seq(
    AttributeReference("id1", IntegerType)(),
    AttributeReference("name", StringType)())
  protected val joinNicknameAttributes = Seq(
    AttributeReference("id2", IntegerType)(),
    AttributeReference("nickname", StringType)())

  /**
   * Wrap a function processing two [[LocalNode]]s such that:
   *   (1) all input rows are automatically converted to unsafe rows
   *   (2) all output rows are automatically converted back to safe rows
   */
  protected def wrapForUnsafe(
      f: (LocalNode, LocalNode) => LocalNode): (LocalNode, LocalNode) => LocalNode = {
    (left: LocalNode, right: LocalNode) => {
      val _left = ConvertToUnsafeNode(conf, left)
      val _right = ConvertToUnsafeNode(conf, right)
      val r = f(_left, _right)
      ConvertToSafeNode(conf, r)
    }
  }

  /**
   * Recursively resolve all expressions in a [[LocalNode]] using the node's attributes.
   */
  protected def resolveExpressions(outputNode: LocalNode): LocalNode = {
    outputNode transform {
      case node: LocalNode =>
        val inputMap = node.output.map { a => (a.name, a) }.toMap
        node transformExpressions {
          case UnresolvedAttribute(Seq(u)) =>
            inputMap.getOrElse(u,
              sys.error(s"Invalid Test: Cannot resolve $u given input $inputMap"))
        }
    }
  }

  /**
   * Resolve all expressions in `expressions` based on the `output` of `localNode`.
   * It assumes that all expressions in the `localNode` are resolved.
   */
  protected def resolveExpressions(
      expressions: Seq[Expression],
      localNode: LocalNode): Seq[Expression] = {
    require(localNode.expressions.forall(_.resolved))
    val inputMap = localNode.output.map { a => (a.name, a) }.toMap
    expressions.map { expression =>
      expression.transformUp {
        case UnresolvedAttribute(Seq(u)) =>
          inputMap.getOrElse(u,
            sys.error(s"Invalid Test: Cannot resolve $u given input $inputMap"))
      }
    }
  }

}
