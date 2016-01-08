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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.encoders.OuterScopes

class CoGroupNodeSuite extends LocalNodeTest {

  private def func(key: Int, data1: Iterator[(Int, String)], data2: Iterator[(Int, String)]) = {
    Iterator(key -> (data1.map(_._2).mkString + "#" + data2.map(_._2).mkString))
  }

  protected val outputAttributes = Seq(
    AttributeReference("id", IntegerType)(),
    AttributeReference("new_name", StringType)())

  protected val leftKeyAttributes = Seq('id1.attr)
  protected val rightKeyAttributes = Seq('id2.attr)

  test("basic") {
    val leftData = Seq(1 -> "a", 3 -> "abc", 3 -> "foo", 5 -> "hello")
    val rightData = Seq(2 -> "q", 3 -> "w", 5 -> "e", 5 -> "r")

    val leftNode = new DummyNode(joinNameAttributes, leftData)
    val rightNode = new DummyNode(joinNicknameAttributes, rightData)

    val resolvedLeftNode = resolveExpressions(leftNode)
    val resolvedLeftKeys = resolveExpressions(
        leftKeyAttributes, resolvedLeftNode).map(_.asInstanceOf[Attribute])

    val resolvedRightNode = resolveExpressions(rightNode)
    val resolvedRightKeys = resolveExpressions(
        rightKeyAttributes, resolvedRightNode).map(_.asInstanceOf[Attribute])

    val os = OuterScopes.outerScopes
    val resolvedKeyEncoder = ExpressionEncoder[Int]().resolve(resolvedLeftKeys, os)
    val resolvedLeftEncoder = ExpressionEncoder[(Int, String)]().resolve(joinNameAttributes, os)
    val resolvedRightEncoder = ExpressionEncoder[(Int, String)]().resolve(
        joinNicknameAttributes, os)
    val resolvedResultEncoder = ExpressionEncoder[(Int, String)]().resolve(outputAttributes, os)

    val coGroupNode = new CoGroupNode(
        conf, resolvedLeftNode, resolvedRightNode, func,
        resolvedKeyEncoder,
        resolvedLeftEncoder,
        resolvedRightEncoder,
        resolvedResultEncoder,
        outputAttributes,
        resolvedLeftKeys,
        resolvedRightKeys)

    val actualOutput = coGroupNode.collect().map {
      case row => (row.getInt(0), row.getString(1))
    }

    val expectedOutput = Seq(1 -> "a#", 2 -> "#q", 3 -> "abcfoo#w", 5 -> "hello#er")
    assert(actualOutput === expectedOutput)
  }
}
