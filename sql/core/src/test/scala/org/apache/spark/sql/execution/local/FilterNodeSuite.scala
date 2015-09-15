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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.IntegerType


class FilterNodeSuite extends LocalNodeTest {
  private val attributes = Seq(
    AttributeReference("k", IntegerType)(),
    AttributeReference("v", IntegerType)())

  test("basic") {
    val n = 100
    val cond = 'k % 2 === 0
    val inputData = (1 to n).map { i => (i, i) }.toArray
    val inputNode = new DummyNode(attributes, inputData)
    val filterNode = new FilterNode(conf, cond, inputNode)
    val resolvedNode = resolveExpressions(filterNode)
    val expectedOutput = inputData.filter { case (k, _) => k % 2 == 0 }
    val actualOutput = resolvedNode.collect().map { case row =>
      (row.getInt(0), row.getInt(1))
    }
    assert(actualOutput === expectedOutput)
  }

  test("empty") {
    val cond = 'k % 2 === 0
    val inputData = Array.empty[(Int, Int)]
    val inputNode = new DummyNode(attributes, inputData)
    val filterNode = new FilterNode(conf, cond, inputNode)
    val resolvedNode = resolveExpressions(filterNode)
    val expectedOutput = inputData
    val actualOutput = resolvedNode.collect().map { case row =>
      (row.getInt(0), row.getInt(1))
    }
    assert(actualOutput === expectedOutput)
  }

}
