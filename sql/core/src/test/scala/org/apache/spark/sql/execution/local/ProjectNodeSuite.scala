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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.types.{IntegerType, StringType}


class ProjectNodeSuite extends LocalNodeTest {
  private val pieAttributes = Seq(
    AttributeReference("id", IntegerType)(),
    AttributeReference("age", IntegerType)(),
    AttributeReference("name", StringType)())

  private def testProject(inputData: Array[(Int, Int, String)] = Array.empty): Unit = {
    val inputNode = new DummyNode(pieAttributes, inputData)
    val columns = Seq[NamedExpression](inputNode.output(0), inputNode.output(2))
    val projectNode = new ProjectNode(conf, columns, inputNode)
    val expectedOutput = inputData.map { case (id, age, name) => (id, name) }
    val actualOutput = projectNode.collect().map { case row =>
      (row.getInt(0), row.getString(1))
    }
    assert(actualOutput === expectedOutput)
  }

  test("empty") {
    testProject()
  }

  test("basic") {
    testProject((1 to 100).map { i => (i, i + 1, "pie" + i) }.toArray)
  }

}
