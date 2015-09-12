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

class IntersectNodeSuite extends LocalNodeTest {

  import testImplicits._

  test("basic") {
    val input1 = (1 to 10).map(i => (i, i.toString)).toDF("key", "value")
    val input2 = (1 to 10).filter(_ % 2 == 0).map(i => (i, i.toString)).toDF("key", "value")

    checkAnswer2(
      input1,
      input2,
      (node1, node2) => IntersectNode(conf, node1, node2),
      input1.intersect(input2).collect()
    )
  }
}
