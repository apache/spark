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

class ExpandNodeSuite extends LocalNodeTest {

  import testImplicits._

  test("expand") {
    val input = Seq((1, 1), (2, 2), (3, 3), (4, 4), (5, 5)).toDF("key", "value")
    checkAnswer(
      input,
      node =>
        ExpandNode(conf, Seq(
          Seq(
            input.col("key") + input.col("value"), input.col("key") - input.col("value")
          ).map(_.expr),
          Seq(
            input.col("key") * input.col("value"), input.col("key") / input.col("value")
          ).map(_.expr)
        ), node.output, node),
      Seq(
        (2, 0),
        (1, 1),
        (4, 0),
        (4, 1),
        (6, 0),
        (9, 1),
        (8, 0),
        (16, 1),
        (10, 0),
        (25, 1)
      ).toDF().collect()
    )
  }
}
