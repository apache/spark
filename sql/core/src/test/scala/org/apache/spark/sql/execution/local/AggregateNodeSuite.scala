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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._

class AggregateNodeSuite extends LocalNodeTest {

  import testImplicits._

  test("basic") {
    val input = {
      for (key <- (0 until 4);
           value <- (0 until 10))
        yield (key.toString, key + value)
    }.toDF("key", "value")

    checkAnswer(
      input,
      node =>
        AggregateNode(conf, Seq(input.col("key").expr), Seq(
          Alias(input.col("key").expr, "max")(),
          Alias(Max(input.col("value").expr), "max")(),
          Alias(Min(input.col("value").expr), "min")(),
          Alias(Sum(input.col("value").expr), "sum")()
        ), node),
      input.groupBy('key).agg(
        max('value),
        min('value),
        sum('value)).collect()
    )

    checkAnswer(
      input,
      node =>
        AggregateNode(conf, Nil, Seq(
          Alias(Max(input.col("value").expr), "max")(),
          Alias(Min(input.col("value").expr), "min")(),
          Alias(Sum(input.col("value").expr), "sum")()
        ), node),
      input.agg(
        max('value),
        min('value),
        sum('value)).collect()
    )
  }

}
