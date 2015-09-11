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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, SortOrder}

class SampleNodeSuite extends LocalNodeTest {

  import testImplicits._

  def columnToSortOrder(sortExprs: Column*): Seq[SortOrder] = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    sortOrder
  }

  test("withReplacement: true") {
    val seed = 0L
    val input = sqlContext.sparkContext.
      parallelize((1 to 10).map(i => (i, i.toString)), 1). // Should be only 1 partition
      toDF("key", "value")
    checkAnswer(
      input,
      node => SampleNode(conf, 0.0, 0.3, true, seed, node),
      input.sample(true, 0.3, seed).collect()
    )
  }

  test("withReplacement: false") {
    val seed = 0L
    val input = sqlContext.sparkContext.
      parallelize((1 to 10).map(i => (i, i.toString)), 1). // Should be only 1 partition
      toDF("key", "value")
    checkAnswer(
      input,
      node => SampleNode(conf, 0.0, 0.3, false, seed, node),
      input.sample(false, 0.3, seed).collect()
    )
  }
}
