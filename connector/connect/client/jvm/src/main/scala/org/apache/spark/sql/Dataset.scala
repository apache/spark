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
package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.SparkResult

class Dataset[T] private[sql] (val session: SparkSession, private[sql] val plan: proto.Plan)
    extends Serializable {

  /**
   * Selects a set of column based expressions.
   * {{{
   *   ds.select($"colA", $"colB" + 1)
   * }}}
   *
   * @group untypedrel
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = session.newDataset { builder =>
    builder.getProjectBuilder
      .setInput(plan.getRoot)
      .addAllExpressions(cols.map(_.expr).asJava)
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 3.4.0
   */
  def filter(condition: Column): Dataset[T] = session.newDataset { builder =>
    builder.getFilterBuilder.setInput(plan.getRoot).setCondition(condition.expr)
  }

  /**
   * Returns a new Dataset by taking the first `n` rows. The difference between this function and
   * `head` is that `head` is an action and returns an array (by triggering query execution) while
   * `limit` returns a new Dataset.
   *
   * @group typedrel
   * @since 3.4.0
   */
  def limit(n: Int): Dataset[T] = session.newDataset { builder =>
    builder.getLimitBuilder
      .setInput(plan.getRoot)
      .setLimit(n)
  }

  private[sql] def analyze: proto.AnalyzePlanResponse = session.analyze(plan)

  def collectResult(): SparkResult = session.execute(plan)
}
