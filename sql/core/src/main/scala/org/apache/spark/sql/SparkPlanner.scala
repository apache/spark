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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.DataSourceStrategy


protected[sql] class SparkPlanner(val sqlContext: SQLContext) extends SparkStrategies {

  def codegenEnabled: Boolean = sqlContext.conf.codegenEnabled

  def numPartitions: Int = sqlContext.conf.numShufflePartitions

  def strategies: Seq[Strategy] =
    sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
        DDLStrategy ::
        TakeOrdered ::
        HashAggregation ::
        LeftSemiJoin ::
        HashJoin ::
        InMemoryScans ::
        ParquetOperations ::
        BasicOperators ::
        CartesianProduct ::
        BroadcastNestedLoopJoin :: Nil)


}
