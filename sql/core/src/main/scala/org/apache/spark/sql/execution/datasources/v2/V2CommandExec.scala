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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.execution.SparkPlan

/**
 * A physical operator that executes run() and saves the result to prevent multiple executions.
 * Any V2 commands that do not require triggering a spark job should extend this class.
 */
abstract class V2CommandExec extends SparkPlan {

  /**
   * Abstract method that each concrete command needs to implement to compute the result.
   */
  protected def run(): Seq[InternalRow]

  /**
   * The value of this field can be used as the contents of the corresponding RDD generated from
   * the physical plan of this command.
   */
  private lazy val result: Seq[InternalRow] = run()

  /**
   * The `execute()` method of all the physical command classes should reference `result`
   * so that the command can be executed eagerly right after the command query is created.
   */
  override def executeCollect(): Array[InternalRow] = result.toArray

  override def executeToIterator(): Iterator[InternalRow] = result.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = result.take(limit).toArray

  override def executeTail(limit: Int): Array[InternalRow] = result.takeRight(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(result, 1)
  }

  override def children: Seq[SparkPlan] = Nil

  override def producedAttributes: AttributeSet = outputSet

}
