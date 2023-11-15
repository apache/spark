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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand

/**
 * A command for writing the data out to a Python data source.
 *
 * @param query the logical plan representing data to write to.
 * @param dataSourceCls the serialized Python data source class.
 * @param provider the data source provider name.
 * @param options the data source options.
 * @param mode the save mode.
 * @param planned whether the logical plan is already planned. This flag is used by
 *                the optimizer rule `PythonDataSourceWrites` to ensure idempotency.
 */
case class SaveIntoPythonDataSourceCommand(
    query: LogicalPlan,
    dataSourceCls: PythonFunction,
    provider: String,
    options: Map[String, String],
    mode: SaveMode,
    planned: Boolean = false) extends DataWritingCommand {

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val rdd = child.execute()

    // TODO(SPARK-45914): support commit protocol (commit and abort).
    val ret = new Array[InternalRow](rdd.partitions.length)
    sparkSession.sparkContext.runJob(
      rdd,
      (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
        assert(iter.hasNext)
        val commitMessage = iter.next()
        assert(!iter.hasNext)
        commitMessage
      },
      rdd.partitions.indices,
      (index, res: InternalRow) => {
        ret(index) = res
      })

    Seq.empty[Row]
  }

  override def simpleString(maxFields: Int): String = {
    val redacted = conf.redactOptions(options)
    s"SaveIntoPythonDataSourceCommand $provider, $redacted, $mode"
  }

  override protected def withNewChildInternal(
    newChild: LogicalPlan): SaveIntoPythonDataSourceCommand = copy(query = newChild)
}
