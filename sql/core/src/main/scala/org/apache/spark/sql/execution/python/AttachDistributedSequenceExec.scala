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

package org.apache.spark.sql.execution.python

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/**
 * A physical plan that adds a new long column with `sequenceAttr` that
 * increases one by one. This is for 'distributed-sequence' default index
 * in pandas API on Spark.
 */
case class AttachDistributedSequenceExec(
    sequenceAttr: Attribute,
    child: SparkPlan)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(sequenceAttr)

  override val output: Seq[Attribute] = sequenceAttr +: child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().map(_.copy())
        .localCheckpoint() // to avoid execute multiple jobs. zipWithIndex launches a Spark job.
        .zipWithIndex().mapPartitions { iter =>
      val unsafeProj = UnsafeProjection.create(output, output)
      val joinedRow = new JoinedRow
      val unsafeRowWriter =
        new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1)

      iter.map { case (row, id) =>
        // Writes to an UnsafeRow directly
        unsafeRowWriter.reset()
        unsafeRowWriter.write(0, id)
        joinedRow(unsafeRowWriter.getRow, row)
      }.map(unsafeProj)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AttachDistributedSequenceExec =
    copy(child = newChild)
}
