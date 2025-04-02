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

package org.apache.spark.sql.execution

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.sql.execution.metric.SQLMetric

class WholeStageCodegenEvaluatorFactory(
    cleanedSourceOpt: Either[Broadcast[CodeAndComment], CodeAndComment],
    durationMs: SQLMetric,
    references: Array[Any]) extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new WholeStageCodegenPartitionEvaluator()
  }

  class WholeStageCodegenPartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val cleanedSource = cleanedSourceOpt match {
        case Left(broadcastCode: Broadcast[CodeAndComment]) =>
          broadcastCode.value
        case Right(code: CodeAndComment) =>
          code
      }
      val (clazz, _) = CodeGenerator.compile(cleanedSource)
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(partitionIndex, inputs.toArray)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next(): InternalRow = buffer.next()
      }
    }
  }
}
