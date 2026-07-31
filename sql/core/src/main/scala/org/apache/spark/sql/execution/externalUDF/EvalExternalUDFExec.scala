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

package org.apache.spark.sql.execution.externalUDF

import org.apache.spark.TaskContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet,
  ExternalUserDefinedFunction}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Physical plan node that evaluates scalar UDF expressions in one external
 * worker session.
 */
@Experimental
case class EvalExternalUDFExec(
    workerSpec: UDFWorkerSpecification,
    udfs: Seq[ExternalUserDefinedFunction],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends ExternalUDFExec {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { _ =>
      withUDFWorkerSession(TaskContext.get(), securityScope = None) { _ =>
        // TODO [SPARK-55278]: Stream scalar UDF rows to and from the worker.
        // scalastyle:off throwerror
        throw new NotImplementedError("doExecute() is not yet implemented.")
        // scalastyle:on throwerror
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): EvalExternalUDFExec =
    copy(child = newChild)
}
