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

package org.apache.spark.sql.execution.columnar.udf

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, BoundReference, ColumnarUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{EvalColumnarUDFs, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLType
import org.apache.spark.sql.columnar.ColumnarScalarFunction
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/** Plans the evaluation of columnar user-defined functions. */
object ColumnarUDFEvals extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case EvalColumnarUDFs(udfs, resultAttrs, child) =>
      EvalColumnarUDFsExec(udfs, resultAttrs, planLater(child)) :: Nil
    case _ => Nil
  }
}

/**
 * Evaluates calls of columnar user-defined functions, one batch at a time. The input and the
 * output are columnar: an output batch is the columns of the input batch, as they are, followed
 * by one column per call. The arguments of the calls are columns of the child.
 *
 * This node runs the implementation of each function that Spark can run, its
 * [[ColumnarScalarFunction]]. An engine that replaces Spark's operators can replace this node
 * with one that runs another implementation, from `ColumnarUDF.engineImplementations`. A
 * function that Spark cannot run fails only when this node is executed, so that such an engine
 * has replaced the node by then.
 */
case class EvalColumnarUDFsExec(
    udfs: Seq[ColumnarUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "evalTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in functions"))

  override protected def doExecute(): RDD[InternalRow] = {
    throw SparkException.internalError(s"$nodeName only supports columnar execution.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val functions: Array[ColumnarScalarFunction] = udfs.map { udf =>
      udf.function.getOrElse {
        throw new SparkUnsupportedOperationException(
          errorClass = "COLUMNAR_UDF_NOT_EXECUTABLE",
          messageParameters = Map(
            "name" -> udf.name,
            "kinds" -> udf.engineImplementations.map(_.kind).mkString("[", ", ", "]")))
      }
    }.toArray
    // The ordinals, in the input batch, of the arguments of each call.
    val argOrdinals: Array[Array[Int]] = udfs.map { udf =>
      udf.children.map { arg =>
        BindReferences.bindReference(arg, child.output) match {
          case b: BoundReference => b.ordinal
          case other => throw SparkException.internalError(
            s"The argument $other of ${udf.name} is not a column of the child.")
        }
      }.toArray
    }.toArray
    val names: Array[String] = udfs.map(_.name).toArray
    val resultTypes: Array[DataType] = udfs.map(_.dataType).toArray
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val evalTime = longMetric("evalTime")
    val numInputColumns = child.output.length

    child.executeColumnar().mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {
        // The results of the current batch, which this node owns.
        private var results: Array[ColumnVector] = Array.empty

        private def closeResults(): Unit = {
          results.foreach(r => if (r != null) r.close())
          results = Array.empty
        }

        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => closeResults()))

        private def isInputColumn(batch: ColumnarBatch, vector: ColumnVector): Boolean = {
          var i = 0
          while (i < numInputColumns) {
            if (batch.column(i) eq vector) return true
            i += 1
          }
          false
        }

        override def hasNext: Boolean = {
          val more = batches.hasNext
          if (!more) closeResults()
          more
        }

        override def next(): ColumnarBatch = {
          closeResults()
          val batch = batches.next()
          val numRows = batch.numRows()
          val columns = new Array[ColumnVector](numInputColumns + functions.length)
          var i = 0
          while (i < numInputColumns) {
            columns(i) = batch.column(i)
            i += 1
          }
          val start = System.nanoTime()
          val newResults = new Array[ColumnVector](functions.length)
          results = newResults
          i = 0
          while (i < functions.length) {
            val args = new ColumnarBatch(argOrdinals(i).map(batch.column), numRows)
            val result = functions(i).eval(args)
            // This node closes the results, and the child still owns its columns: it can use
            // them again for its next batch. So a result that is a column of the child is not
            // kept as a result, and is rejected.
            if (result != null && isInputColumn(batch, result)) {
              throw new SparkRuntimeException(
                errorClass = "COLUMNAR_UDF_RESULT_IS_ARGUMENT",
                messageParameters = Map("name" -> names(i)))
            }
            // Set before the check, so that a vector of the wrong type is closed as well.
            newResults(i) = result
            if (result == null ||
                !DataType.equalsStructurally(result.dataType(), resultTypes(i), true)) {
              val actualType = if (result == null) "NULL" else toSQLType(result.dataType())
              throw new SparkRuntimeException(
                errorClass = "COLUMNAR_UDF_RESULT_TYPE_MISMATCH",
                messageParameters = Map(
                  "name" -> names(i),
                  "actualType" -> actualType,
                  "expectedType" -> toSQLType(resultTypes(i))))
            }
            columns(numInputColumns + i) = newResults(i)
            i += 1
          }
          evalTime += System.nanoTime() - start
          numOutputRows += numRows
          numOutputBatches += 1
          new ColumnarBatch(columns, numRows)
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): EvalColumnarUDFsExec =
    copy(child = newChild)
}
