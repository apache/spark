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

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, BoundReference, ColumnarUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{EvalColumnarUDFs, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.columnar.ColumnarScalarFunction
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.{DataType, StructType, UserDefinedType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

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
    val driverFunctions: Array[ColumnarScalarFunction] = udfs.map { udf =>
      udf.function.getOrElse {
        throw QueryExecutionErrors.columnarUDFNotExecutableError(
          udf.name, udf.engineImplementations.map(_.kind))
      }
    }.toArray
    // The closures of the nodes of a stage are serialized together, so the nodes would share one
    // instance of a function that several of them call, and an operator between them can read
    // its child from another thread. The functions are serialized apart here, so that each
    // partition of this node reads its own instances, which only this node calls.
    val serializedFunctions: Array[Byte] = Utils.serialize(driverFunctions)
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
    val inputTypeStrings: Array[String] =
      udfs.map(_.children.map(_.dataType.catalogString).mkString(", ")).toArray
    val resultTypes: Array[DataType] = udfs.map(_.dataType).toArray
    // A column vector has the SQL type of a user-defined type, so the type of a result is
    // compared with the declared type after the same replacement. CHAR and VARCHAR, which a
    // declared type has when they are first-class types, are compared as STRING, so that the
    // vector of such a result can be of either type.
    val resultVectorTypes: Array[DataType] = resultTypes.map(EvalColumnarUDFsExec.vectorType)
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val evalTime = longMetric("evalTime")
    val numInputColumns = child.output.length
    val numFunctions = udfs.length

    child.executeColumnar().mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {
        private val functions = Utils.deserialize[Array[ColumnarScalarFunction]](
          serializedFunctions, Utils.getContextOrSparkClassLoader)

        // The results of the current batch, which this node owns.
        private var results: Array[ColumnVector] = Array.empty

        private def closeResults(): Unit = {
          results.foreach(r => if (r != null) r.close())
          results = Array.empty
        }

        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => closeResults()))

        // Whether `vector` is `column` or a vector inside it. A writable vector, which the
        // vectorized readers and `RowToColumnarExec` produce, gives its children for every
        // type that has them: the fields of a struct, the elements of an array, the keys and
        // values of a map. Other vectors are only known to give the fields of a struct.
        private def isPartOf(column: ColumnVector, vector: ColumnVector): Boolean = {
          (column eq vector) || (column match {
            case w: WritableColumnVector =>
              (0 until w.getNumChildren).exists(i => isPartOf(w.getChild(i), vector))
            case _ => column.dataType() match {
              case s: StructType =>
                s.fields.indices.exists(i => isPartOf(column.getChild(i), vector))
              case _ => false
            }
          })
        }

        private def isInputColumn(batch: ColumnarBatch, vector: ColumnVector): Boolean = {
          var i = 0
          while (i < numInputColumns) {
            if (isPartOf(batch.column(i), vector)) return true
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
          val columns = new Array[ColumnVector](numInputColumns + numFunctions)
          var i = 0
          while (i < numInputColumns) {
            columns(i) = batch.column(i)
            i += 1
          }
          val start = System.nanoTime()
          val newResults = new Array[ColumnVector](numFunctions)
          results = newResults
          i = 0
          while (i < numFunctions) {
            val args = new ColumnarBatch(argOrdinals(i).map(batch.column), numRows)
            val result = try {
              functions(i).eval(args)
            } catch {
              case NonFatal(e) =>
                throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
                  names(i), inputTypeStrings(i), resultTypes(i).catalogString, e)
            }
            // This node closes the results, and the child still owns its columns: it can use
            // them again for its next batch. So a result that is a column of the child, or a
            // vector inside one, is not kept as a result, and is rejected.
            if (result != null && isInputColumn(batch, result)) {
              throw QueryExecutionErrors.columnarUDFResultIsArgumentError(names(i))
            }
            // Set before the checks, so that a vector that fails them is closed as well.
            newResults(i) = result
            if (result == null || !EvalColumnarUDFsExec.hasType(result, resultVectorTypes(i))) {
              throw QueryExecutionErrors.columnarUDFResultTypeMismatchError(
                names(i), Option(result).map(_.dataType()), resultTypes(i))
            }
            // A vector that is too short would be read past its end by the operators above.
            // Only a writable vector tells how many values it can hold.
            result match {
              case w: WritableColumnVector if w.getCapacity < numRows =>
                throw QueryExecutionErrors.columnarUDFResultTooShortError(
                  names(i), w.getCapacity, numRows)
              case _ =>
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

object EvalColumnarUDFsExec {

  /** The type that the vector of a result of the given type has. */
  private def vectorType(dataType: DataType): DataType = {
    CharVarcharUtils.replaceCharVarcharWithStringAlways(dataType.transformRecursively {
      case udt: UserDefinedType[_] => udt.sqlType
    })
  }

  private def hasType(result: ColumnVector, expected: DataType): Boolean = {
    DataType.equalsStructurally(result.dataType(), expected, true) ||
      DataType.equalsStructurally(vectorType(result.dataType()), expected, true)
  }
}
