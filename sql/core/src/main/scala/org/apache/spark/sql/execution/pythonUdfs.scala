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

import java.util.{List => JList, Map => JMap}

import net.razorvine.pickle.{Pickler, Unpickler}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.{Accumulator, Logging => SparkLogging}

import scala.collection.JavaConversions._

/**
 * A serialized version of a Python lambda function.  Suitable for use in a [[PythonRDD]].
 */
private[spark] case class PythonUDF(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    accumulator: Accumulator[JList[Array[Byte]]],
    dataType: DataType,
    children: Seq[Expression]) extends Expression with SparkLogging {

  override def toString = s"PythonUDF#$name(${children.mkString(",")})"

  def nullable: Boolean = true

  override def eval(input: Row) = sys.error("PythonUDFs can not be directly evaluated.")
}

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
private[spark] object ExtractPythonUdfs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan) = plan transform {
    // Skip EvaluatePython nodes.
    case p: EvaluatePython => p

    case l: LogicalPlan =>
      // Extract any PythonUDFs from the current operator.
      val udfs = l.expressions.flatMap(_.collect { case udf: PythonUDF => udf})
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        l
      } else {
        // Pick the UDF we are going to evaluate (TODO: Support evaluating multiple UDFs at a time)
        // If there is more than one, we will add another evaluation operator in a subsequent pass.
        val udf = udfs.head

        var evaluation: EvaluatePython = null

        // Rewrite the child that has the input required for the UDF
        val newChildren = l.children.map { child =>
          // Check to make sure that the UDF can be evaluated with only the input of this child.
          // Other cases are disallowed as they are ambiguous or would require a cartisian product.
          if (udf.references.subsetOf(child.outputSet)) {
            evaluation = EvaluatePython(udf, child)
            evaluation
          } else if (udf.references.intersect(child.outputSet).nonEmpty) {
            sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
          } else {
            child
          }
        }

        assert(evaluation != null, "Unable to evaluate PythonUDF.  Missing input attributes.")

        // Trim away the new UDF value if it was only used for filtering or something.
        logical.Project(
          l.output,
          l.transformExpressions {
            case p: PythonUDF if p.id == udf.id => evaluation.resultAttribute
          }.withNewChildren(newChildren))
      }
  }
}

/**
 * :: DeveloperApi ::
 * Evaluates a [[PythonUDF]], appending the result to the end of the input tuple.
 */
@DeveloperApi
case class EvaluatePython(udf: PythonUDF, child: LogicalPlan) extends logical.UnaryNode {
  val resultAttribute = AttributeReference("pythonUDF", udf.dataType, nullable=true)()

  def output = child.output :+ resultAttribute
}

/**
 * :: DeveloperApi ::
 * Uses PythonRDD to evaluate a [[PythonUDF]], one partition of tuples at a time.  The input
 * data is cached and zipped with the result of the udf evaluation.
 */
@DeveloperApi
case class BatchPythonEvaluation(udf: PythonUDF, output: Seq[Attribute], child: SparkPlan)
  extends SparkPlan {
  def children = child :: Nil

  def execute() = {
    // TODO: Clean up after ourselves?
    val childResults = child.execute().map(_.copy()).cache()

    val parent = childResults.mapPartitions { iter =>
      val pickle = new Pickler
      val currentRow = newMutableProjection(udf.children, child.output)()
      iter.grouped(1000).map { inputRows =>
        val toBePickled = inputRows.map(currentRow(_).toArray).toArray
        pickle.dumps(toBePickled)
      }
    }

    val pyRDD = new PythonRDD(
      parent,
      udf.command,
      udf.envVars,
      udf.pythonIncludes,
      false,
      udf.pythonExec,
      Seq[Broadcast[Array[Byte]]](),
      udf.accumulator
    ).mapPartitions { iter =>
      val pickle = new Unpickler
      iter.flatMap { pickedResult =>
        val unpickledBatch = pickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]]
      }
    }.mapPartitions { iter =>
      val row = new GenericMutableRow(1)
      iter.map { result =>
        row(0) = udf.dataType match {
          case StringType => result.toString
          case other => result
        }
        row: Row
      }
    }

    childResults.zip(pyRDD).mapPartitions { iter =>
      val joinedRow = new JoinedRow()
      iter.map {
        case (row, udfResult) =>
          joinedRow(row, udfResult)
      }
    }
  }
}
