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

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import net.razorvine.pickle.{Pickler, Unpickler}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.{PythonBroadcast, PythonRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.{Accumulator, Logging => SparkLogging}

/**
 * A serialized version of a Python lambda function.  Suitable for use in a [[PythonRDD]].
 */
private[spark] case class PythonUDF(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
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
            case p: PythonUDF if p.fastEquals(udf) => evaluation.resultAttribute
          }.withNewChildren(newChildren))
      }
  }
}

object EvaluatePython {
  def apply(udf: PythonUDF, child: LogicalPlan) =
    new EvaluatePython(udf, child, AttributeReference("pythonUDF", udf.dataType)())

  /**
   * Helper for converting a Scala object to a java suitable for pyspark serialization.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: Row, struct: StructType) =>
      val fields = struct.fields.map(field => field.dataType)
      row.toSeq.zip(fields).map {
        case (obj, dataType) => toJava(obj, dataType)
      }.toArray

    case (seq: Seq[Any], array: ArrayType) =>
      seq.map(x => toJava(x, array.elementType)).asJava
    case (list: JList[_], array: ArrayType) =>
      list.map(x => toJava(x, array.elementType)).asJava
    case (arr, array: ArrayType) if arr.getClass.isArray =>
      arr.asInstanceOf[Array[Any]].map(x => toJava(x, array.elementType))

    case (obj: Map[_, _], mt: MapType) => obj.map {
      case (k, v) => (toJava(k, mt.keyType), toJava(v, mt.valueType))
    }.asJava

    case (ud, udt: UserDefinedType[_]) => toJava(udt.serialize(ud), udt.sqlType)

    // Pyrolite can handle Timestamp and Decimal
    case (other, _) => other
  }

  /**
   * Convert Row into Java Array (for pickled into Python)
   */
  def rowToArray(row: Row, fields: Seq[DataType]): Array[Any] = {
    // TODO: this is slow!
    row.toSeq.zip(fields).map {case (obj, dt) => toJava(obj, dt)}.toArray
  }

  // Converts value to the type specified by the data type.
  // Because Python does not have data types for TimestampType, FloatType, ShortType, and
  // ByteType, we need to explicitly convert values in columns of these data types to the desired
  // JVM data types.
  def fromJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    // TODO: We should check nullable
    case (null, _) => null

    case (c: java.util.List[_], ArrayType(elementType, _)) =>
      c.map { e => fromJava(e, elementType)}: Seq[Any]

    case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
      c.asInstanceOf[Array[_]].map(e => fromJava(e, elementType)): Seq[Any]

    case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) => c.map {
      case (key, value) => (fromJava(key, keyType), fromJava(value, valueType))
    }.toMap

    case (c, StructType(fields)) if c.getClass.isArray =>
      new GenericRow(c.asInstanceOf[Array[_]].zip(fields).map {
        case (e, f) => fromJava(e, f.dataType)
      }): Row

    case (c: java.util.Calendar, DateType) =>
      new java.sql.Date(c.getTime().getTime())

    case (c: java.util.Calendar, TimestampType) =>
      new java.sql.Timestamp(c.getTime().getTime())

    case (_, udt: UserDefinedType[_]) =>
      fromJava(obj, udt.sqlType)

    case (c: Int, ByteType) => c.toByte
    case (c: Long, ByteType) => c.toByte
    case (c: Int, ShortType) => c.toShort
    case (c: Long, ShortType) => c.toShort
    case (c: Long, IntegerType) => c.toInt
    case (c: Double, FloatType) => c.toFloat
    case (c, StringType) if !c.isInstanceOf[String] => c.toString

    case (c, _) => c
  }
}

/**
 * :: DeveloperApi ::
 * Evaluates a [[PythonUDF]], appending the result to the end of the input tuple.
 */
@DeveloperApi
case class EvaluatePython(
    udf: PythonUDF,
    child: LogicalPlan,
    resultAttribute: AttributeReference)
  extends logical.UnaryNode {

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
      val fields = udf.children.map(_.dataType)
      iter.grouped(1000).map { inputRows =>
        val toBePickled = inputRows.map { row =>
          EvaluatePython.rowToArray(currentRow(row), fields)
        }.toArray
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
      udf.broadcastVars,
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
        row(0) = EvaluatePython.fromJava(result, udf.dataType)
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
