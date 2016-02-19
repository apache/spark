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

import scala.collection.JavaConverters._

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{PythonFunction, PythonRunner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.types.{ObjectType, StructField, StructType}

/**
 * Helper functions for physical operators that work with user defined objects.
 */
trait ObjectOperator extends SparkPlan {
  def generateToObject(objExpr: Expression, inputSchema: Seq[Attribute]): InternalRow => Any = {
    val objectProjection = GenerateSafeProjection.generate(objExpr :: Nil, inputSchema)
    (i: InternalRow) => objectProjection(i).get(0, objExpr.dataType)
  }

  def generateToRow(serializer: Seq[Expression]): Any => InternalRow = {
    val outputProjection = if (serializer.head.dataType.isInstanceOf[ObjectType]) {
      GenerateSafeProjection.generate(serializer)
    } else {
      GenerateUnsafeProjection.generate(serializer)
    }
    val inputType = serializer.head.collect { case b: BoundReference => b.dataType }.head
    val outputRow = new SpecificMutableRow(inputType :: Nil)
    (o: Any) => {
      outputRow(0) = o
      outputProjection(outputRow)
    }
  }
}

/**
 * Applies the given function to each input row and encodes the result.
 */
case class MapPartitions(
    func: Iterator[Any] => Iterator[Any],
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with ObjectOperator {
  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = generateToObject(deserializer, child.output)
      val outputObject = generateToRow(serializer)
      func(iter.map(getObject)).map(outputObject)
    }
  }
}

case class PythonMapPartitions(
    func: PythonFunction,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  override def expressions: Seq[Expression] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val isChildPickled = EvaluatePython.isPickled(child.schema)
    val isOutputPickled = EvaluatePython.isPickled(schema)

    inputRDD.mapPartitions { iter =>
      val inputIterator = if (isChildPickled) {
        iter.map(_.getBinary(0))
      } else {
        EvaluatePython.registerPicklers()  // register pickler for Row
        val pickle = new Pickler

        // Input iterator to Python: input rows are grouped so we send them in batches to Python.
        iter.grouped(100).map { inputRows =>
          val toBePickled = inputRows.map { row =>
            EvaluatePython.toJava(row, child.schema)
          }.toArray
          pickle.dumps(toBePickled)
        }
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator =
        new PythonRunner(
          func.command,
          func.envVars,
          func.pythonIncludes,
          func.pythonExec,
          func.pythonVer,
          func.broadcastVars,
          func.accumulator,
          bufferSize,
          reuseWorker
        ).compute(inputIterator, context.partitionId(), context)

      val toUnsafe = UnsafeProjection.create(output, output)

      if (isOutputPickled) {
        val row = new GenericMutableRow(1)
        outputIterator.map { bytes =>
          row(0) = bytes
          toUnsafe(row)
        }
      } else {
        val unpickle = new Unpickler
        outputIterator.flatMap { pickedResult =>
          val unpickledBatch = unpickle.loads(pickedResult)
          unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
        }.map { result =>
          toUnsafe(EvaluatePython.fromJava(result, schema).asInstanceOf[InternalRow])
        }
      }
    }
  }
}

/**
 * Applies the given function to each input row, appending the encoded result at the end of the row.
 */
case class AppendColumns(
    func: Any => Any,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with ObjectOperator {

  override def output: Seq[Attribute] = child.output ++ serializer.map(_.toAttribute)

  private def newColumnSchema = serializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = generateToObject(deserializer, child.output)
      val combiner = GenerateUnsafeRowJoiner.create(child.schema, newColumnSchema)
      val outputObject = generateToRow(serializer)

      iter.map { row =>
        val newColumns = outputObject(func(getObject(row)))

        // This operates on the assumption that we always serialize the result...
        combiner.join(row.asInstanceOf[UnsafeRow], newColumns.asInstanceOf[UnsafeRow]): InternalRow
      }
    }
  }
}

case class PythonAppendColumns(
    func: PythonFunction,
    newColumns: Seq[Attribute],
    isFlat: Boolean,
    child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ newColumns

  override def expressions: Seq[Expression] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val newColumnSchema = newColumns.toStructType
    val isChildPickled = EvaluatePython.isPickled(child.schema)

    inputRDD.mapPartitionsInternal { iter =>
      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = new java.util.LinkedList[InternalRow]()

      val inputIterator = if (isChildPickled) {
        iter.map { row =>
          queue.add(row)
          row.getBinary(0)
        }
      } else {
        EvaluatePython.registerPicklers()  // register pickler for Row
        val pickle = new Pickler

        // Input iterator to Python: input rows are grouped so we send them in batches to Python.
        // For each row, add it to the queue.
        iter.grouped(100).map { inputRows =>
          val toBePickled = inputRows.map { row =>
            queue.add(row)
            EvaluatePython.toJava(row, child.schema)
          }.toArray
          pickle.dumps(toBePickled)
        }
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator =
        new PythonRunner(
          func.command,
          func.envVars,
          func.pythonIncludes,
          func.pythonExec,
          func.pythonVer,
          func.broadcastVars,
          func.accumulator,
          bufferSize,
          reuseWorker
        ).compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val toUnsafe = UnsafeProjection.create(newColumns, newColumns)
      val combiner = GenerateUnsafeRowJoiner.create(child.schema, newColumnSchema)

      val newData = outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
      }

      val newRows = if (isFlat) {
        val row = new GenericMutableRow(1)
        newData.map { key =>
          row(0) = EvaluatePython.fromJava(key, newColumns.head.dataType)
          toUnsafe(row)
        }
      } else {
        newData.map { key =>
          toUnsafe(EvaluatePython.fromJava(key, newColumnSchema).asInstanceOf[InternalRow])
        }
      }

      newRows.map { newRow =>
        combiner.join(queue.poll().asInstanceOf[UnsafeRow], newRow)
      }
    }
  }
}

/**
 * Groups the input rows together and calls the function with each group and an iterator containing
 * all elements in the group.  The result of this function is encoded and flattened before
 * being output.
 */
case class MapGroups(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    serializer: Seq[NamedExpression],
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    child: SparkPlan) extends UnaryNode with ObjectOperator {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)

      val getKey = generateToObject(keyDeserializer, groupingAttributes)
      val getValue = generateToObject(valueDeserializer, dataAttributes)
      val outputObject = generateToRow(serializer)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          getKey(key),
          rowIter.map(getValue))
        result.map(outputObject)
      }
    }
  }
}

case class PythonMapGroups(
    func: PythonFunction,
    groupingExprs: Seq[Expression],
    dataAttributes: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  override def expressions: Seq[Expression] = groupingExprs

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingExprs) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingExprs.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)

    val keySchema = StructType(groupingExprs.map(_.dataType).map(dt => StructField("k", dt)))
    val valueSchema = dataAttributes.toStructType
    val isValuePickled = EvaluatePython.isPickled(valueSchema)
    val isOutputPickled = EvaluatePython.isPickled(schema)

    inputRDD.mapPartitionsInternal { iter =>
      EvaluatePython.registerPicklers()  // register pickler for Row
      val pickle = new Pickler

      val getKey = UnsafeProjection.create(groupingExprs, child.output)
      val getValue: InternalRow => InternalRow = if (dataAttributes == child.output) {
        identity
      } else {
        UnsafeProjection.create(dataAttributes, child.output)
      }

      val inputIterator = iter.map { input =>
        val keyBytes = pickle.dumps(EvaluatePython.toJava(getKey(input), keySchema))
        val valueBytes = if (isValuePickled) {
          input.getBinary(0)
        } else {
          pickle.dumps(EvaluatePython.toJava(getValue(input), valueSchema))
        }
        keyBytes -> valueBytes
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator =
        new PythonRunner(
          func.command,
          func.envVars,
          func.pythonIncludes,
          func.pythonExec,
          func.pythonVer,
          func.broadcastVars,
          func.accumulator,
          bufferSize,
          reuseWorker
        ).compute(inputIterator, context.partitionId(), context)

      val toUnsafe = UnsafeProjection.create(output, output)

      if (isOutputPickled) {
        val row = new GenericMutableRow(1)
        outputIterator.map { bytes =>
          row(0) = bytes
          toUnsafe(row)
        }
      } else {
        val unpickle = new Unpickler
        outputIterator.flatMap { pickedResult =>
          val unpickledBatch = unpickle.loads(pickedResult)
          unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
        }.map { result =>
          toUnsafe(EvaluatePython.fromJava(result, schema).asInstanceOf[InternalRow])
        }
      }
    }
  }
}

/**
 * Co-groups the data from left and right children, and calls the function with each group and 2
 * iterators containing all elements in the group from left and right side.
 * The result of this function is encoded and flattened before being output.
 */
case class CoGroup(
    func: (Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    leftDeserializer: Expression,
    rightDeserializer: Expression,
    serializer: Seq[NamedExpression],
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with ObjectOperator {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    leftGroup.map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)

      val getKey = generateToObject(keyDeserializer, leftGroup)
      val getLeft = generateToObject(leftDeserializer, leftAttr)
      val getRight = generateToObject(rightDeserializer, rightAttr)
      val outputObject = generateToRow(serializer)

      new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup).flatMap {
        case (key, leftResult, rightResult) =>
          val result = func(
            getKey(key),
            leftResult.map(getLeft),
            rightResult.map(getRight))
          result.map(outputObject)
      }
    }
  }
}
