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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import scala.collection.JavaConverters._
import scala.language.existentials

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.r._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.r.SQLUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, FunctionUtils, LogicalGroupState}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.python.BatchIterator
import org.apache.spark.sql.execution.r.ArrowRRunner
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.types._

/**
 * Physical version of `ObjectProducer`.
 */
trait ObjectProducerExec extends SparkPlan {
  // The attribute that reference to the single object field this operator outputs.
  protected def outputObjAttr: Attribute

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  def outputObjectType: DataType = outputObjAttr.dataType
}

/**
 * Physical version of `ObjectConsumer`.
 */
trait ObjectConsumerExec extends UnaryExecNode {
  assert(child.output.length == 1)

  // This operator always need all columns of its child, even it doesn't reference to.
  @transient
  override lazy val references: AttributeSet = child.outputSet

  def inputObjectType: DataType = child.output.head.dataType
}

/**
 * Takes the input row from child and turns it into object using the given deserializer expression.
 * The output of this operator is a single-field safe row containing the deserialized object.
 */
case class DeserializeToObjectExec(
    deserializer: Expression,
    outputObjAttr: Attribute,
    child: SparkPlan) extends UnaryExecNode with ObjectProducerExec with CodegenSupport {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val resultObj = BindReferences.bindReference(deserializer, child.output).genCode(ctx)
    consume(ctx, resultObj :: Nil)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val projection = GenerateSafeProjection.generate(deserializer :: Nil, child.output)
      projection.initialize(index)
      iter.map(projection)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeserializeToObjectExec =
    copy(child = newChild)
}

/**
 * Takes the input object from child and turns in into unsafe row using the given serializer
 * expression.  The output of its child must be a single-field row containing the input object.
 */
case class SerializeFromObjectExec(
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends ObjectConsumerExec with CodegenSupport {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val resultVars = serializer.map { expr =>
      BindReferences.bindReference[Expression](expr, child.output).genCode(ctx)
    }
    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val projection = UnsafeProjection.create(serializer)
      projection.initialize(index)
      iter.map(projection)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SerializeFromObjectExec =
    copy(child = newChild)
}

/**
 * Helper functions for physical operators that work with user defined objects.
 */
object ObjectOperator {
  def deserializeRowToObject(
      deserializer: Expression,
      inputSchema: Seq[Attribute]): InternalRow => Any = {
    val proj = GenerateSafeProjection.generate(deserializer :: Nil, inputSchema)
    (i: InternalRow) => proj(i).get(0, deserializer.dataType)
  }

  def deserializeRowToObject(deserializer: Expression): InternalRow => Any = {
    val proj = GenerateSafeProjection.generate(deserializer :: Nil)
    (i: InternalRow) => proj(i).get(0, deserializer.dataType)
  }

  def serializeObjectToRow(serializer: Seq[Expression]): Any => UnsafeRow = {
    val proj = GenerateUnsafeProjection.generate(serializer)
    val objType = serializer.head.collect { case b: BoundReference => b.dataType }.head
    val objRow = new SpecificInternalRow(objType :: Nil)
    (o: Any) => {
      objRow(0) = o
      proj(objRow)
    }
  }

  def wrapObjectToRow(objType: DataType): Any => InternalRow = {
    val outputRow = new SpecificInternalRow(objType :: Nil)
    (o: Any) => {
      outputRow(0) = o
      outputRow
    }
  }

  def unwrapObjectFromRow(objType: DataType): InternalRow => Any = {
    (i: InternalRow) => i.get(0, objType)
  }
}

/**
 * Applies the given function to input object iterator.
 * The output of its child must be a single-field row containing the input object.
 */
case class MapPartitionsExec(
    func: Iterator[Any] => Iterator[Any],
    outputObjAttr: Attribute,
    child: SparkPlan)
  extends ObjectConsumerExec with ObjectProducerExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)
      func(iter.map(getObject)).map(outputObject)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): MapPartitionsExec =
    copy(child = newChild)
}

/**
 * Similar with [[MapPartitionsExec]] and
 * [[org.apache.spark.sql.execution.r.MapPartitionsRWrapper]] but serializes and deserializes
 * input/output in Arrow format.
 *
 * This is somewhat similar with [[org.apache.spark.sql.execution.python.ArrowEvalPythonExec]]
 */
case class MapPartitionsInRWithArrowExec(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {
  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { inputIter =>
      val outputTypes = schema.map(_.dataType)

      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter =
        if (batchSize > 0) new BatchIterator(inputIter, batchSize) else Iterator(inputIter)

      val runner = new ArrowRRunner(func, packageNames, broadcastVars, inputSchema,
        SQLConf.get.sessionLocalTimeZone, RRunnerModes.DATAFRAME_DAPPLY)

      // The communication mechanism is as follows:
      //
      //    JVM side                           R side
      //
      // 1. Internal rows            --------> Arrow record batches
      // 2.                                    Converts each Arrow record batch to each R data frame
      // 3.                                    Combine R data frames into one R data frame
      // 4.                                    Computes R native function on the data frame
      // 5.                                    Converts the R data frame to Arrow record batches
      // 6. Columnar batches         <-------- Arrow record batches
      // 7. Each row from each batch
      //
      // Note that, unlike Python vectorization implementation, R side sends Arrow formatted
      // binary in a batch due to the limitation of R API. See also ARROW-4512.
      val columnarBatchIter = runner.compute(batchIter, -1)
      val outputProject = UnsafeProjection.create(output, output)
      columnarBatchIter.flatMap { batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(outputTypes == actualDataTypes, "Invalid schema from dapply(): " +
          s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
        batch.rowIterator.asScala
      }.map(outputProject)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): MapPartitionsInRWithArrowExec =
    copy(child = newChild)
}

/**
 * Applies the given function to each input object.
 * The output of its child must be a single-field row containing the input object.
 *
 * This operator is kind of a safe version of [[ProjectExec]], as its output is custom object,
 * we need to use safe row to contain it.
 */
case class MapElementsExec(
    func: AnyRef,
    outputObjAttr: Attribute,
    child: SparkPlan)
  extends ObjectConsumerExec with ObjectProducerExec with CodegenSupport {

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val (funcClass, funcName) = func match {
      case m: MapFunction[_, _] => classOf[MapFunction[_, _]] -> "call"
      case _ => FunctionUtils.getFunctionOneName(outputObjectType, child.output(0).dataType)
    }
    val funcObj = Literal.create(func, ObjectType(funcClass))
    val callFunc = Invoke(funcObj, funcName, outputObjectType, child.output, propagateNull = false)

    val result = BindReferences.bindReference(callFunc, child.output).genCode(ctx)

    consume(ctx, result :: Nil)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val callFunc: Any => Any = func match {
      case m: MapFunction[_, _] => i => m.asInstanceOf[MapFunction[Any, Any]].call(i)
      case _ => func.asInstanceOf[Any => Any]
    }

    child.execute().mapPartitionsInternal { iter =>
      val getObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)
      iter.map(row => outputObject(callFunc(getObject(row))))
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): MapElementsExec =
    copy(child = newChild)
}

/**
 * Applies the given function to each input row, appending the encoded result at the end of the row.
 */
case class AppendColumnsExec(
    func: Any => Any,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ serializer.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def newColumnSchema = serializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = ObjectOperator.deserializeRowToObject(deserializer, child.output)
      val combiner = GenerateUnsafeRowJoiner.create(child.schema, newColumnSchema)
      val outputObject = ObjectOperator.serializeObjectToRow(serializer)

      iter.map { row =>
        val newColumns = outputObject(func(getObject(row)))
        combiner.join(row.asInstanceOf[UnsafeRow], newColumns): InternalRow
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AppendColumnsExec =
    copy(child = newChild)
}

/**
 * An optimized version of [[AppendColumnsExec]], that can be executed
 * on deserialized object directly.
 */
case class AppendColumnsWithObjectExec(
    func: Any => Any,
    inputSerializer: Seq[NamedExpression],
    newColumnsSerializer: Seq[NamedExpression],
    child: SparkPlan) extends ObjectConsumerExec {

  override def output: Seq[Attribute] = (inputSerializer ++ newColumnsSerializer).map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def inputSchema = inputSerializer.map(_.toAttribute).toStructType
  private def newColumnSchema = newColumnsSerializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getChildObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputChildObject = ObjectOperator.serializeObjectToRow(inputSerializer)
      val outputNewColumnOjb = ObjectOperator.serializeObjectToRow(newColumnsSerializer)
      val combiner = GenerateUnsafeRowJoiner.create(inputSchema, newColumnSchema)

      iter.map { row =>
        val childObj = getChildObject(row)
        val newColumns = outputNewColumnOjb(func(childObj))
        combiner.join(outputChildObject(childObj), newColumns): InternalRow
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AppendColumnsWithObjectExec =
    copy(child = newChild)
}

/**
 * Groups the input rows together and calls the function with each group and an iterator containing
 * all elements in the group.  The result of this function is flattened before being output.
 */
case class MapGroupsExec(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    child: SparkPlan) extends UnaryExecNode with ObjectProducerExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)

      val getKey = ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
      val getValue = ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          getKey(key),
          rowIter.map(getValue))
        result.map(outputObject)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): MapGroupsExec =
    copy(child = newChild)
}

object MapGroupsExec {
  def apply(
      func: (Any, Iterator[Any], LogicalGroupState[Any]) => TraversableOnce[Any],
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      outputObjAttr: Attribute,
      timeoutConf: GroupStateTimeout,
      child: SparkPlan): MapGroupsExec = {
    val watermarkPresent = child.output.exists {
      case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
      case _ => false
    }
    val f = (key: Any, values: Iterator[Any]) => {
      func(key, values, GroupStateImpl.createForBatch(timeoutConf, watermarkPresent))
    }
    new MapGroupsExec(f, keyDeserializer, valueDeserializer,
      groupingAttributes, dataAttributes, outputObjAttr, child)
  }
}

/**
 * Groups the input rows together and calls the R function with each group and an iterator
 * containing all elements in the group.
 * The result of this function is flattened before being output.
 */
case class FlatMapGroupsInRExec(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    outputSchema: StructType,
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    child: SparkPlan) extends UnaryExecNode with ObjectProducerExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val isSerializedRData = outputSchema == SERIALIZED_R_DATA_SCHEMA
    val serializerForR = if (!isSerializedRData) {
      SerializationFormats.ROW
    } else {
      SerializationFormats.BYTE
    }

    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)
      val getKey = ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
      val getValue = ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)
      val runner = new RRunner[(Array[Byte], Iterator[Array[Byte]]), Array[Byte]](
        func, SerializationFormats.ROW, serializerForR, packageNames, broadcastVars,
        isDataFrame = true, colNames = inputSchema.fieldNames,
        mode = RRunnerModes.DATAFRAME_GAPPLY)

      val groupedRBytes = grouped.map { case (key, rowIter) =>
        val deserializedIter = rowIter.map(getValue)
        val newIter =
          deserializedIter.asInstanceOf[Iterator[Row]].map { row => rowToRBytes(row) }
        val newKey = rowToRBytes(getKey(key).asInstanceOf[Row])
        (newKey, newIter)
      }

      val outputIter = runner.compute(groupedRBytes, -1)
      if (!isSerializedRData) {
        val result = outputIter.map { bytes => bytesToRow(bytes, outputSchema) }
        result.map(outputObject)
      } else {
        val result = outputIter.map { bytes => Row.fromSeq(Seq(bytes)) }
        result.map(outputObject)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): FlatMapGroupsInRExec =
    copy(child = newChild)
}

/**
 * Similar with [[FlatMapGroupsInRExec]] but serializes and deserializes input/output in
 * Arrow format.
 * This is also somewhat similar with
 * [[org.apache.spark.sql.execution.python.FlatMapGroupsInPandasExec]].
 */
case class FlatMapGroupsInRWithArrowExec(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    output: Seq[Attribute],
    keyDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] =
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)
      val getKey = ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

      val keys = collection.mutable.ArrayBuffer.empty[Array[Byte]]
      val groupedByRKey: Iterator[Iterator[InternalRow]] =
        grouped.map { case (key, rowIter) =>
          keys.append(rowToRBytes(getKey(key).asInstanceOf[Row]))
          rowIter
        }

      val runner = new ArrowRRunner(func, packageNames, broadcastVars, inputSchema,
        SQLConf.get.sessionLocalTimeZone, RRunnerModes.DATAFRAME_GAPPLY) {
        protected override def bufferedWrite(
            dataOut: DataOutputStream)(writeFunc: ByteArrayOutputStream => Unit): Unit = {
          super.bufferedWrite(dataOut)(writeFunc)
          // Don't forget we're sending keys additionally.
          keys.foreach(dataOut.write)
        }
      }

      // The communication mechanism is as follows:
      //
      //    JVM side                           R side
      //
      // 1. Group internal rows
      // 2. Grouped internal rows    --------> Arrow record batches
      // 3. Grouped keys             --------> Regular serialized keys
      // 4.                                    Converts each Arrow record batch to each R data frame
      // 5.                                    Deserializes keys
      // 6.                                    Maps each key to each R Data frame
      // 7.                                    Computes R native function on each key/R data frame
      // 8.                                    Converts all R data frames to Arrow record batches
      // 9. Columnar batches         <-------- Arrow record batches
      // 10. Each row from each batch
      //
      // Note that, unlike Python vectorization implementation, R side sends Arrow formatted
      // binary in a batch due to the limitation of R API. See also ARROW-4512.
      val columnarBatchIter = runner.compute(groupedByRKey, -1)
      val outputProject = UnsafeProjection.create(output, output)
      val outputTypes = StructType.fromAttributes(output).map(_.dataType)

      columnarBatchIter.flatMap { batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(outputTypes == actualDataTypes, "Invalid schema from gapply(): " +
          s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
        batch.rowIterator().asScala
      }.map(outputProject)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): FlatMapGroupsInRWithArrowExec =
    copy(child = newChild)
}

/**
 * Co-groups the data from left and right children, and calls the function with each group and 2
 * iterators containing all elements in the group from left and right side.
 * The result of this function is flattened before being output.
 */
case class CoGroupExec(
    func: (Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    leftDeserializer: Expression,
    rightDeserializer: Expression,
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    outputObjAttr: Attribute,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with ObjectProducerExec {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    leftGroup.map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)

      val getKey = ObjectOperator.deserializeRowToObject(keyDeserializer, leftGroup)
      val getLeft = ObjectOperator.deserializeRowToObject(leftDeserializer, leftAttr)
      val getRight = ObjectOperator.deserializeRowToObject(rightDeserializer, rightAttr)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjectType)

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

  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, newRight: SparkPlan): CoGroupExec = copy(left = newLeft, right = newRight)
}
