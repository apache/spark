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

import scala.language.existentials

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.r._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.r.SQLUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.FunctionUtils
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalGroupState
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


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
  override def references: AttributeSet = child.outputSet

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
    val bound = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(deserializer, child.output))
    ctx.currentVars = input
    val resultVars = bound.genCode(ctx) :: Nil
    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val projection = GenerateSafeProjection.generate(deserializer :: Nil, child.output)
      projection.initialize(index)
      iter.map(projection)
    }
  }
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
    val bound = serializer.map { expr =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(expr, child.output))
    }
    ctx.currentVars = input
    val resultVars = bound.map(_.genCode(ctx))
    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val projection = UnsafeProjection.create(serializer)
      projection.initialize(index)
      iter.map(projection)
    }
  }
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
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)
      func(iter.map(getObject)).map(outputObject)
    }
  }
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
    val (funcClass, methodName) = func match {
      case m: MapFunction[_, _] => classOf[MapFunction[_, _]] -> "call"
      case _ => FunctionUtils.getFunctionOneName(outputObjAttr.dataType, child.output(0).dataType)
    }
    val funcObj = Literal.create(func, ObjectType(funcClass))
    val callFunc = Invoke(funcObj, methodName, outputObjAttr.dataType, child.output)

    val bound = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(callFunc, child.output))
    ctx.currentVars = input
    val resultVars = bound.genCode(ctx) :: Nil

    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val callFunc: Any => Any = func match {
      case m: MapFunction[_, _] => i => m.asInstanceOf[MapFunction[Any, Any]].call(i)
      case _ => func.asInstanceOf[Any => Any]
    }

    child.execute().mapPartitionsInternal { iter =>
      val getObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)
      iter.map(row => outputObject(callFunc(getObject(row))))
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
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
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          getKey(key),
          rowIter.map(getValue))
        result.map(outputObject)
      }
    }
  }
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
    val f = (key: Any, values: Iterator[Any]) => {
      func(key, values, GroupStateImpl.createForBatch(timeoutConf))
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

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val isSerializedRData =
      if (outputSchema == SERIALIZED_R_DATA_SCHEMA) true else false
    val serializerForR = if (!isSerializedRData) {
      SerializationFormats.ROW
    } else {
      SerializationFormats.BYTE
    }

    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)
      val getKey = ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
      val getValue = ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)
      val runner = new RRunner[Array[Byte]](
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
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

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
