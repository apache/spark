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

package org.apache.spark.sql.hive

import java.lang.{Boolean => JBoolean}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.{ConstantObjectInspector, ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[hive] case class HiveSimpleUDF(
    name: String, funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression
  with HiveInspectors
  with CodegenFallback
  with Logging
  with UserDefinedExpression {

  override lazy val deterministic: Boolean = isUDFDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = true

  @transient
  lazy val function = funcWrapper.createFunction[UDF]()

  @transient
  private lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo).asJava)

  @transient
  private lazy val arguments = children.map(toInspector).toArray

  @transient
  private lazy val isUDFDeterministic = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic() && !udfType.stateful()
  }

  override def foldable: Boolean = isUDFDeterministic && children.forall(_.foldable)

  // Create parameter converters
  @transient
  private lazy val conversionHelper = new ConversionHelper(method, arguments)

  override lazy val dataType = javaTypeToDataType(method.getGenericReturnType)

  @transient
  private lazy val wrappers = children.map(x => wrapperFor(toInspector(x), x.dataType)).toArray

  @transient
  lazy val unwrapper = unwrapperFor(ObjectInspectorFactory.getReflectionObjectInspector(
    method.getGenericReturnType, ObjectInspectorOptions.JAVA))

  @transient
  private lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  // TODO: Finish input output types.
  override def eval(input: InternalRow): Any = {
    val inputs = wrap(children.map(_.eval(input)), wrappers, cached, inputDataTypes)
    val ret = FunctionRegistry.invoke(
      method,
      function,
      conversionHelper.convertIfNecessary(inputs : _*): _*)
    unwrapper(ret)
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

// Adapter from Catalyst ExpressionResult to Hive DeferredObject
private[hive] class DeferredObjectAdapter(oi: ObjectInspector, dataType: DataType)
  extends DeferredObject with HiveInspectors {

  private val wrapper = wrapperFor(oi, dataType)
  private var func: () => Any = _
  def set(func: () => Any): Unit = {
    this.func = func
  }
  override def prepare(i: Int): Unit = {}
  override def get(): AnyRef = wrapper(func()).asInstanceOf[AnyRef]
}

private[hive] case class HiveGenericUDF(
    name: String, funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression
  with HiveInspectors
  with CodegenFallback
  with Logging
  with UserDefinedExpression {

  override def nullable: Boolean = true

  override lazy val deterministic: Boolean = isUDFDeterministic && children.forall(_.deterministic)

  override def foldable: Boolean =
    isUDFDeterministic && returnInspector.isInstanceOf[ConstantObjectInspector]

  @transient
  lazy val function = funcWrapper.createFunction[GenericUDF]()

  @transient
  private lazy val argumentInspectors = children.map(toInspector)

  @transient
  private lazy val returnInspector = {
    function.initializeAndFoldConstants(argumentInspectors.toArray)
  }

  @transient
  private lazy val unwrapper = unwrapperFor(returnInspector)

  @transient
  private lazy val isUDFDeterministic = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic() && !udfType.stateful()
  }

  @transient
  private lazy val deferredObjects = argumentInspectors.zip(children).map { case (inspect, child) =>
    new DeferredObjectAdapter(inspect, child.dataType)
  }.toArray[DeferredObject]

  override lazy val dataType: DataType = inspectorToDataType(returnInspector)

  override def eval(input: InternalRow): Any = {
    returnInspector // Make sure initialized.

    var i = 0
    val length = children.length
    while (i < length) {
      val idx = i
      deferredObjects(i).asInstanceOf[DeferredObjectAdapter]
        .set(() => children(idx).eval(input))
      i += 1
    }
    unwrapper(function.evaluate(deferredObjects))
  }

  override def prettyName: String = name

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }
}

/**
 * Converts a Hive Generic User Defined Table Generating Function (UDTF) to a
 * `Generator`. Note that the semantics of Generators do not allow
 * Generators to maintain state in between input rows.  Thus UDTFs that rely on partitioning
 * dependent operations like calls to `close()` before producing output will not operate the same as
 * in Hive.  However, in practice this should not affect compatibility for most sane UDTFs
 * (e.g. explode or GenericUDTFParseUrlTuple).
 *
 * Operators that require maintaining state in between input rows should instead be implemented as
 * user defined aggregations, which have clean semantics even in a partitioned execution.
 */
private[hive] case class HiveGenericUDTF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression])
  extends Generator with HiveInspectors with CodegenFallback with UserDefinedExpression {

  @transient
  protected lazy val function: GenericUDTF = {
    val fun: GenericUDTF = funcWrapper.createFunction()
    fun.setCollector(collector)
    fun
  }

  @transient
  protected lazy val inputInspectors = children.map(toInspector)

  @transient
  protected lazy val outputInspector = function.initialize(inputInspectors.toArray)

  @transient
  protected lazy val udtInput = new Array[AnyRef](children.length)

  @transient
  protected lazy val collector = new UDTFCollector

  override lazy val elementSchema = StructType(outputInspector.getAllStructFieldRefs.asScala.map {
    field => StructField(field.getFieldName, inspectorToDataType(field.getFieldObjectInspector),
      nullable = true)
  })

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  @transient
  private lazy val wrappers = children.map(x => wrapperFor(toInspector(x), x.dataType)).toArray

  @transient
  private lazy val unwrapper = unwrapperFor(outputInspector)

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    outputInspector // Make sure initialized.

    val inputProjection = new InterpretedProjection(children)

    function.process(wrap(inputProjection(input), wrappers, udtInput, inputDataTypes))
    collector.collectRows()
  }

  protected class UDTFCollector extends Collector {
    var collected = new ArrayBuffer[InternalRow]

    override def collect(input: java.lang.Object) {
      // We need to clone the input here because implementations of
      // GenericUDTF reuse the same object. Luckily they are always an array, so
      // it is easy to clone.
      collected += unwrapper(input).asInstanceOf[InternalRow]
    }

    def collectRows(): Seq[InternalRow] = {
      val toCollect = collected
      collected = new ArrayBuffer[InternalRow]
      toCollect
    }
  }

  override def terminate(): TraversableOnce[InternalRow] = {
    outputInspector // Make sure initialized.
    function.close()
    collector.collectRows()
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name
}

/**
 * While being evaluated by Spark SQL, the aggregation state of a Hive UDAF may be in the following
 * three formats:
 *
 *  1. An instance of some concrete `GenericUDAFEvaluator.AggregationBuffer` class
 *
 *     This is the native Hive representation of an aggregation state. Hive `GenericUDAFEvaluator`
 *     methods like `iterate()`, `merge()`, `terminatePartial()`, and `terminate()` use this format.
 *     We call these methods to evaluate Hive UDAFs.
 *
 *  2. A Java object that can be inspected using the `ObjectInspector` returned by the
 *     `GenericUDAFEvaluator.init()` method.
 *
 *     Hive uses this format to produce a serializable aggregation state so that it can shuffle
 *     partial aggregation results. Whenever we need to convert a Hive `AggregationBuffer` instance
 *     into a Spark SQL value, we have to convert it to this format first and then do the conversion
 *     with the help of `ObjectInspector`s.
 *
 *  3. A Spark SQL value
 *
 *     We use this format for serializing Hive UDAF aggregation states on Spark side. To be more
 *     specific, we convert `AggregationBuffer`s into equivalent Spark SQL values, write them into
 *     `UnsafeRow`s, and then retrieve the byte array behind those `UnsafeRow`s as serialization
 *     results.
 *
 * We may use the following methods to convert the aggregation state back and forth:
 *
 *  - `wrap()`/`wrapperFor()`: from 3 to 1
 *  - `unwrap()`/`unwrapperFor()`: from 1 to 3
 *  - `GenericUDAFEvaluator.terminatePartial()`: from 2 to 3
 *
 *  Note that, Hive UDAF is initialized with aggregate mode, and some specific Hive UDAFs can't
 *  mix UPDATE and MERGE actions during its life cycle. However, Spark may do UPDATE on a UDAF and
 *  then do MERGE, in case of hash aggregate falling back to sort aggregate. To work around this
 *  issue, we track the ability to do MERGE in the Hive UDAF aggregate buffer. If Spark does
 *  UPDATE then MERGE, we can detect it and re-create the aggregate buffer with a different
 *  aggregate mode.
 */
private[hive] case class HiveUDAFFunction(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    isUDAFBridgeRequired: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HiveUDAFBuffer]
  with HiveInspectors
  with UserDefinedExpression {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  // Hive `ObjectInspector`s for all child expressions (input parameters of the function).
  @transient
  private lazy val inputInspectors = children.map(toInspector).toArray

  // Spark SQL data types of input parameters.
  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  private def newEvaluator(): GenericUDAFEvaluator = {
    val resolver = if (isUDAFBridgeRequired) {
      new GenericUDAFBridge(funcWrapper.createFunction[UDAF]())
    } else {
      funcWrapper.createFunction[AbstractGenericUDAFResolver]()
    }

    val clazz = Utils.classForName(classOf[SimpleGenericUDAFParameterInfo].getName)
    if (HiveUtils.isHive23) {
      val ctor = clazz.getDeclaredConstructor(
        classOf[Array[ObjectInspector]], JBoolean.TYPE, JBoolean.TYPE, JBoolean.TYPE)
      val args = Array[AnyRef](inputInspectors, JBoolean.FALSE, JBoolean.FALSE, JBoolean.FALSE)
      val parameterInfo = ctor.newInstance(args: _*).asInstanceOf[SimpleGenericUDAFParameterInfo]
      resolver.getEvaluator(parameterInfo)
    } else {
      val ctor = clazz.getDeclaredConstructor(
        classOf[Array[ObjectInspector]], JBoolean.TYPE, JBoolean.TYPE)
      val args = Array[AnyRef](inputInspectors, JBoolean.FALSE, JBoolean.FALSE)
      val parameterInfo = ctor.newInstance(args: _*).asInstanceOf[SimpleGenericUDAFParameterInfo]
      resolver.getEvaluator(parameterInfo)
    }
  }

  private case class HiveEvaluator(
      evaluator: GenericUDAFEvaluator,
      objectInspector: ObjectInspector)

  // The UDAF evaluator used to consume raw input rows and produce partial aggregation results.
  // Hive `ObjectInspector` used to inspect partial aggregation results.
  @transient
  private lazy val partial1HiveEvaluator = {
    val evaluator = newEvaluator()
    HiveEvaluator(evaluator, evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputInspectors))
  }

  // The UDAF evaluator used to consume partial aggregation results and produce final results.
  // Hive `ObjectInspector` used to inspect final results.
  @transient
  private lazy val finalHiveEvaluator = {
    val evaluator = newEvaluator()
    HiveEvaluator(
      evaluator,
      evaluator.init(GenericUDAFEvaluator.Mode.FINAL, Array(partial1HiveEvaluator.objectInspector)))
  }

  // Spark SQL data type of partial aggregation results
  @transient
  private lazy val partialResultDataType =
    inspectorToDataType(partial1HiveEvaluator.objectInspector)

  // Wrapper functions used to wrap Spark SQL input arguments into Hive specific format.
  @transient
  private lazy val inputWrappers = children.map(x => wrapperFor(toInspector(x), x.dataType)).toArray

  // Unwrapper function used to unwrap final aggregation result objects returned by Hive UDAFs into
  // Spark SQL specific format.
  @transient
  private lazy val resultUnwrapper = unwrapperFor(finalHiveEvaluator.objectInspector)

  @transient
  private lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  @transient
  private lazy val aggBufferSerDe: AggregationBufferSerDe = new AggregationBufferSerDe

  override def nullable: Boolean = true

  override lazy val dataType: DataType = inspectorToDataType(finalHiveEvaluator.objectInspector)

  override def prettyName: String = name

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else " "
    s"$name($distinct${children.map(_.sql).mkString(", ")})"
  }

  // The hive UDAF may create different buffers to handle different inputs: original data or
  // aggregate buffer. However, the Spark UDAF framework does not expose this information when
  // creating the buffer. Here we return null, and create the buffer in `update` and `merge`
  // on demand, so that we can know what input we are dealing with.
  override def createAggregationBuffer(): HiveUDAFBuffer = null

  @transient
  private lazy val inputProjection = UnsafeProjection.create(children)

  override def update(buffer: HiveUDAFBuffer, input: InternalRow): HiveUDAFBuffer = {
    // The input is original data, we create buffer with the partial1 evaluator.
    val nonNullBuffer = if (buffer == null) {
      HiveUDAFBuffer(partial1HiveEvaluator.evaluator.getNewAggregationBuffer, false)
    } else {
      buffer
    }

    assert(!nonNullBuffer.canDoMerge, "can not call `merge` then `update` on a Hive UDAF.")

    partial1HiveEvaluator.evaluator.iterate(
      nonNullBuffer.buf, wrap(inputProjection(input), inputWrappers, cached, inputDataTypes))
    nonNullBuffer
  }

  override def merge(buffer: HiveUDAFBuffer, input: HiveUDAFBuffer): HiveUDAFBuffer = {
    // The input is aggregate buffer, we create buffer with the final evaluator.
    val nonNullBuffer = if (buffer == null) {
      HiveUDAFBuffer(finalHiveEvaluator.evaluator.getNewAggregationBuffer, true)
    } else {
      buffer
    }

    // It's possible that we've called `update` of this Hive UDAF, and some specific Hive UDAF
    // implementation can't mix the `update` and `merge` calls during its life cycle. To work
    // around it, here we create a fresh buffer with final evaluator, and merge the existing buffer
    // to it, and replace the existing buffer with it.
    val mergeableBuf = if (!nonNullBuffer.canDoMerge) {
      val newBuf = finalHiveEvaluator.evaluator.getNewAggregationBuffer
      finalHiveEvaluator.evaluator.merge(
        newBuf, partial1HiveEvaluator.evaluator.terminatePartial(nonNullBuffer.buf))
      HiveUDAFBuffer(newBuf, true)
    } else {
      nonNullBuffer
    }

    // The 2nd argument of the Hive `GenericUDAFEvaluator.merge()` method is an input aggregation
    // buffer in the 3rd format mentioned in the ScalaDoc of this class. Originally, Hive converts
    // this `AggregationBuffer`s into this format before shuffling partial aggregation results, and
    // calls `GenericUDAFEvaluator.terminatePartial()` to do the conversion.
    finalHiveEvaluator.evaluator.merge(
      mergeableBuf.buf, partial1HiveEvaluator.evaluator.terminatePartial(input.buf))
    mergeableBuf
  }

  override def eval(buffer: HiveUDAFBuffer): Any = {
    resultUnwrapper(finalHiveEvaluator.evaluator.terminate(buffer.buf))
  }

  override def serialize(buffer: HiveUDAFBuffer): Array[Byte] = {
    // Serializes an `AggregationBuffer` that holds partial aggregation results so that we can
    // shuffle it for global aggregation later.
    aggBufferSerDe.serialize(buffer.buf)
  }

  override def deserialize(bytes: Array[Byte]): HiveUDAFBuffer = {
    // Deserializes an `AggregationBuffer` from the shuffled partial aggregation phase to prepare
    // for global aggregation by merging multiple partial aggregation results within a single group.
    HiveUDAFBuffer(aggBufferSerDe.deserialize(bytes), false)
  }

  // Helper class used to de/serialize Hive UDAF `AggregationBuffer` objects
  private class AggregationBufferSerDe {
    private val partialResultUnwrapper = unwrapperFor(partial1HiveEvaluator.objectInspector)

    private val partialResultWrapper =
      wrapperFor(partial1HiveEvaluator.objectInspector, partialResultDataType)

    private val projection = UnsafeProjection.create(Array(partialResultDataType))

    private val mutableRow = new GenericInternalRow(1)

    def serialize(buffer: AggregationBuffer): Array[Byte] = {
      // The buffer may be null if there is no input. It's unclear if the hive UDAF accepts null
      // buffer, for safety we create an empty buffer here.
      val nonNullBuffer = if (buffer == null) {
        partial1HiveEvaluator.evaluator.getNewAggregationBuffer
      } else {
        buffer
      }

      // `GenericUDAFEvaluator.terminatePartial()` converts an `AggregationBuffer` into an object
      // that can be inspected by the `ObjectInspector` returned by `GenericUDAFEvaluator.init()`.
      // Then we can unwrap it to a Spark SQL value.
      mutableRow.update(0, partialResultUnwrapper(
        partial1HiveEvaluator.evaluator.terminatePartial(nonNullBuffer)))
      val unsafeRow = projection(mutableRow)
      val bytes = ByteBuffer.allocate(unsafeRow.getSizeInBytes)
      unsafeRow.writeTo(bytes)
      bytes.array()
    }

    def deserialize(bytes: Array[Byte]): AggregationBuffer = {
      // `GenericUDAFEvaluator` doesn't provide any method that is capable to convert an object
      // returned by `GenericUDAFEvaluator.terminatePartial()` back to an `AggregationBuffer`. The
      // workaround here is creating an initial `AggregationBuffer` first and then merge the
      // deserialized object into the buffer.
      val buffer = finalHiveEvaluator.evaluator.getNewAggregationBuffer
      val unsafeRow = new UnsafeRow(1)
      unsafeRow.pointTo(bytes, bytes.length)
      val partialResult = unsafeRow.get(0, partialResultDataType)
      finalHiveEvaluator.evaluator.merge(buffer, partialResultWrapper(partialResult))
      buffer
    }
  }
}

case class HiveUDAFBuffer(buf: AggregationBuffer, canDoMerge: Boolean)
