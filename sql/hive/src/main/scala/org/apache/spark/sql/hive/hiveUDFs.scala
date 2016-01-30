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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.{ConstantObjectInspector, ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{analysis, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.sequenceOption
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.types._


private[hive] class HiveFunctionRegistry(
    underlying: analysis.FunctionRegistry,
    executionHive: HiveClientImpl)
  extends analysis.FunctionRegistry with HiveInspectors {

  def getFunctionInfo(name: String): FunctionInfo = {
    // Hive Registry need current database to lookup function
    // TODO: the current database of executionHive should be consistent with metadataHive
    executionHive.withHiveState {
      FunctionRegistry.getFunctionInfo(name)
    }
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    Try(underlying.lookupFunction(name, children)).getOrElse {
      // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
      // not always serializable.
      val functionInfo: FunctionInfo =
        Option(getFunctionInfo(name.toLowerCase)).getOrElse(
          throw new AnalysisException(s"undefined function $name"))

      val functionClassName = functionInfo.getFunctionClass.getName

      // When we instantiate hive UDF wrapper class, we may throw exception if the input expressions
      // don't satisfy the hive UDF, such as type mismatch, input number mismatch, etc. Here we
      // catch the exception and throw AnalysisException instead.
      try {
        if (classOf[GenericUDFMacro].isAssignableFrom(functionInfo.getFunctionClass)) {
          HiveGenericUDF(
            name, new HiveFunctionWrapper(functionClassName, functionInfo.getGenericUDF), children)
        } else if (classOf[UDF].isAssignableFrom(functionInfo.getFunctionClass)) {
          HiveSimpleUDF(name, new HiveFunctionWrapper(functionClassName), children)
        } else if (classOf[GenericUDF].isAssignableFrom(functionInfo.getFunctionClass)) {
          HiveGenericUDF(name, new HiveFunctionWrapper(functionClassName), children)
        } else if (
          classOf[AbstractGenericUDAFResolver].isAssignableFrom(functionInfo.getFunctionClass)) {
          HiveUDAFFunction(name, new HiveFunctionWrapper(functionClassName), children)
        } else if (classOf[UDAF].isAssignableFrom(functionInfo.getFunctionClass)) {
          HiveUDAFFunction(
            name, new HiveFunctionWrapper(functionClassName), children, isUDAFBridgeRequired = true)
        } else if (classOf[GenericUDTF].isAssignableFrom(functionInfo.getFunctionClass)) {
          val udtf = HiveGenericUDTF(name, new HiveFunctionWrapper(functionClassName), children)
          udtf.elementTypes // Force it to check input data types.
          udtf
        } else {
          throw new AnalysisException(s"No handler for udf ${functionInfo.getFunctionClass}")
        }
      } catch {
        case analysisException: AnalysisException =>
          // If the exception is an AnalysisException, just throw it.
          throw analysisException
        case throwable: Throwable =>
          // If there is any other error, we throw an AnalysisException.
          val errorMessage = s"No handler for Hive udf ${functionInfo.getFunctionClass} " +
            s"because: ${throwable.getMessage}."
          throw new AnalysisException(errorMessage)
      }
    }
  }

  override def registerFunction(name: String, info: ExpressionInfo, builder: FunctionBuilder)
  : Unit = underlying.registerFunction(name, info, builder)

  /* List all of the registered function names. */
  override def listFunction(): Seq[String] = {
    (FunctionRegistry.getFunctionNames.asScala ++ underlying.listFunction()).toList.sorted
  }

  /* Get the class of the registered function by specified name. */
  override def lookupFunction(name: String): Option[ExpressionInfo] = {
    underlying.lookupFunction(name).orElse(
    Try {
      val info = getFunctionInfo(name)
      val annotation = info.getFunctionClass.getAnnotation(classOf[Description])
      if (annotation != null) {
        Some(new ExpressionInfo(
          info.getFunctionClass.getCanonicalName,
          annotation.name(),
          annotation.value(),
          annotation.extended()))
      } else {
        Some(new ExpressionInfo(
          info.getFunctionClass.getCanonicalName,
          name,
          null,
          null))
      }
    }.getOrElse(None))
  }
}

private[hive] case class HiveSimpleUDF(
    name: String, funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression with HiveInspectors with CodegenFallback with Logging {

  override def deterministic: Boolean = isUDFDeterministic

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
    val udfType = function.getClass().getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic()
  }

  override def foldable: Boolean = isUDFDeterministic && children.forall(_.foldable)

  // Create parameter converters
  @transient
  private lazy val conversionHelper = new ConversionHelper(method, arguments)

  override val dataType = javaClassToDataType(method.getReturnType)

  @transient
  lazy val returnInspector = ObjectInspectorFactory.getReflectionObjectInspector(
    method.getGenericReturnType(), ObjectInspectorOptions.JAVA)

  @transient
  private lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  // TODO: Finish input output types.
  override def eval(input: InternalRow): Any = {
    val inputs = wrap(children.map(c => c.eval(input)), arguments, cached, inputDataTypes)
    val ret = FunctionRegistry.invoke(
      method,
      function,
      conversionHelper.convertIfNecessary(inputs : _*): _*)
    unwrap(ret, returnInspector)
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

// Adapter from Catalyst ExpressionResult to Hive DeferredObject
private[hive] class DeferredObjectAdapter(oi: ObjectInspector, dataType: DataType)
  extends DeferredObject with HiveInspectors {

  private var func: () => Any = _
  def set(func: () => Any): Unit = {
    this.func = func
  }
  override def prepare(i: Int): Unit = {}
  override def get(): AnyRef = wrap(func(), oi, dataType)
}

private[hive] case class HiveGenericUDF(
    name: String, funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression with HiveInspectors with CodegenFallback with Logging {

  override def nullable: Boolean = true

  override def deterministic: Boolean = isUDFDeterministic

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
  private lazy val isUDFDeterministic = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic()
  }

  @transient
  private lazy val deferedObjects = argumentInspectors.zip(children).map { case (inspect, child) =>
    new DeferredObjectAdapter(inspect, child.dataType)
  }.toArray[DeferredObject]

  override val dataType: DataType = inspectorToDataType(returnInspector)

  override def eval(input: InternalRow): Any = {
    returnInspector // Make sure initialized.

    var i = 0
    while (i < children.length) {
      val idx = i
      deferedObjects(i).asInstanceOf[DeferredObjectAdapter].set(
        () => {
          children(idx).eval(input)
        })
      i += 1
    }
    unwrap(function.evaluate(deferedObjects), returnInspector)
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/**
 * Converts a Hive Generic User Defined Table Generating Function (UDTF) to a
 * [[Generator]].  Note that the semantics of Generators do not allow
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
  extends Generator with HiveInspectors with CodegenFallback {

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

  override lazy val elementTypes = outputInspector.getAllStructFieldRefs.asScala.map {
    field => (inspectorToDataType(field.getFieldObjectInspector), true, field.getFieldName)
  }

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    outputInspector // Make sure initialized.

    val inputProjection = new InterpretedProjection(children)

    function.process(wrap(inputProjection(input), inputInspectors, udtInput, inputDataTypes))
    collector.collectRows()
  }

  protected class UDTFCollector extends Collector {
    var collected = new ArrayBuffer[InternalRow]

    override def collect(input: java.lang.Object) {
      // We need to clone the input here because implementations of
      // GenericUDTF reuse the same object. Luckily they are always an array, so
      // it is easy to clone.
      collected += unwrap(input, outputInspector).asInstanceOf[InternalRow]
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

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/**
 * Currently we don't support partial aggregation for queries using Hive UDAF, which may hurt
 * performance a lot.
 */
private[hive] case class HiveUDAFFunction(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    isUDAFBridgeRequired: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate with HiveInspectors {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  @transient
  private lazy val resolver =
    if (isUDAFBridgeRequired) {
      new GenericUDAFBridge(funcWrapper.createFunction[UDAF]())
    } else {
      funcWrapper.createFunction[AbstractGenericUDAFResolver]()
    }

  @transient
  private lazy val inspectors = children.map(toInspector).toArray

  @transient
  private lazy val functionAndInspector = {
    val parameterInfo = new SimpleGenericUDAFParameterInfo(inspectors, false, false)
    val f = resolver.getEvaluator(parameterInfo)
    f -> f.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)
  }

  @transient
  private lazy val function = functionAndInspector._1

  @transient
  private lazy val returnInspector = functionAndInspector._2

  @transient
  private[this] var buffer: GenericUDAFEvaluator.AggregationBuffer = _

  override def eval(input: InternalRow): Any = unwrap(function.evaluate(buffer), returnInspector)

  @transient
  private lazy val inputProjection = new InterpretedProjection(children)

  @transient
  private lazy val cached = new Array[AnyRef](children.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  // Hive UDAF has its own buffer, so we don't need to occupy a slot in the aggregation
  // buffer for it.
  override def aggBufferSchema: StructType = StructType(Nil)

  override def update(_buffer: MutableRow, input: InternalRow): Unit = {
    val inputs = inputProjection(input)
    function.iterate(buffer, wrap(inputs, inspectors, cached, inputDataTypes))
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "Hive UDAF doesn't support partial aggregate")
  }

  override def initialize(_buffer: MutableRow): Unit = {
    buffer = function.getNewAggregationBuffer
  }

  override val aggBufferAttributes: Seq[AttributeReference] = Nil

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] = Nil

  // We rely on Hive to check the input data types, so use `AnyDataType` here to bypass our
  // catalyst type checking framework.
  override def inputTypes: Seq[AbstractDataType] = children.map(_ => AnyDataType)

  override def nullable: Boolean = true

  override def supportsPartial: Boolean = false

  override val dataType: DataType = inspectorToDataType(returnInspector)

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else " "
    s"$name($distinct${children.map(_.sql).mkString(", ")})"
  }
}
