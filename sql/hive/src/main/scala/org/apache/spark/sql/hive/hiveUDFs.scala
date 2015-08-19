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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ConstantObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types._


private[hive] class HiveFunctionRegistry(underlying: analysis.FunctionRegistry)
  extends analysis.FunctionRegistry with HiveInspectors {

  def getFunctionInfo(name: String): FunctionInfo = FunctionRegistry.getFunctionInfo(name)

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    Try(underlying.lookupFunction(name, children)).getOrElse {
      // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
      // not always serializable.
      val functionInfo: FunctionInfo =
        Option(FunctionRegistry.getFunctionInfo(name.toLowerCase)).getOrElse(
          throw new AnalysisException(s"undefined function $name"))

      val functionClassName = functionInfo.getFunctionClass.getName

      if (classOf[UDF].isAssignableFrom(functionInfo.getFunctionClass)) {
        HiveSimpleUDF(new HiveFunctionWrapper(functionClassName), children)
      } else if (classOf[GenericUDF].isAssignableFrom(functionInfo.getFunctionClass)) {
        HiveGenericUDF(new HiveFunctionWrapper(functionClassName), children)
      } else if (
        classOf[AbstractGenericUDAFResolver].isAssignableFrom(functionInfo.getFunctionClass)) {
        HiveGenericUDAF(new HiveFunctionWrapper(functionClassName), children)
      } else if (classOf[UDAF].isAssignableFrom(functionInfo.getFunctionClass)) {
        HiveUDAF(new HiveFunctionWrapper(functionClassName), children)
      } else if (classOf[GenericUDTF].isAssignableFrom(functionInfo.getFunctionClass)) {
        HiveGenericUDTF(new HiveFunctionWrapper(functionClassName), children)
      } else {
        sys.error(s"No handler for udf ${functionInfo.getFunctionClass}")
      }
    }
  }

  override def registerFunction(name: String, info: ExpressionInfo, builder: FunctionBuilder)
  : Unit = underlying.registerFunction(name, info, builder)

  /* List all of the registered function names. */
  override def listFunction(): Seq[String] = {
    val a = FunctionRegistry.getFunctionNames ++ underlying.listFunction()
    a.toList.sorted
  }

  /* Get the class of the registered function by specified name. */
  override def lookupFunction(name: String): Option[ExpressionInfo] = {
    underlying.lookupFunction(name).orElse(
    Try {
      val info = FunctionRegistry.getFunctionInfo(name)
      val annotation = info.getFunctionClass.getAnnotation(classOf[Description])
      if (annotation != null) {
        Some(new ExpressionInfo(
          info.getFunctionClass.getCanonicalName,
          annotation.name(),
          annotation.value(),
          annotation.extended()))
      } else {
        None
      }
    }.getOrElse(None))
  }
}

private[hive] case class HiveSimpleUDF(funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression with HiveInspectors with CodegenFallback with Logging {

  override def deterministic: Boolean = isUDFDeterministic

  override def nullable: Boolean = true

  @transient
  lazy val function = funcWrapper.createFunction[UDF]()

  @transient
  private lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

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

  val dataType = javaClassToDataType(method.getReturnType)

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

private[hive] case class HiveGenericUDF(funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
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

  lazy val dataType: DataType = inspectorToDataType(returnInspector)

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
}

/**
 * Resolves [[UnresolvedWindowFunction]] to [[HiveWindowFunction]].
 */
private[spark] object ResolveHiveWindowFunction extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p: LogicalPlan if !p.childrenResolved => p

    // We are resolving WindowExpressions at here. When we get here, we have already
    // replaced those WindowSpecReferences.
    case p: LogicalPlan =>
      p transformExpressions {
        case WindowExpression(
          UnresolvedWindowFunction(name, children),
          windowSpec: WindowSpecDefinition) =>
          // First, let's find the window function info.
          val windowFunctionInfo: WindowFunctionInfo =
            Option(FunctionRegistry.getWindowFunctionInfo(name.toLowerCase)).getOrElse(
              throw new AnalysisException(s"Couldn't find window function $name"))

          // Get the class of this function.
          // In Hive 0.12, there is no windowFunctionInfo.getFunctionClass. So, we use
          // windowFunctionInfo.getfInfo().getFunctionClass for both Hive 0.13 and Hive 0.13.1.
          val functionClass = windowFunctionInfo.getFunctionClass()
          val newChildren =
            // Rank(), DENSE_RANK(), CUME_DIST(), and PERCENT_RANK() do not take explicit
            // input parameters and requires implicit parameters, which
            // are expressions in Order By clause.
            if (classOf[GenericUDAFRank].isAssignableFrom(functionClass)) {
              if (children.nonEmpty) {
               throw  new AnalysisException(s"$name does not take input parameters.")
              }
              windowSpec.orderSpec.map(_.child)
            } else {
              children
            }

          // If the class is UDAF, we need to use UDAFBridge.
          val isUDAFBridgeRequired =
            if (classOf[UDAF].isAssignableFrom(functionClass)) {
              true
            } else {
              false
            }

          // Create the HiveWindowFunction. For the meaning of isPivotResult, see the doc of
          // HiveWindowFunction.
          val windowFunction =
            HiveWindowFunction(
              new HiveFunctionWrapper(functionClass.getName),
              windowFunctionInfo.isPivotResult,
              isUDAFBridgeRequired,
              newChildren)

          // Second, check if the specified window function can accept window definition.
          windowSpec.frameSpecification match {
            case frame: SpecifiedWindowFrame if !windowFunctionInfo.isSupportsWindow =>
              // This Hive window function does not support user-speficied window frame.
              throw new AnalysisException(
                s"Window function $name does not take a frame specification.")
            case frame: SpecifiedWindowFrame if windowFunctionInfo.isSupportsWindow &&
                                                windowFunctionInfo.isPivotResult =>
              // These two should not be true at the same time when a window frame is defined.
              // If so, throw an exception.
              throw new AnalysisException(s"Could not handle Hive window function $name because " +
                s"it supports both a user specified window frame and pivot result.")
            case _ => // OK
          }
          // Resolve those UnspecifiedWindowFrame because the physical Window operator still needs
          // a window frame specification to work.
          val newWindowSpec = windowSpec.frameSpecification match {
            case UnspecifiedFrame =>
              val newWindowFrame =
                SpecifiedWindowFrame.defaultWindowFrame(
                  windowSpec.orderSpec.nonEmpty,
                  windowFunctionInfo.isSupportsWindow)
              WindowSpecDefinition(windowSpec.partitionSpec, windowSpec.orderSpec, newWindowFrame)
            case _ => windowSpec
          }

          // Finally, we create a WindowExpression with the resolved window function and
          // specified window spec.
          WindowExpression(windowFunction, newWindowSpec)
      }
  }
}

/**
 * A [[WindowFunction]] implementation wrapping Hive's window function.
 * @param funcWrapper The wrapper for the Hive Window Function.
 * @param pivotResult If it is true, the Hive function will return a list of values representing
 *                    the values of the added columns. Otherwise, a single value is returned for
 *                    current row.
 * @param isUDAFBridgeRequired If it is true, the function returned by functionWrapper's
 *                             createFunction is UDAF, we need to use GenericUDAFBridge to wrap
 *                             it as a GenericUDAFResolver2.
 * @param children Input parameters.
 */
private[hive] case class HiveWindowFunction(
    funcWrapper: HiveFunctionWrapper,
    pivotResult: Boolean,
    isUDAFBridgeRequired: Boolean,
    children: Seq[Expression]) extends WindowFunction
  with HiveInspectors with Unevaluable {

  // Hive window functions are based on GenericUDAFResolver2.
  type UDFType = GenericUDAFResolver2

  @transient
  protected lazy val resolver: GenericUDAFResolver2 =
    if (isUDAFBridgeRequired) {
      new GenericUDAFBridge(funcWrapper.createFunction[UDAF]())
    } else {
      funcWrapper.createFunction[GenericUDAFResolver2]()
    }

  @transient
  protected lazy val inputInspectors = children.map(toInspector).toArray

  // The GenericUDAFEvaluator used to evaluate the window function.
  @transient
  protected lazy val evaluator: GenericUDAFEvaluator = {
    val parameterInfo = new SimpleGenericUDAFParameterInfo(inputInspectors, false, false)
    resolver.getEvaluator(parameterInfo)
  }

  // The object inspector of values returned from the Hive window function.
  @transient
  protected lazy val returnInspector = {
    evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inputInspectors)
  }

  override def dataType: DataType =
    if (!pivotResult) {
      inspectorToDataType(returnInspector)
    } else {
      // If pivotResult is true, we should take the element type out as the data type of this
      // function.
      inspectorToDataType(returnInspector) match {
        case ArrayType(dt, _) => dt
        case _ =>
          sys.error(
            s"error resolve the data type of window function ${funcWrapper.functionClassName}")
      }
    }

  override def nullable: Boolean = true

  @transient
  lazy val inputProjection = new InterpretedProjection(children)

  @transient
  private var hiveEvaluatorBuffer: AggregationBuffer = _
  // Output buffer.
  private var outputBuffer: Any = _

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  override def init(): Unit = {
    evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inputInspectors)
  }

  // Reset the hiveEvaluatorBuffer and outputPosition
  override def reset(): Unit = {
    // We create a new aggregation buffer to workaround the bug in GenericUDAFRowNumber.
    // Basically, GenericUDAFRowNumberEvaluator.reset calls RowNumberBuffer.init.
    // However, RowNumberBuffer.init does not really reset this buffer.
    hiveEvaluatorBuffer = evaluator.getNewAggregationBuffer
    evaluator.reset(hiveEvaluatorBuffer)
  }

  override def prepareInputParameters(input: InternalRow): AnyRef = {
    wrap(
      inputProjection(input),
      inputInspectors,
      new Array[AnyRef](children.length),
      inputDataTypes)
  }

  // Add input parameters for a single row.
  override def update(input: AnyRef): Unit = {
    evaluator.iterate(hiveEvaluatorBuffer, input.asInstanceOf[Array[AnyRef]])
  }

  override def batchUpdate(inputs: Array[AnyRef]): Unit = {
    var i = 0
    while (i < inputs.length) {
      evaluator.iterate(hiveEvaluatorBuffer, inputs(i).asInstanceOf[Array[AnyRef]])
      i += 1
    }
  }

  override def evaluate(): Unit = {
    outputBuffer = unwrap(evaluator.evaluate(hiveEvaluatorBuffer), returnInspector)
  }

  override def get(index: Int): Any = {
    if (!pivotResult) {
      // if pivotResult is false, we will get a single value for all rows in the frame.
      outputBuffer
    } else {
      // if pivotResult is true, we will get a ArrayData having the same size with the size
      // of the window frame. At here, we will return the result at the position of
      // index in the output buffer.
      outputBuffer.asInstanceOf[ArrayData].get(index, dataType)
    }
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def newInstance(): WindowFunction =
    new HiveWindowFunction(funcWrapper, pivotResult, isUDAFBridgeRequired, children)
}

private[hive] case class HiveGenericUDAF(
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression]) extends AggregateExpression1
  with HiveInspectors {

  type UDFType = AbstractGenericUDAFResolver

  @transient
  protected lazy val resolver: AbstractGenericUDAFResolver = funcWrapper.createFunction()

  @transient
  protected lazy val objectInspector = {
    val parameterInfo = new SimpleGenericUDAFParameterInfo(inspectors.toArray, false, false)
    resolver.getEvaluator(parameterInfo)
      .init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors.toArray)
  }

  @transient
  protected lazy val inspectors = children.map(toInspector)

  def dataType: DataType = inspectorToDataType(objectInspector)

  def nullable: Boolean = true

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  def newInstance(): HiveUDAFFunction = new HiveUDAFFunction(funcWrapper, children, this)
}

/** It is used as a wrapper for the hive functions which uses UDAF interface */
private[hive] case class HiveUDAF(
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression]) extends AggregateExpression1
  with HiveInspectors {

  type UDFType = UDAF

  @transient
  protected lazy val resolver: AbstractGenericUDAFResolver =
    new GenericUDAFBridge(funcWrapper.createFunction())

  @transient
  protected lazy val objectInspector = {
    val parameterInfo = new SimpleGenericUDAFParameterInfo(inspectors.toArray, false, false)
    resolver.getEvaluator(parameterInfo)
      .init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors.toArray)
  }

  @transient
  protected lazy val inspectors = children.map(toInspector)

  def dataType: DataType = inspectorToDataType(objectInspector)

  def nullable: Boolean = true

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  def newInstance(): HiveUDAFFunction = new HiveUDAFFunction(funcWrapper, children, this, true)
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

  lazy val elementTypes = outputInspector.getAllStructFieldRefs.map {
    field => (inspectorToDataType(field.getFieldObjectInspector), true)
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
}

private[hive] case class HiveUDAFFunction(
    funcWrapper: HiveFunctionWrapper,
    exprs: Seq[Expression],
    base: AggregateExpression1,
    isUDAFBridgeRequired: Boolean = false)
  extends AggregateFunction1
  with HiveInspectors {

  def this() = this(null, null, null)

  private val resolver =
    if (isUDAFBridgeRequired) {
      new GenericUDAFBridge(funcWrapper.createFunction[UDAF]())
    } else {
      funcWrapper.createFunction[AbstractGenericUDAFResolver]()
    }

  private val inspectors = exprs.map(toInspector).toArray

  private val function = {
    val parameterInfo = new SimpleGenericUDAFParameterInfo(inspectors, false, false)
    resolver.getEvaluator(parameterInfo)
  }

  private val returnInspector = function.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)

  private val buffer =
    function.getNewAggregationBuffer

  override def eval(input: InternalRow): Any = unwrap(function.evaluate(buffer), returnInspector)

  @transient
  val inputProjection = new InterpretedProjection(exprs)

  @transient
  protected lazy val cached = new Array[AnyRef](exprs.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = exprs.map(_.dataType).toArray

  def update(input: InternalRow): Unit = {
    val inputs = inputProjection(input)
    function.iterate(buffer, wrap(inputs, inspectors, cached, inputDataTypes))
  }
}

