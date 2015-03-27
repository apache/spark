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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ConstantObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.exec.{UDF, UDAF}
import org.apache.hadoop.hive.ql.exec.{FunctionInfo, FunctionRegistry}
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Project, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.errors.TreeNodeException

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] abstract class HiveFunctionRegistry
  extends analysis.FunctionRegistry with HiveInspectors {

  def getFunctionInfo(name: String): FunctionInfo = FunctionRegistry.getFunctionInfo(name)

  def lookupFunction(
      name: String,
      children: Seq[Expression],
      distinct: Boolean = false): Expression = {
    // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
    // not always serializable.
    val functionInfo: FunctionInfo =
      Option(FunctionRegistry.getFunctionInfo(name.toLowerCase)).getOrElse(
        sys.error(s"Couldn't find function $name"))
    val funcClazz = functionInfo.getFunctionClass

    val functionClassName = functionInfo.getFunctionClass.getName

    if (classOf[UDF].isAssignableFrom(funcClazz)) {
      HiveSimpleUdf(new HiveFunctionWrapper(functionClassName), children)
    } else if (classOf[GenericUDF].isAssignableFrom(funcClazz)) {
      HiveGenericUdf(new HiveFunctionWrapper(functionClassName), children)
    } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(funcClazz)) {
      HiveGenericUdaf(new HiveFunctionWrapper(functionClassName), children, distinct, false)
    } else if (classOf[UDAF].isAssignableFrom(funcClazz)) {
      HiveGenericUdaf(new HiveFunctionWrapper(functionClassName), children, distinct, true)
    } else if (classOf[GenericUDTF].isAssignableFrom(funcClazz)) {
      HiveGenericUdtf(new HiveFunctionWrapper(functionClassName), Nil, children)
    } else {
      sys.error(s"No handler for udf ${functionInfo.getFunctionClass}")
    }
  }
}

private[hive] case class HiveSimpleUdf(funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression with HiveInspectors with Logging {
  type EvaluatedType = Any
  type UDFType = UDF

  override def nullable: Boolean = true

  @transient
  lazy val function = funcWrapper.createFunction[UDFType]()

  @transient
  protected lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

  @transient
  protected lazy val arguments = children.map(toInspector).toArray

  @transient
  protected lazy val isUDFDeterministic = {
    val udfType = function.getClass().getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic()
  }

  override def foldable: Boolean = isUDFDeterministic && children.forall(_.foldable)

  // Create parameter converters
  @transient
  protected lazy val conversionHelper = new ConversionHelper(method, arguments)

  @transient
  lazy val dataType = javaClassToDataType(method.getReturnType)

  @transient
  lazy val returnInspector = ObjectInspectorFactory.getReflectionObjectInspector(
    method.getGenericReturnType(), ObjectInspectorOptions.JAVA)

  @transient
  protected lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  // TODO: Finish input output types.
  override def eval(input: Row): Any = {
    unwrap(
      FunctionRegistry.invoke(method, function, conversionHelper
        .convertIfNecessary(wrap(children.map(c => c.eval(input)), arguments, cached): _*): _*),
      returnInspector)
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }
}

// Adapter from Catalyst ExpressionResult to Hive DeferredObject
private[hive] class DeferredObjectAdapter(oi: ObjectInspector)
  extends DeferredObject with HiveInspectors {
  private var func: () => Any = _
  def set(func: () => Any): Unit = {
    this.func = func
  }
  override def prepare(i: Int): Unit = {}
  override def get(): AnyRef = wrap(func(), oi)
}

private[hive] case class HiveGenericUdf(funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression with HiveInspectors with Logging {
  type UDFType = GenericUDF
  type EvaluatedType = Any

  override def nullable: Boolean = true

  @transient
  lazy val function = funcWrapper.createFunction[UDFType]()

  @transient
  protected lazy val argumentInspectors = children.map(toInspector)

  @transient
  protected lazy val returnInspector = {
    function.initializeAndFoldConstants(argumentInspectors.toArray)
  }

  @transient
  protected lazy val isUDFDeterministic = {
    val udfType = function.getClass().getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic()
  }

  override def foldable: Boolean =
    isUDFDeterministic && returnInspector.isInstanceOf[ConstantObjectInspector]

  @transient
  protected lazy val deferedObjects =
    argumentInspectors.map(new DeferredObjectAdapter(_)).toArray[DeferredObject]

  lazy val dataType: DataType = inspectorToDataType(returnInspector)

  override def eval(input: Row): Any = {
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

private[hive] case class HiveGenericUdaf(
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    distinct: Boolean,
    isUDAF: Boolean) extends AggregateExpression
  with HiveInspectors {
  type UDFType = AbstractGenericUDAFResolver

  protected def createEvaluator = resolver.getEvaluator(
    new SimpleGenericUDAFParameterInfo(inspectors, false, false))

  // Hive UDAF evaluator
  @transient
  lazy val evaluator = createEvaluator

  @transient
  protected lazy val resolver: AbstractGenericUDAFResolver = if (isUDAF) {
    // if it's UDAF, we need the UDAF bridge
    new GenericUDAFBridge(funcWrapper.createFunction())
  } else {
    funcWrapper.createFunction()
  }

  // Output data object inspector
  @transient
  lazy val objectInspector = createEvaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)

  // Aggregation Buffer Inspector
  @transient
  lazy val bufferObjectInspector = {
    createEvaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inspectors)
  }

  // Input arguments object inspectors
  @transient
  lazy val inspectors = children.map(toInspector).toArray

  @transient
  override val distinctLike: Boolean = {
    val annotation = evaluator.getClass().getAnnotation(classOf[HiveUDFType])
    if (annotation == null || !annotation.distinctLike()) false else true
  }
  override def toString = s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"

  // Aggregation Buffer Data Type, We assume only 1 element for the Hive Aggregation Buffer
  // It will be StructType if more than 1 element (Actually will be StructSettableObjectInspector)
  override def bufferDataType: Seq[DataType] = inspectorToDataType(bufferObjectInspector) :: Nil

  // Output data type
  override def dataType: DataType = inspectorToDataType(objectInspector)

  ///////////////////////////////////////////////////////////////////////////////////////////////
  //            The following code will be called within the executors                         //
  ///////////////////////////////////////////////////////////////////////////////////////////////
  @transient var bound: BoundReference = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    bound = buffers(0)
    mode match {
      case FINAL => evaluator.init(GenericUDAFEvaluator.Mode.FINAL, Array(bufferObjectInspector))
      case COMPLETE => evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)
      case PARTIAL1 => evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inspectors)
    }
  }

  // Initialize (reinitialize) the aggregation buffer
  override def reset(buf: MutableRow): Unit = {
    val buffer = evaluator.getNewAggregationBuffer
      .asInstanceOf[GenericUDAFEvaluator.AbstractAggregationBuffer]
    evaluator.reset(buffer)
    // This is a hack, we never use the mutable row as buffer, but define our own buffer,
    // which is set as the first element of the buffer
    buf(bound) = buffer
  }

  // Expect the aggregate function fills the aggregation buffer when fed with each value
  // in the group
  override def iterate(arguments: Any, buf: MutableRow): Unit = {
    val args = arguments.asInstanceOf[Seq[AnyRef]].zip(inspectors).map {
      case (value, oi) => wrap(value, oi)
    }.toArray

    evaluator.iterate(
      buf.getAs[GenericUDAFEvaluator.AbstractAggregationBuffer](bound.ordinal),
      args)
  }

  // Merge 2 aggregation buffer, and write back to the later one
  override def merge(value: Row, buf: MutableRow): Unit = {
    val buffer = buf.getAs[GenericUDAFEvaluator.AbstractAggregationBuffer](bound.ordinal)
    evaluator.merge(buffer, wrap(value.get(bound.ordinal), bufferObjectInspector))
  }

  @deprecated
  override def terminatePartial(buf: MutableRow): Unit = {
    val buffer = buf.getAs[GenericUDAFEvaluator.AbstractAggregationBuffer](bound.ordinal)
    // this is for serialization
    buf(bound) = unwrap(evaluator.terminatePartial(buffer), bufferObjectInspector)
  }

  // Output the final result by feeding the aggregation buffer
  override def terminate(input: Row): Any = {
    unwrap(evaluator.terminate(
      input.getAs[GenericUDAFEvaluator.AbstractAggregationBuffer](bound.ordinal)),
      objectInspector)
  }
}

/**
 * Converts a Hive Generic User Defined Table Generating Function (UDTF) to a
 * [[catalyst.expressions.Generator Generator]].  Note that the semantics of Generators do not
 * allow Generators to maintain state in between input rows.  Thus UDTFs that rely on partitioning
 * dependent operations like calls to `close()` before producing output will not operate the same
 * asin Hive.  However, in practice this should not affect compatibility for most sane UDTFs
 * (e.g. explode or GenericUDTFParseUrlTuple).
 *
 * Operators that require maintaining state in between input rows should instead be implemented as
 * user defined aggregations, which have clean semantics even in a partitioned execution.
 */
private[hive] case class HiveGenericUdtf(
    funcWrapper: HiveFunctionWrapper,
    aliasNames: Seq[String],
    children: Seq[Expression])
  extends Generator with HiveInspectors {

  @transient
  protected lazy val function: GenericUDTF = funcWrapper.createFunction()

  @transient
  protected lazy val inputInspectors = children.map(toInspector)

  @transient
  protected lazy val outputInspector = function.initialize(inputInspectors.toArray)

  @transient
  protected lazy val udtInput = new Array[AnyRef](children.length)

  protected lazy val outputDataTypes = outputInspector.getAllStructFieldRefs.map {
    field => inspectorToDataType(field.getFieldObjectInspector)
  }

  override protected def makeOutput() = {
    // Use column names when given, otherwise _c1, _c2, ... _cn.
    if (aliasNames.size == outputDataTypes.size) {
      aliasNames.zip(outputDataTypes).map {
        case (attrName, attrDataType) =>
          AttributeReference(attrName, attrDataType, nullable = true)()
      }
    } else {
      outputDataTypes.zipWithIndex.map {
        case (attrDataType, i) =>
          AttributeReference(s"_c$i", attrDataType, nullable = true)()
      }
    }
  }

  override def eval(input: Row): TraversableOnce[Row] = {
    outputInspector // Make sure initialized.

    val inputProjection = new InterpretedProjection(children)
    val collector = new UDTFCollector
    function.setCollector(collector)
    function.process(wrap(inputProjection(input), inputInspectors, udtInput))
    collector.collectRows()
  }

  protected class UDTFCollector extends Collector {
    var collected = new ArrayBuffer[Row]

    override def collect(input: java.lang.Object) {
      // We need to clone the input here because implementations of
      // GenericUDTF reuse the same object. Luckily they are always an array, so
      // it is easy to clone.
      collected += unwrap(input, outputInspector).asInstanceOf[Row]
    }

    def collectRows(): Seq[Row] = {
      val toCollect = collected
      collected = new ArrayBuffer[Row]
      toCollect
    }
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }
}

private[spark] object ResolveUdtfsAlias extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(projectList, _)
      if projectList.exists(_.isInstanceOf[MultiAlias]) && projectList.size != 1 =>
      throw new TreeNodeException(p, "only single Generator supported for SELECT clause")

    case Project(Seq(Alias(udtf @ HiveGenericUdtf(_, _, _), name)), child) =>
        Generate(udtf.copy(aliasNames = Seq(name)), join = false, outer = false, None, child)
    case Project(Seq(MultiAlias(udtf @ HiveGenericUdtf(_, _, _), names)), child) =>
        Generate(udtf.copy(aliasNames = names), join = false, outer = false, None, child)
  }
}

