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

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.exec.{FunctionInfo, FunctionRegistry}
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic._

import org.apache.spark.sql.Logging
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.Utils.getContextOrSparkClassLoader

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] abstract class HiveFunctionRegistry
  extends analysis.FunctionRegistry with HiveInspectors {

  def getFunctionInfo(name: String) = FunctionRegistry.getFunctionInfo(name)

  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
    // not always serializable.
    val functionInfo: FunctionInfo = Option(FunctionRegistry.getFunctionInfo(name)).getOrElse(
      sys.error(s"Couldn't find function $name"))

    val functionClassName = functionInfo.getFunctionClass.getName()

    if (classOf[UDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      val function = functionInfo.getFunctionClass.newInstance().asInstanceOf[UDF]
      val method = function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

      lazy val expectedDataTypes = method.getParameterTypes.map(javaClassToDataType)

      HiveSimpleUdf(
        functionClassName,
        children.zip(expectedDataTypes).map { case (e, t) => Cast(e, t) }
      )
    } else if (classOf[GenericUDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdf(functionClassName, children)
    } else if (
         classOf[AbstractGenericUDAFResolver].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdaf(functionClassName, children)

    } else if (classOf[GenericUDTF].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdtf(functionClassName, Nil, children)
    } else {
      sys.error(s"No handler for udf ${functionInfo.getFunctionClass}")
    }
  }
}

private[hive] trait HiveFunctionFactory {
  val functionClassName: String

  def createFunction[UDFType]() =
    getContextOrSparkClassLoader.loadClass(functionClassName).newInstance.asInstanceOf[UDFType]
}

private[hive] abstract class HiveUdf extends Expression with Logging with HiveFunctionFactory {
  self: Product =>

  type UDFType
  type EvaluatedType = Any

  def nullable = true
  def references = children.flatMap(_.references).toSet

  lazy val function = createFunction[UDFType]()

  override def toString = s"$nodeName#$functionClassName(${children.mkString(",")})"
}

private[hive] case class HiveSimpleUdf(functionClassName: String, children: Seq[Expression])
  extends HiveUdf with HiveInspectors {

  type UDFType = UDF

  @transient
  protected lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

  @transient
  lazy val dataType = javaClassToDataType(method.getReturnType)

  protected lazy val wrappers: Array[(Any) => AnyRef] = method.getParameterTypes.map { argClass =>
    val primitiveClasses = Seq(
      Integer.TYPE, classOf[java.lang.Integer], classOf[java.lang.String], java.lang.Double.TYPE,
      classOf[java.lang.Double], java.lang.Long.TYPE, classOf[java.lang.Long],
      classOf[HiveDecimal], java.lang.Byte.TYPE, classOf[java.lang.Byte],
      classOf[java.sql.Timestamp]
    )
    val matchingConstructor = argClass.getConstructors.find { c =>
      c.getParameterTypes.size == 1 && primitiveClasses.contains(c.getParameterTypes.head)
    }

    val constructor = matchingConstructor.getOrElse(
      sys.error(s"No matching wrapper found, options: ${argClass.getConstructors.toSeq}."))

    (a: Any) => {
      logger.debug(
        s"Wrapping $a of type ${if (a == null) "null" else a.getClass.getName} using $constructor.")
      // We must make sure that primitives get boxed java style.
      if (a == null) {
        null
      } else {
        constructor.newInstance(a match {
          case i: Int => i: java.lang.Integer
          case bd: BigDecimal => new HiveDecimal(bd.underlying())
          case other: AnyRef => other
        }).asInstanceOf[AnyRef]
      }
    }
  }

  // TODO: Finish input output types.
  override def eval(input: Row): Any = {
    val evaluatedChildren = children.map(_.eval(input))
    // Wrap the function arguments in the expected types.
    val args = evaluatedChildren.zip(wrappers).map {
      case (arg, wrapper) => wrapper(arg)
    }

    // Invoke the udf and unwrap the result.
    unwrap(method.invoke(function, args: _*))
  }
}

private[hive] case class HiveGenericUdf(functionClassName: String, children: Seq[Expression])
  extends HiveUdf with HiveInspectors {

  import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._

  type UDFType = GenericUDF

  @transient
  protected lazy val argumentInspectors = children.map(_.dataType).map(toInspector)

  @transient
  protected lazy val returnInspector = function.initialize(argumentInspectors.toArray)

  @transient
  protected lazy val isUDFDeterministic = {
    val udfType = function.getClass().getAnnotation(classOf[HiveUDFType])
    (udfType != null && udfType.deterministic())
  }

  override def foldable = {
    isUDFDeterministic && children.foldLeft(true)((prev, n) => prev && n.foldable)
  }

  protected lazy val deferedObjects = Array.fill[DeferredObject](children.length)({
    new DeferredObjectAdapter
  })

  // Adapter from Catalyst ExpressionResult to Hive DeferredObject
  class DeferredObjectAdapter extends DeferredObject {
    private var func: () => Any = _
    def set(func: () => Any) {
      this.func = func
    }
    override def prepare(i: Int) = {}
    override def get(): AnyRef = wrap(func())
  }

  val dataType: DataType = inspectorToDataType(returnInspector)

  override def eval(input: Row): Any = {
    returnInspector // Make sure initialized.
    var i = 0
    while (i < children.length) {
      val idx = i
      deferedObjects(i).asInstanceOf[DeferredObjectAdapter].set(() => {children(idx).eval(input)})
      i += 1
    }
    unwrap(function.evaluate(deferedObjects))
  }
}

private[hive] case class HiveGenericUdaf(
    functionClassName: String,
    children: Seq[Expression]) extends AggregateExpression
  with HiveInspectors
  with HiveFunctionFactory {

  type UDFType = AbstractGenericUDAFResolver

  @transient
  protected lazy val resolver: AbstractGenericUDAFResolver = createFunction()

  @transient
  protected lazy val objectInspector  = {
    resolver.getEvaluator(children.map(_.dataType.toTypeInfo).toArray)
      .init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors.toArray)
  }

  @transient
  protected lazy val inspectors = children.map(_.dataType).map(toInspector)

  def dataType: DataType = inspectorToDataType(objectInspector)

  def nullable: Boolean = true

  def references: Set[Attribute] = children.map(_.references).flatten.toSet

  override def toString = s"$nodeName#$functionClassName(${children.mkString(",")})"

  def newInstance() = new HiveUdafFunction(functionClassName, children, this)
}

/**
 * Converts a Hive Generic User Defined Table Generating Function (UDTF) to a
 * [[catalyst.expressions.Generator Generator]].  Note that the semantics of Generators do not allow
 * Generators to maintain state in between input rows.  Thus UDTFs that rely on partitioning
 * dependent operations like calls to `close()` before producing output will not operate the same as
 * in Hive.  However, in practice this should not affect compatibility for most sane UDTFs
 * (e.g. explode or GenericUDTFParseUrlTuple).
 *
 * Operators that require maintaining state in between input rows should instead be implemented as
 * user defined aggregations, which have clean semantics even in a partitioned execution.
 */
private[hive] case class HiveGenericUdtf(
    functionClassName: String,
    aliasNames: Seq[String],
    children: Seq[Expression])
  extends Generator with HiveInspectors with HiveFunctionFactory {

  override def references = children.flatMap(_.references).toSet

  @transient
  protected lazy val function: GenericUDTF = createFunction()

  @transient
  protected lazy val inputInspectors = children.map(_.dataType).map(toInspector)

  @transient
  protected lazy val outputInspectors = {
    val structInspector = function.initialize(inputInspectors.toArray)
    structInspector.getAllStructFieldRefs.map(_.getFieldObjectInspector)
  }

  protected lazy val outputDataTypes = outputInspectors.map(inspectorToDataType)

  override protected def makeOutput() = {
    // Use column names when given, otherwise c_1, c_2, ... c_n.
    if (aliasNames.size == outputDataTypes.size) {
      aliasNames.zip(outputDataTypes).map {
        case (attrName, attrDataType) =>
          AttributeReference(attrName, attrDataType, nullable = true)()
      }
    } else {
      outputDataTypes.zipWithIndex.map {
        case (attrDataType, i) =>
          AttributeReference(s"c_$i", attrDataType, nullable = true)()
      }
    }
  }

  override def eval(input: Row): TraversableOnce[Row] = {
    outputInspectors // Make sure initialized.

    val inputProjection = new InterpretedProjection(children)
    val collector = new UDTFCollector
    function.setCollector(collector)

    val udtInput = inputProjection(input).map(wrap).toArray
    function.process(udtInput)
    collector.collectRows()
  }

  protected class UDTFCollector extends Collector {
    var collected = new ArrayBuffer[Row]

    override def collect(input: java.lang.Object) {
      // We need to clone the input here because implementations of
      // GenericUDTF reuse the same object. Luckily they are always an array, so
      // it is easy to clone.
      collected += new GenericRow(input.asInstanceOf[Array[_]].map(unwrap))
    }

    def collectRows() = {
      val toCollect = collected
      collected = new ArrayBuffer[Row]
      toCollect
    }
  }

  override def toString = s"$nodeName#$functionClassName(${children.mkString(",")})"
}

private[hive] case class HiveUdafFunction(
    functionClassName: String,
    exprs: Seq[Expression],
    base: AggregateExpression)
  extends AggregateFunction
  with HiveInspectors
  with HiveFunctionFactory {

  def this() = this(null, null, null)

  private val resolver = createFunction[AbstractGenericUDAFResolver]()

  private val inspectors = exprs.map(_.dataType).map(toInspector).toArray

  private val function = resolver.getEvaluator(exprs.map(_.dataType.toTypeInfo).toArray)

  private val returnInspector = function.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)

  // Cast required to avoid type inference selecting a deprecated Hive API.
  private val buffer =
    function.getNewAggregationBuffer.asInstanceOf[GenericUDAFEvaluator.AbstractAggregationBuffer]

  override def eval(input: Row): Any = unwrapData(function.evaluate(buffer), returnInspector)

  @transient
  val inputProjection = new InterpretedProjection(exprs)

  def update(input: Row): Unit = {
    val inputs = inputProjection(input).asInstanceOf[Seq[AnyRef]].toArray
    function.iterate(buffer, inputs)
  }
}
