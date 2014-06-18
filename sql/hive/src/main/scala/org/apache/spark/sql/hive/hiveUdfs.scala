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
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.{io => hadoopIo}

import org.apache.spark.sql.Logging
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types
import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] object HiveFunctionRegistry
  extends analysis.FunctionRegistry with HiveFunctionFactory with HiveInspectors {

  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
    // not always serializable.
    val functionInfo: FunctionInfo = Option(FunctionRegistry.getFunctionInfo(name)).getOrElse(
      sys.error(s"Couldn't find function $name"))

    if (classOf[UDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      val function = createFunction[UDF](name)
      val method = function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

      lazy val expectedDataTypes = method.getParameterTypes.map(javaClassToDataType)

      HiveSimpleUdf(
        name,
        children.zip(expectedDataTypes).map { case (e, t) => Cast(e, t) }
      )
    } else if (classOf[GenericUDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdf(name, children)
    } else if (
         classOf[AbstractGenericUDAFResolver].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdaf(name, children)

    } else if (classOf[GenericUDTF].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdtf(name, Nil, children)
    } else {
      sys.error(s"No handler for udf ${functionInfo.getFunctionClass}")
    }
  }

  def javaClassToDataType(clz: Class[_]): DataType = clz match {
    // writable
    case c: Class[_] if c == classOf[hadoopIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.HiveDecimalWritable] => DecimalType
    case c: Class[_] if c == classOf[hiveIo.ByteWritable] => ByteType
    case c: Class[_] if c == classOf[hiveIo.ShortWritable] => ShortType
    case c: Class[_] if c == classOf[hiveIo.TimestampWritable] => TimestampType
    case c: Class[_] if c == classOf[hadoopIo.Text] => StringType
    case c: Class[_] if c == classOf[hadoopIo.IntWritable] => IntegerType
    case c: Class[_] if c == classOf[hadoopIo.LongWritable] => LongType
    case c: Class[_] if c == classOf[hadoopIo.FloatWritable] => FloatType
    case c: Class[_] if c == classOf[hadoopIo.BooleanWritable] => BooleanType
    case c: Class[_] if c == classOf[hadoopIo.BytesWritable] => BinaryType
    
    // java class
    case c: Class[_] if c == classOf[java.lang.String] => StringType
    case c: Class[_] if c == classOf[java.sql.Timestamp] => TimestampType
    case c: Class[_] if c == classOf[HiveDecimal] => DecimalType
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType
    case c: Class[_] if c == classOf[Array[Byte]] => BinaryType
    case c: Class[_] if c == classOf[java.lang.Short] => ShortType
    case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
    case c: Class[_] if c == classOf[java.lang.Long] => LongType
    case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
    case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
    case c: Class[_] if c == classOf[java.lang.Float] => FloatType
    case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType
    
    // primitive type
    case c: Class[_] if c == java.lang.Short.TYPE => ShortType
    case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
    case c: Class[_] if c == java.lang.Long.TYPE => LongType
    case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
    case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
    case c: Class[_] if c == java.lang.Float.TYPE => FloatType
    case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType
    
    case c: Class[_] if c.isArray => ArrayType(javaClassToDataType(c.getComponentType))
  }
}

private[hive] trait HiveFunctionFactory {
  def getFunctionInfo(name: String) = FunctionRegistry.getFunctionInfo(name)
  def getFunctionClass(name: String) = getFunctionInfo(name).getFunctionClass
  def createFunction[UDFType](name: String) =
    getFunctionClass(name).newInstance.asInstanceOf[UDFType]

  /** Converts hive types to native catalyst types. */
  def unwrap(a: Any): Any = a match {
    case null => null
    case i: hadoopIo.IntWritable => i.get
    case t: hadoopIo.Text => t.toString
    case l: hadoopIo.LongWritable => l.get
    case d: hadoopIo.DoubleWritable => d.get
    case d: hiveIo.DoubleWritable => d.get
    case s: hiveIo.ShortWritable => s.get
    case b: hadoopIo.BooleanWritable => b.get
    case b: hiveIo.ByteWritable => b.get
    case b: hadoopIo.FloatWritable => b.get
    case b: hadoopIo.BytesWritable => {
      val bytes = new Array[Byte](b.getLength)
      System.arraycopy(b.getBytes(), 0, bytes, 0, b.getLength)
      bytes
    }
    case t: hiveIo.TimestampWritable => t.getTimestamp
    case b: hiveIo.HiveDecimalWritable => BigDecimal(b.getHiveDecimal().bigDecimalValue())
    case list: java.util.List[_] => list.map(unwrap)
    case map: java.util.Map[_,_] => map.map { case (k, v) => (unwrap(k), unwrap(v)) }.toMap
    case array: Array[_] => array.map(unwrap).toSeq
    case p: java.lang.Short => p
    case p: java.lang.Long => p
    case p: java.lang.Float => p
    case p: java.lang.Integer => p
    case p: java.lang.Double => p
    case p: java.lang.Byte => p
    case p: java.lang.Boolean => p
    case str: String => str
    case p: BigDecimal => p
    case p: Array[Byte] => p
    case p: java.sql.Timestamp => p
  }
}

private[hive] abstract class HiveUdf extends Expression with Logging with HiveFunctionFactory {
  self: Product =>

  type UDFType
  type EvaluatedType = Any

  val name: String

  def nullable = true
  def references = children.flatMap(_.references).toSet

  // FunctionInfo is not serializable so we must look it up here again.
  lazy val functionInfo = getFunctionInfo(name)
  lazy val function = createFunction[UDFType](name)

  override def toString = s"$nodeName#${functionInfo.getDisplayName}(${children.mkString(",")})"
}

private[hive] case class HiveSimpleUdf(name: String, children: Seq[Expression]) extends HiveUdf {
  import org.apache.spark.sql.hive.HiveFunctionRegistry._
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

private[hive] case class HiveGenericUdf(name: String, children: Seq[Expression])
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

private[hive] trait HiveInspectors {

  def unwrapData(data: Any, oi: ObjectInspector): Any = oi match {
    case pi: PrimitiveObjectInspector => pi.getPrimitiveJavaObject(data)
    case li: ListObjectInspector =>
      Option(li.getList(data))
        .map(_.map(unwrapData(_, li.getListElementObjectInspector)).toSeq)
        .orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k,v) =>
            (unwrapData(k, mi.getMapKeyObjectInspector),
              unwrapData(v, mi.getMapValueObjectInspector))
        }.toMap).orNull
    case si: StructObjectInspector =>
      val allRefs = si.getAllStructFieldRefs
      new GenericRow(
        allRefs.map(r =>
          unwrapData(si.getStructFieldData(data,r), r.getFieldObjectInspector)).toArray)
  }

  /** Converts native catalyst types to the types expected by Hive */
  def wrap(a: Any): AnyRef = a match {
    case s: String => new hadoopIo.Text(s) // TODO why should be Text?
    case i: Int => i: java.lang.Integer
    case b: Boolean => b: java.lang.Boolean
    case f: Float => f: java.lang.Float
    case d: Double => d: java.lang.Double
    case l: Long => l: java.lang.Long
    case l: Short => l: java.lang.Short
    case l: Byte => l: java.lang.Byte
    case b: BigDecimal => b.bigDecimal
    case b: Array[Byte] => b
    case t: java.sql.Timestamp => t
    case s: Seq[_] => seqAsJavaList(s.map(wrap))
    case m: Map[_,_] =>
      mapAsJavaMap(m.map { case (k, v) => wrap(k) -> wrap(v) })
    case null => null
  }

  def toInspector(dataType: DataType): ObjectInspector = dataType match {
    case ArrayType(tpe) => ObjectInspectorFactory.getStandardListObjectInspector(toInspector(tpe))
    case MapType(keyType, valueType) =>
      ObjectInspectorFactory.getStandardMapObjectInspector(
        toInspector(keyType), toInspector(valueType))
    case StringType => PrimitiveObjectInspectorFactory.javaStringObjectInspector
    case IntegerType => PrimitiveObjectInspectorFactory.javaIntObjectInspector
    case DoubleType => PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
    case BooleanType => PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
    case LongType => PrimitiveObjectInspectorFactory.javaLongObjectInspector
    case FloatType => PrimitiveObjectInspectorFactory.javaFloatObjectInspector
    case ShortType => PrimitiveObjectInspectorFactory.javaShortObjectInspector
    case ByteType => PrimitiveObjectInspectorFactory.javaByteObjectInspector
    case NullType => PrimitiveObjectInspectorFactory.javaVoidObjectInspector
    case BinaryType => PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector
    case TimestampType => PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
    case DecimalType => PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        fields.map(f => f.name), fields.map(f => toInspector(f.dataType)))
  }

  def inspectorToDataType(inspector: ObjectInspector): DataType = inspector match {
    case s: StructObjectInspector =>
      StructType(s.getAllStructFieldRefs.map(f => {
        types.StructField(
          f.getFieldName, inspectorToDataType(f.getFieldObjectInspector), nullable = true)
      }))
    case l: ListObjectInspector => ArrayType(inspectorToDataType(l.getListElementObjectInspector))
    case m: MapObjectInspector =>
      MapType(
        inspectorToDataType(m.getMapKeyObjectInspector),
        inspectorToDataType(m.getMapValueObjectInspector))
    case _: WritableStringObjectInspector => StringType
    case _: JavaStringObjectInspector => StringType
    case _: WritableIntObjectInspector => IntegerType
    case _: JavaIntObjectInspector => IntegerType
    case _: WritableDoubleObjectInspector => DoubleType
    case _: JavaDoubleObjectInspector => DoubleType
    case _: WritableBooleanObjectInspector => BooleanType
    case _: JavaBooleanObjectInspector => BooleanType
    case _: WritableLongObjectInspector => LongType
    case _: JavaLongObjectInspector => LongType
    case _: WritableShortObjectInspector => ShortType
    case _: JavaShortObjectInspector => ShortType
    case _: WritableByteObjectInspector => ByteType
    case _: JavaByteObjectInspector => ByteType
    case _: WritableFloatObjectInspector => FloatType
    case _: JavaFloatObjectInspector => FloatType
    case _: WritableBinaryObjectInspector => BinaryType
    case _: JavaBinaryObjectInspector => BinaryType
    case _: WritableHiveDecimalObjectInspector => DecimalType
    case _: JavaHiveDecimalObjectInspector => DecimalType
    case _: WritableTimestampObjectInspector => TimestampType
    case _: JavaTimestampObjectInspector => TimestampType
  }

  implicit class typeInfoConversions(dt: DataType) {
    import org.apache.hadoop.hive.serde2.typeinfo._
    import TypeInfoFactory._

    def toTypeInfo: TypeInfo = dt match {
      case BinaryType => binaryTypeInfo
      case BooleanType => booleanTypeInfo
      case ByteType => byteTypeInfo
      case DoubleType => doubleTypeInfo
      case FloatType => floatTypeInfo
      case IntegerType => intTypeInfo
      case LongType => longTypeInfo
      case ShortType => shortTypeInfo
      case StringType => stringTypeInfo
      case DecimalType => decimalTypeInfo
      case TimestampType => timestampTypeInfo
      case NullType => voidTypeInfo
    }
  }
}

private[hive] case class HiveGenericUdaf(
    name: String,
    children: Seq[Expression]) extends AggregateExpression
  with HiveInspectors
  with HiveFunctionFactory {

  type UDFType = AbstractGenericUDAFResolver

  @transient
  protected lazy val resolver: AbstractGenericUDAFResolver = createFunction(name)

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

  override def toString = s"$nodeName#$name(${children.mkString(",")})"

  def newInstance() = new HiveUdafFunction(name, children, this)
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
    name: String,
    aliasNames: Seq[String],
    children: Seq[Expression])
  extends Generator with HiveInspectors with HiveFunctionFactory {

  override def references = children.flatMap(_.references).toSet

  @transient
  protected lazy val function: GenericUDTF = createFunction(name)

  protected lazy val inputInspectors = children.map(_.dataType).map(toInspector)

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

    val inputProjection = new Projection(children)
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

  override def toString = s"$nodeName#$name(${children.mkString(",")})"
}

private[hive] case class HiveUdafFunction(
    functionName: String,
    exprs: Seq[Expression],
    base: AggregateExpression)
  extends AggregateFunction
  with HiveInspectors
  with HiveFunctionFactory {

  def this() = this(null, null, null)

  private val resolver = createFunction[AbstractGenericUDAFResolver](functionName)

  private val inspectors = exprs.map(_.dataType).map(toInspector).toArray

  private val function = resolver.getEvaluator(exprs.map(_.dataType.toTypeInfo).toArray)

  private val returnInspector = function.init(GenericUDAFEvaluator.Mode.COMPLETE, inspectors)

  // Cast required to avoid type inference selecting a deprecated Hive API.
  private val buffer =
    function.getNewAggregationBuffer.asInstanceOf[GenericUDAFEvaluator.AbstractAggregationBuffer]

  override def eval(input: Row): Any = unwrapData(function.evaluate(buffer), returnInspector)

  @transient
  val inputProjection = new Projection(exprs)

  def update(input: Row): Unit = {
    val inputs = inputProjection(input).asInstanceOf[Seq[AnyRef]].toArray
    function.iterate(buffer, inputs)
  }
}
