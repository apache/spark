package catalyst
package execution

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.exec.{FunctionInfo, FunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{AbstractPrimitiveJavaObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.io._

import expressions._
import types._

object HiveFunctionRegistry extends analysis.FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // We only look it up to see if it exists, but do not include it in the HiveUDF since it is
    // not always serializable.
    val functionInfo: FunctionInfo = Option(FunctionRegistry.getFunctionInfo(name)).getOrElse(
      sys.error(s"Couldn't find function $name"))

    if (classOf[UDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      val functionInfo = FunctionRegistry.getFunctionInfo(name)
      val function = functionInfo.getFunctionClass.newInstance.asInstanceOf[UDF]
      val method = function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

      lazy val expectedDataTypes = method.getParameterTypes.map(javaClassToDataType)

      HiveSimpleUdf(
        name,
        children.zip(expectedDataTypes).map { case (e, t) => Cast(e, t) }
      )
    } else if (classOf[GenericUDF].isAssignableFrom(functionInfo.getFunctionClass)) {
      HiveGenericUdf(name, IntegerType, children)
    } else {
      sys.error(s"No handler for udf ${functionInfo.getFunctionClass}")
    }
  }

  def javaClassToDataType(clz: Class[_]): DataType = clz match {
    case c: Class[_] if c == classOf[DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[org.apache.hadoop.hive.serde2.io.ByteWritable] => ByteType
    case c: Class[_] if c == classOf[org.apache.hadoop.hive.serde2.io.ShortWritable] => ShortType
    case c: Class[_] if c == classOf[Text] => StringType
    case c: Class[_] if c == classOf[org.apache.hadoop.io.IntWritable] => IntegerType
    case c: Class[_] if c == classOf[org.apache.hadoop.io.LongWritable] => LongType
    case c: Class[_] if c == classOf[org.apache.hadoop.io.FloatWritable] => FloatType
    case c: Class[_] if c == classOf[java.lang.String] => StringType
    case c: Class[_] if c == java.lang.Short.TYPE => ShortType
    case c: Class[_] if c == java.lang.Integer.TYPE => ShortType
    case c: Class[_] if c == java.lang.Long.TYPE => LongType
    case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
    case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
    case c: Class[_] if c == java.lang.Float.TYPE => FloatType
    case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType
    case c: Class[_] if c == classOf[java.lang.Short] => ShortType
    case c: Class[_] if c == classOf[java.lang.Integer] => ShortType
    case c: Class[_] if c == classOf[java.lang.Long] => LongType
    case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
    case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
    case c: Class[_] if c == classOf[java.lang.Float] => FloatType
    case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType
  }
}

abstract class HiveUdf extends Expression with ImplementedUdf with Logging {
  self: Product =>

  type UDFType
  val name: String

  def nullable = true
  def references = children.flatMap(_.references).toSet

  // FunctionInfo is not serializable so we must look it up here again.
  lazy val functionInfo = FunctionRegistry.getFunctionInfo(name)
  lazy val function = functionInfo.getFunctionClass.newInstance.asInstanceOf[UDFType]

  override def toString = s"${nodeName}#${functionInfo.getDisplayName}(${children.mkString(",")})"

  def unwrap(a: Any): Any = a match {
    case null => null
    case i: IntWritable => i.get
    case t: Text => t.toString
    case l: LongWritable => l.get
    case d: DoubleWritable => d.get()
    case d: org.apache.hadoop.hive.serde2.io.DoubleWritable => d.get
    case b: BooleanWritable => b.get()
    case list: java.util.List[_] => list.map(unwrap)
    case p: java.lang.Short => p
    case p: java.lang.Long => p
    case p: java.lang.Float => p
    case p: java.lang.Integer => p
    case p: java.lang.Double => p
    case p: java.lang.Byte => p
    case p: java.lang.Boolean => p
    case str: String => str
  }
}

case class HiveSimpleUdf(name: String, children: Seq[Expression]) extends HiveUdf {
  import HiveFunctionRegistry._
  type UDFType = UDF

  @transient
  lazy val method = function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))
  @transient
  lazy val dataType = javaClassToDataType(method.getReturnType)

  lazy val wrappers: Array[(Any) => AnyRef] = method.getParameterTypes.map { argClass =>
    val primitiveClasses = Seq(
      Integer.TYPE, classOf[java.lang.Integer], classOf[java.lang.String], java.lang.Double.TYPE,
      classOf[java.lang.Double], java.lang.Long.TYPE, classOf[java.lang.Long]
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
          case other: AnyRef => other
        }).asInstanceOf[AnyRef]
      }
    }
  }

  // TODO: Finish input output types.
  def evaluate(evaluatedChildren: Seq[Any]): Any = {
    // Wrap the function arguments in the expected types.
    val args = evaluatedChildren.zip(wrappers).map {
      case (arg, wrapper) => wrapper(arg)
    }

    // Invoke the udf and unwrap the result.
    unwrap(method.invoke(function, args: _*))
  }
}

case class HiveGenericUdf(
    name: String,
    dataType: DataType,
    children: Seq[Expression]) extends HiveUdf {
  import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._
  type UDFType = GenericUDF

  lazy val inspectors: Seq[AbstractPrimitiveJavaObjectInspector] = children.map(_.dataType).map {
    case StringType => PrimitiveObjectInspectorFactory.javaStringObjectInspector
    case IntegerType => PrimitiveObjectInspectorFactory.javaIntObjectInspector
    case DoubleType => PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
    case BooleanType => PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
    case LongType => PrimitiveObjectInspectorFactory.javaLongObjectInspector
    case ShortType => PrimitiveObjectInspectorFactory.javaShortObjectInspector
    case ByteType => PrimitiveObjectInspectorFactory.javaByteObjectInspector
  }

  lazy val instance = {
    function.initialize(inspectors.toArray)
    function
  }

  def wrap(a: Any): Any = a match {
    case s: String => new Text(s)
    case i: Int => i: java.lang.Integer
    case b: Boolean => b: java.lang.Boolean
    case d: Double => d: java.lang.Double
    case l: Long => l: java.lang.Long
    case l: Short => l: java.lang.Short
    case l: Byte => l: java.lang.Byte
    case s: Seq[_] => seqAsJavaList(s.map(wrap))
    case null => null
  }

  def evaluate(evaluatedChildren: Seq[Any]): Any = {
    val args = evaluatedChildren.map(wrap).map { v =>
      new DeferredJavaObject(v): DeferredObject
    }.toArray
    unwrap(instance.evaluate(args))
  }
}