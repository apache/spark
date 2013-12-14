package catalyst
package shark2

import org.apache.hadoop.hive.ql.exec.{FunctionInfo, FunctionRegistry}

import expressions._
import types._
import org.apache.hadoop.io.{DoubleWritable, LongWritable, IntWritable, Text}

import collection.JavaConversions._

object HiveFunctionRegistry extends analysis.FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // We only look it up to see if it exists, but do not include it in the HiveUDF since it is not always serializable.
    FunctionRegistry.getFunctionInfo(name)
    // TODO: Check that the types match up.
    HiveUdf(name, IntegerType, children)
  }
}

case class HiveUdf(
    name: String,
    dataType: DataType,
    children: Seq[Expression]) extends Expression with ImplementedUdf {
  def nullable = true
  def references = children.flatMap(_.references).toSet

  // FunctionInfo is not serializable so we must look it up here again.
  lazy val functionInfo = FunctionRegistry.getFunctionInfo(name)
  lazy val function = functionInfo.getFunctionClass.newInstance.asInstanceOf[org.apache.hadoop.hive.ql.exec.UDF]
  lazy val method = function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo))

  // TODO: Finish input output types.
  def evaluate(evaluatedChildren: Seq[Any]): Any = {
    // Wrap the function arguments in the expected types.
    val args = evaluatedChildren.zip(method.getParameterTypes).map {
      case (null, _) => null
      case (arg: Double, argClass) if argClass.isAssignableFrom(classOf[DoubleWritable]) =>
        new DoubleWritable(arg)
      case (arg: Double, argClass) if argClass.isAssignableFrom(classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable]) =>
        new org.apache.hadoop.hive.serde2.io.DoubleWritable(arg)
      case (arg: Int, argClass) if argClass.isAssignableFrom(classOf[org.apache.hadoop.hive.serde2.io.DoubleWritable]) =>
        new org.apache.hadoop.hive.serde2.io.DoubleWritable(arg)
      case (arg: Int, argClass) if argClass.isAssignableFrom(classOf[IntWritable]) =>
        new IntWritable(arg)
      case (arg, argClass) =>
        argClass.getConstructor(arg.getClass).newInstance(arg.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    }.toArray

    // Invoke the udf and unwrap the result.
    method.invoke(function, args: _*) match {
      case i: IntWritable => i.get
      case t: Text => t.toString
      case l: LongWritable => l.get
      case null => null
      case other => other
    }
  }

  override def toString = s"${functionInfo.getDisplayName}(${children.mkString(",")})"
}