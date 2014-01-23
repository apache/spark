package catalyst
package expressions

import errors._
import types._

/**
 * Performs evaluation of an expression tree, given a set of input tuples.
 */
object Evaluate extends Logging {
  def apply(e: Expression, input: Seq[Row]): Any = attachTree(e, "Expression Evaluation Failed") {
    def eval(e: Expression) = Evaluate(e, input)

    /**
     * A set of helper functions that return the correct descendant of [[scala.math.Numeric]] type and do any casting
     * necessary of child evaluation.
     *
     * Instead of matching here we could consider pushing the appropriate Fractional/Integral type into the type objects
     * themselves.
     */
    @inline
    def n1(e: Expression, f: ((Numeric[Any], Any) => Any)): Any  = {
      val evalE = eval(e)
      if (evalE == null)
        null
      else
        e.dataType match {
          case n: NumericType =>
            f.asInstanceOf[(Numeric[n.JvmType], n.JvmType) => n.JvmType](n.numeric, eval(e).asInstanceOf[n.JvmType])
          case other => sys.error(s"Type $other does not support numeric operations")
        }
    }

    @inline
    def n2(e1: Expression, e2: Expression, f: ((Numeric[Any], Any, Any) => Any)): Any  = {
      if (e1.dataType != e2.dataType)
        throw new OptimizationException(e, s"Data types do not match ${e1.dataType} != ${e2.dataType}")

      val evalE1 = eval(e1)
      val evalE2 = eval(e2)
      if (evalE1 == null || evalE2 == null)
        null
      else
        e1.dataType match {
          case n: NumericType =>
            f.asInstanceOf[(Numeric[n.JvmType], n.JvmType, n.JvmType) => Int](
              n.numeric, evalE1.asInstanceOf[n.JvmType], evalE2.asInstanceOf[n.JvmType])
          case other => sys.error(s"Type $other does not support numeric operations")
        }
    }

    @inline
    def f2(e1: Expression, e2: Expression, f: ((Fractional[Any], Any, Any) => Any)): Any  = {
      if (e1.dataType != e2.dataType)
        throw new OptimizationException(e, s"Data types do not match ${e1.dataType} != ${e2.dataType}")

      val evalE1 = eval(e1)
      val evalE2 = eval(e2)
      if (evalE1 == null || evalE2 == null)
        null
      else
        e1.dataType match {
          case f: FractionalType =>
            f.asInstanceOf[(Fractional[f.JvmType], f.JvmType, f.JvmType) => f.JvmType](
              f.fractional, evalE1.asInstanceOf[f.JvmType], evalE2.asInstanceOf[f.JvmType])
          case other => sys.error(s"Type $other does not support fractional operations")
        }
    }

    @inline
    def i2(e1: Expression, e2: Expression, f: ((Integral[Any], Any, Any) => Any)): Any  = {
      if (e1.dataType != e2.dataType)
        throw new OptimizationException(e, s"Data types do not match ${e1.dataType} != ${e2.dataType}")
      val evalE1 = eval(e1)
      val evalE2 = eval(e2)
      if (evalE1 == null || evalE2 == null)
        null
      else
        e1.dataType match {
          case i: IntegralType =>
            f.asInstanceOf[(Integral[i.JvmType], i.JvmType, i.JvmType) => i.JvmType](
              i.integral, evalE1.asInstanceOf[i.JvmType], evalE2.asInstanceOf[i.JvmType])
          case other => sys.error(s"Type $other does not support numeric operations")
        }
    }

    @inline def castOrNull[A](e: Expression, f: String => A) =
      try {
        eval(e) match {
          case null => null
          case s: String => f(s)
        }
      } catch { case _: java.lang.NumberFormatException => null }

    val result = e match {
      case Literal(v, _) => v

      /* Alias operations do not effect evaluation */
      case Alias(c, _) => eval(c)

      /* Aggregate functions are already computed so we just need to pull out the result */
      case af: AggregateFunction => af.result

      /* Arithmetic */
      case Add(l, r) => n2(l,r, _.plus(_, _))
      case Subtract(l, r) => n2(l,r, _.minus(_, _))
      case Multiply(l, r) => n2(l,r, _.times(_, _))
      // Divide implementation are different for fractional and integral dataTypes.
      case Divide(l @ FractionalType(), r)  => f2(l,r, _.div(_, _))
      case Divide(l @ IntegralType(), r) => i2(l,r, _.quot(_, _))
      // Remainder is only allowed on Integral types.
      case Remainder(l, r) => i2(l,r, _.rem(_, _))
      case UnaryMinus(child) => n1(child, _.negate(_))

      /* Control Flow */
      case If(e, t, f) => if (eval(e).asInstanceOf[Boolean]) eval(t) else eval(f)

      /* Comparisons */
      case Equals(l, r) =>
        val left = eval(l)
        val right = eval(r)
        if (left == null || right == null)
          null
        else
          left == right

      case In(value, list) =>
        val evaluatedValue = eval(value)
        list.exists(e => eval(e) == evaluatedValue)

      // Strings
      case GreaterThan(l, r) if l.dataType == StringType && r.dataType == StringType =>
        eval(l).asInstanceOf[String] > eval(r).asInstanceOf[String]
      case GreaterThanOrEqual(l, r) if l.dataType == StringType && r.dataType == StringType =>
        eval(l).asInstanceOf[String] >= eval(r).asInstanceOf[String]
      case LessThan(l, r) if l.dataType == StringType && r.dataType == StringType =>
        eval(l).asInstanceOf[String] < eval(r).asInstanceOf[String]
      case LessThanOrEqual(l, r) if l.dataType == StringType && r.dataType == StringType =>
        eval(l).asInstanceOf[String] <= eval(r).asInstanceOf[String]
      // Numerics
      case GreaterThan(l, r) => n2(l, r, _.gt(_, _))
      case GreaterThanOrEqual(l, r) => n2(l, r, _.gteq(_, _))
      case LessThan(l, r) => n2(l, r, _.lt(_, _))
      case LessThanOrEqual(l, r) => n2(l, r, _.lteq(_, _))

      case IsNull(e) => eval(e) == null
      case IsNotNull(e) => eval(e) != null
      case Coalesce(exprs) =>
        var currentExpression: Any = null
        var i = 0
        while (i < exprs.size && currentExpression == null) {
          currentExpression = eval(exprs(i))
          i += 1
        }
        currentExpression

      /* Casts */
      // toString
      case Cast(e, StringType) =>
        eval(e) match {
          case null => null
          case other => other.toString
        }

      // String => Numeric Types
      case Cast(e @ StringType(), IntegerType) => castOrNull(e, _.toInt)
      case Cast(e @ StringType(), DoubleType) => castOrNull(e, _.toDouble)
      case Cast(e @ StringType(), FloatType) => castOrNull(e, _.toFloat)
      case Cast(e @ StringType(), LongType) => castOrNull(e, _.toLong)
      case Cast(e @ StringType(), ShortType) => castOrNull(e, _.toShort)
      case Cast(e @ StringType(), ByteType) => castOrNull(e, _.toByte)
      case Cast(e @ StringType(), DecimalType) => castOrNull(e, BigDecimal(_))

      // Boolean conversions
      case Cast(e, ByteType) if e.dataType == BooleanType =>
        eval(e) match {
          case null => null
          case true => 1.toByte
          case false => 0.toByte
        }

      // Numeric Type => Numeric Type
      case Cast(e, IntegerType) => n1(e, _.toInt(_))
      case Cast(e, DoubleType) => n1(e, _.toDouble(_))
      case Cast(e, FloatType) => n1(e, _.toFloat(_))
      case Cast(e, LongType) => n1(e, _.toLong(_))
      case Cast(e, ShortType) => n1(e, _.toInt(_).toShort)
      case Cast(e, ByteType) => n1(e, _.toInt(_).toByte)
      case Cast(e, DecimalType) => n1(e, (n,v) => BigDecimal(n.toDouble(v)))

      /* Boolean Logic */
      case Not(c) =>
        eval(c) match {
          case null => null
          case b: Boolean => !b
        }

      case And(l,r) =>
        val left = eval(l)
        val right = eval(r)
        if (left == false || right == false)
          false
        else if (left == null || right == null )
          null
        else
          true
      case Or(l,r) =>
        val left = eval(l)
        val right = eval(r)
        if (left == true || right == true)
          true
        else if (left == null || right == null)
          null
        else
          false

      /* References to input tuples */
      case br @ BoundReference(inputTuple, ordinal, _) => try input(inputTuple)(ordinal) catch {
        case iob: IndexOutOfBoundsException =>
          throw new OptimizationException(br, s"Reference not in tuple: $input")
      }

      /* Functions */
      case Rand => scala.util.Random.nextDouble()

      /* UDFs */
      case implementedFunction: ImplementedUdf =>
        implementedFunction.evaluate(implementedFunction.children.map(eval))

      case a: Attribute =>
        throw new OptimizationException(a,
          "Unable to evaluate unbound reference without access to the input schema.")
      case other => throw new OptimizationException(other, "evaluation not implemented")
    }

    logger.debug(s"Evaluated $e => $result of type ${if (result == null) "null" else result.getClass.getName}, expected: ${e.dataType}")
    result
  }
}