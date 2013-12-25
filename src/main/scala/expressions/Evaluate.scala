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
    def n1(e: Expression, f: ((Numeric[Any], Any) => Any)): Any  = e.dataType match {
      case IntegerType =>
        f.asInstanceOf[(Numeric[Int], Int) => Int](implicitly[Numeric[Int]], eval(e).asInstanceOf[Int])
      case DoubleType =>
         f.asInstanceOf[(Numeric[Double], Double) => Double](implicitly[Numeric[Double]], eval(e).asInstanceOf[Double])
      case LongType =>
         f.asInstanceOf[(Numeric[Long], Long) => Long](implicitly[Numeric[Long]], eval(e).asInstanceOf[Long])
      case FloatType =>
         f.asInstanceOf[(Numeric[Float], Float) => Float](implicitly[Numeric[Float]], eval(e).asInstanceOf[Float])
      case ByteType =>
         f.asInstanceOf[(Numeric[Byte], Byte) => Byte](implicitly[Numeric[Byte]], eval(e).asInstanceOf[Byte])
      case ShortType =>
         f.asInstanceOf[(Numeric[Short], Short) => Short](implicitly[Numeric[Short]], eval(e).asInstanceOf[Short])
      case other => sys.error(s"Type $other does not support numeric operations")
    }

    @inline
    def n2(e1: Expression, e2: Expression, f: ((Numeric[Any], Any, Any) => Any)): Any  = {
      assert(e1.dataType == e2.dataType, s"Data types do not match ${e1.dataType} != ${e2.dataType}")
      e1.dataType match {
        case IntegerType =>
          f.asInstanceOf[(Numeric[Int], Int, Int) => Int](implicitly[Numeric[Int]], eval(e1).asInstanceOf[Int], eval(e2).asInstanceOf[Int])
        case DoubleType =>
          f.asInstanceOf[(Numeric[Double], Double, Double) => Double](implicitly[Numeric[Double]], eval(e1).asInstanceOf[Double], eval(e2).asInstanceOf[Double])
        case LongType =>
          f.asInstanceOf[(Numeric[Long], Long, Long) => Long](implicitly[Numeric[Long]], eval(e1).asInstanceOf[Long], eval(e2).asInstanceOf[Long])
        case FloatType =>
          f.asInstanceOf[(Numeric[Float], Float, Float) => Float](implicitly[Numeric[Float]], eval(e1).asInstanceOf[Float], eval(e2).asInstanceOf[Float])
        case ByteType =>
          f.asInstanceOf[(Numeric[Byte], Byte, Byte) => Byte](implicitly[Numeric[Byte]], eval(e1).asInstanceOf[Byte], eval(e2).asInstanceOf[Byte])
        case ShortType =>
          f.asInstanceOf[(Numeric[Short], Short, Short) => Short](implicitly[Numeric[Short]], eval(e1).asInstanceOf[Short], eval(e2).asInstanceOf[Short])
        case other => sys.error(s"Type $other does not support numeric operations")
      }
    }

    @inline
    def f2(e1: Expression, e2: Expression, f: ((Fractional[Any], Any, Any) => Any)): Any  = {
      assert(e1.dataType == e2.dataType, s"Data types do not match ${e1.dataType} != ${e2.dataType}")
      e1.dataType match {
        case DoubleType =>
          f.asInstanceOf[(Fractional[Double], Double, Double) => Double](implicitly[Fractional[Double]], eval(e1).asInstanceOf[Double], eval(e2).asInstanceOf[Double])
        case FloatType =>
          f.asInstanceOf[(Fractional[Float], Float, Float) => Float](implicitly[Fractional[Float]], eval(e1).asInstanceOf[Float], eval(e2).asInstanceOf[Float])
        case other => sys.error(s"Type $other does not support fractional operations")
      }
    }

    @inline
    def i2(e1: Expression, e2: Expression, f: ((Integral[Any], Any, Any) => Any)): Any  = {
      assert(e1.dataType == e2.dataType, s"Data types do not match ${e1.dataType} != ${e2.dataType}")
      e1.dataType match {
        case IntegerType =>
          f.asInstanceOf[(Integral[Int], Int, Int) => Int](implicitly[Integral[Int]], eval(e1).asInstanceOf[Int], eval(e2).asInstanceOf[Int])
        case LongType =>
          f.asInstanceOf[(Integral[Long], Long, Long) => Long](implicitly[Integral[Long]], eval(e1).asInstanceOf[Long], eval(e2).asInstanceOf[Long])
        case ByteType =>
          f.asInstanceOf[(Integral[Byte], Byte, Byte) => Byte](implicitly[Integral[Byte]], eval(e1).asInstanceOf[Byte], eval(e2).asInstanceOf[Byte])
        case ShortType =>
          f.asInstanceOf[(Integral[Short], Short, Short) => Short](implicitly[Integral[Short]], eval(e1).asInstanceOf[Short], eval(e2).asInstanceOf[Short])
        case other => sys.error(s"Type $other does not support numeric operations")
      }
    }

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
      // Divide & remainder implementation are different for fractional and integral dataTypes.
      case Divide(l, r) if(l.dataType == DoubleType || l.dataType == FloatType) => f2(l,r, _.div(_, _))
      case Divide(l, r) => i2(l,r, _.quot(_, _))
      // Remainder is only allowed on Integral types.
      case Remainder(l, r) => i2(l,r, _.rem(_, _))
      case UnaryMinus(child) => n1(child, _.negate(_))

      /* Comparisons */
      case Equals(l, r) =>
        val left = eval(l)
        val right = eval(r)
        if(left == null || right == null)
          null
        else
          left == right

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

      /* Casts */
      // toString
      case Cast(e, StringType) => eval(e).toString
      // String => Numeric Types
      case Cast(e, IntegerType) if e.dataType == StringType => eval(e).asInstanceOf[String].toInt
      case Cast(e, DoubleType) if e.dataType == StringType => eval(e).asInstanceOf[String].toDouble
      // Numeric Type => Numeric Type
      case Cast(e, IntegerType) => n1(e, _.toInt(_))
      case Cast(e, DoubleType) => n1(e, _.toDouble(_))
      case Cast(e, FloatType) => n1(e, _.toFloat(_))
      case Cast(e, LongType) => n1(e, _.toLong(_))
      case Cast(e, ShortType) => n1(e, _.toInt(_).toShort)
      case Cast(e, ByteType) => n1(e, _.toInt(_).toByte)

      /* Boolean Logic */
      case Not(c) =>
        val child = eval(c)
        if(child == null)
          null
        else
          !child.asInstanceOf[Boolean]
      case And(l,r) =>
        val left = eval(l)
        val right = eval(r)
        if(left == false || right == false)
          false
        else if(left == null || right == null )
          null
        else
          true
      case Or(l,r) =>
        val left = eval(l)
        val right = eval(r)
        if(left == true || right == true)
          true
        else if(left == null || right == null)
          null
        else
          false

      /* References to input tuples */
      case br @ BoundReference(inputTuple, ordinal, _) => try input(inputTuple)(ordinal) catch {
        case iob: IndexOutOfBoundsException => throw new OptimizationException(br, s"Reference not in tuple: $input")
      }

      /* Functions */
      case Rand => scala.util.Random.nextDouble

      /* UDFs */
      case implementedFunction: ImplementedUdf => implementedFunction.evaluate(implementedFunction.children.map(eval))

      case other => throw new OptimizationException(other, "evaluation not implemented")
    }

    logger.debug(s"Evaluated $e => $result")
    result
  }
}