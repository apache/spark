package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.util.{Success, Failure, Try}

object ExpressionBuilders {

  type ExpressionBuilder = (Seq[Expression]) => Expression

  private def camelToUnderscores(name: String) = "[A-Z\\d]".r
    .replaceAllIn(name, { m => "_" + m.group(0).toLowerCase(Locale.ENGLISH) })

  def expression[T <: Expression](name: String)(implicit ev: ClassTag[T]): (String, ExpressionBuilder) =
    name -> expressionByReflection[T]

  /* TODO: Substring needs change so that it accepts a 2-arg constructor */
  def expression[T <: Expression](implicit ev: ClassTag[T])
  : (String, ExpressionBuilder) = {
    val name = camelToUnderscores(ev.runtimeClass.getSimpleName)
    /* XXX: With macros: name -> ExpressionMacros.expressionBuilder[T] */
    name -> expressionByReflection[T]
  }

  private def expressionByReflection[T <: Expression](implicit tag: ClassTag[T]): ExpressionBuilder = {
    val constructors = tag.runtimeClass.getDeclaredConstructors.toSeq
    (expressions: Seq[Expression]) => {
      val arity = expressions.size
      val validBuilders = constructors.flatMap { c =>
        val parameterTypes = c.getParameterTypes
        if (parameterTypes.size == arity &&
          parameterTypes.forall(_.getClasses.contains(classOf[Expression]))) {
          Some(expressionFixedArity[T](arity))
        } else if (parameterTypes.size == 1 && parameterTypes(0).getClass == classOf[Seq[Expression]]) {
          Some(expressionVariableArity[T])
        } else {
          None
        }
      }
      val builder = validBuilders.head
      builder(expressions)
    }
  }

  private def expressionVariableArity[T <: Expression](implicit tag: ClassTag[T]): ExpressionBuilder = {
    val argTypes = classOf[Seq[Expression]]
    val clazz = tag.runtimeClass
    val constructor = Try(clazz.getDeclaredConstructor(argTypes)) match {
      case Failure(ex : NoSuchMethodException) =>
        sys.error(s"Did not find a constructor with Seq[Expression] for ${clazz.getCanonicalName}")
      case Failure(ex) => throw ex
      case Success(c) => c
    }
    (expressions: Seq[Expression]) => {
      constructor.newInstance(expressions).asInstanceOf[Expression]
    }
  }

  private def expressionFixedArity[T <: Expression](arity: Int)(implicit tag: ClassTag[T]): ExpressionBuilder = {
    val argTypes = (1 to arity).map(x => classOf[Expression])
    val constructor = tag.runtimeClass.getDeclaredConstructor(argTypes: _*)
    (expressions: Seq[Expression]) => {
      if (expressions.size != arity) {
        throw new IllegalArgumentException(
          s"Invalid number of arguments: ${expressions.size} (must be equal to $arity)"
        )
      }
      constructor.newInstance(expressions: _*).asInstanceOf[Expression]
    }
  }

}
