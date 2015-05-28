package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.ExpressionBuilders.ExpressionBuilder

import scala.reflect.macros.Context
import scala.language.experimental.macros

private[catalyst] object ExpressionMacros {

  def expressionBuilder[T <: Expression]: ExpressionBuilder =
  macro ExpressionMacrosImpl.expressionImpl[T]

}

object ExpressionMacrosImpl {

  @DeveloperApi
  def expressionImpl[T <: Expression](c: Context): c.Expr[ExpressionBuilder] = {
    import c.universe._
    val ev1 = implicitly[c.WeakTypeTag[T]]
    ev1.tpe.declarations
      .filter(_.isMethod)
      .map(_.asMethod)
      .filter(_.isPrimaryConstructor)
      .flatMap({ methodSymbol =>
      methodSymbol.typeParams match {
        case Nil =>
          Some(Block(
            q"""if (expr.nonEmpty) { sys.error("Expressions takes no arguments:") }""",
            Apply(Ident(methodSymbol), Nil)
          ))
        case seq :: Nil if seq.asTerm == newTermName("Seq") =>
          Some(Block(

            Apply(Ident(methodSymbol), Ident(newTermName("expr")) :: Nil),
            Apply(Ident(methodSymbol), Ident(newTermName("expr")) :: Nil)
          ))
        case seq if seq.forall({ s => s.asTerm == newTermName("Expression") }) =>
          val args = (0 to seq.size).map({ i =>
            Apply(Select(Ident(newTermName("expr")), newTermName("apply")), Literal(Constant(i)) :: Nil)
          })
          val argNumber = Literal(Constant(args.size))
          val errorMsg = Literal(Constant(s"Expressions takes ${args.size}"))
          /* TODO: Add a check just in case there are input expressions without processing */
          Some(Block(
            q"""if (expr.size != 0) { sys.error($errorMsg) }""",
            Apply(Ident(methodSymbol), args.toList)
          ))
        case _ =>
          None
      }
    })
      .headOption match {
      case None =>
        sys.error("Expression generator requires a constructor accepting Expression... or Seq[Expression]")
      case Some(tree) =>
        c.Expr(q"(expr: Seq[Expression]) => { $tree }")
    }
  }

}
