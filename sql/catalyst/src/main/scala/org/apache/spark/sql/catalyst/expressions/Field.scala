package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AtomicType, DataType, IntegerType, NullType}
/**
  * A function that returns the greatest value of all parameters, skipping null values.
  * It takes at least 2 parameters, and returns null iff all parameters are null.
  */

case class Field(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.nullable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(children(0).dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires at least 2 arguments")
    }
    else if (children.map(_.dataType).count(t => (!classOf[AtomicType].isAssignableFrom(t.getClass()))) == 0) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires all arguments to be of AtomicType")
    }
    else
      TypeCheckResult.TypeCheckSuccess
  }
  override def dataType: DataType = IntegerType
  override def eval(input: InternalRow): Int = {
    val param1 = children.head.eval(input)
    val targetDataType = children.head.dataType
    def findEqual(target: Any, params: Seq[Expression], index: Int): Int = {
      params match {
        case Nil => 0
        case head::tail if targetDataType == head.dataType && ordering.equiv(target, head.eval(input)) => index
        case _ => findEqual(target, params.tail, index + 1)
      }
    }
    findEqual(param1, children.tail, 1)
  }
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val target = evalChildren(0)
    val rest = evalChildren.drop(1)

//    def updateEval(eval: ExprCode): String = {
//      s"""
//        ${eval.code}
//        if (${target.isNull}) {
//          if(${eval.isNull}) {
//            ${ev.value}
//          }
//        }
//        if (!${eval.isNull} && (${ev.isNull} ||
//          ${ctx.genGreater(dataType, eval.value, ev.value)})) {
//          ${ev.value} = ${eval.value};
//        }
//      """
//    }

    ev.copy(code = s"""
      ${target.code}
      boolean ${ev.isNull} = false;
      int ${ev.value} = 2;
      """)
  }

}
