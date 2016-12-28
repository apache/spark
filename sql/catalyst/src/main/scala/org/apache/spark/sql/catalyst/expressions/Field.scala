package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AtomicType, DataType, IntegerType}
/**
  * A function that returns the index of str in (str1, str2, ...) list or 0 if not found.
  * It takes at least 2 parameters, and all parameters' types should be subtypes of AtomicTppe
  */
@ExpressionDescription(
  usage = "_FUNC_(str, str1, str2, ...) - Returns the index of str in the str1,str2,... list or 0 if not found.",
  extended = """
    Examples:
      > SELECT _FUNC_(10, 9, 3, 10, 4);
       3
             """)
case class Field(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(children(0).dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires at least 2 arguments")
    }
    else if (!children.forall(_.dataType.isInstanceOf[AtomicType])) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires all arguments to be of AtomicType")
    }
    else
      TypeCheckResult.TypeCheckSuccess
  }

  override def dataType: DataType = IntegerType

  override def eval(input: InternalRow): Int = {
    val target = children.head.eval(input)
    val targetDataType = children.head.dataType
    def findEqual(target: Any, params: Seq[Expression], index: Int): Int = {
      params match {
        case Nil => 0
        case head::tail if targetDataType == head.dataType && ordering.equiv(target, head.eval(input)) => index
        case _ => findEqual(target, params.tail, index + 1)
      }
    }
    findEqual(target, children.tail, 1)
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val target = evalChildren(0)
    val targetDataType = children(0).dataType
    val rest = evalChildren.drop(1)
    val restDataType = children.drop(1).map(_.dataType)

    def updateEval(evalWithIndex: Tuple2[Tuple2[ExprCode, DataType], Int]): String = {
      val ((eval, dataType), index) = evalWithIndex
      s"""
        ${eval.code}
        if (${dataType.equals(targetDataType)} && ${ctx.genEqual(targetDataType, eval.value, target.value)}) {
          ${ev.value} = ${index};
        }
      """
    }

    def genIfElseStructure(code1: String, code2: String): String = {
      s"""
         ${code1}
         else {
          ${code2}
         }
       """
    }

    def dataTypeEqualsTarget(evalWithIndex: Tuple2[Tuple2[ExprCode, DataType], Int]): Boolean = {
      val ((eval, dataType), index) = evalWithIndex
      if (dataType.equals(targetDataType))
        true
      else
        false
    }

    ev.copy(code = s"""
      ${target.code}
      boolean ${ev.isNull} = false;
      int ${ev.value} = 0;
      ${rest.zip(restDataType).zipWithIndex.map(x => (x._1, x._2 + 1)).filter(dataTypeEqualsTarget).map(updateEval).reduceRight(genIfElseStructure)}
      """)
  }
}