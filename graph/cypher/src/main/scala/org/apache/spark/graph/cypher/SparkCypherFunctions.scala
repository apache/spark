package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.conversions.ExprConversions._
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.types.CTNull
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.reflect.runtime.universe.TypeTag

object SparkCypherFunctions {

  val NULL_LIT: Column = lit(null)
  val TRUE_LIT: Column = lit(true)
  val FALSE_LIT: Column = lit(false)
  val ONE_LIT: Column = lit(1)
  val E_LIT: Column = lit(Math.E)
  val PI_LIT: Column = lit(Math.PI)
  // See: https://issues.apache.org/jira/browse/SPARK-20193
  val EMPTY_STRUCT: Column = udf(() => new GenericRowWithSchema(Array(), StructType(Nil)), StructType(Nil))()

  implicit class RichColumn(column: Column) {

    /**
      * This is a copy of {{{org.apache.spark.sql.Column#getItem}}}. The original method only allows fixed
      * values (Int, or String) as index although the underlying implementation seem capable of processing arbitrary
      * expressions. This method exposes these features
      */
    def get(idx: Column): Column =
      new Column(UnresolvedExtractValue(column.expr, idx.expr))
  }


  def list_slice(list: Column, maybeFrom: Option[Column], maybeTo: Option[Column]): Column = {
    val start = maybeFrom.map(_ + ONE_LIT).getOrElse(ONE_LIT)
    val length = (maybeTo.getOrElse(size(list)) - start) + ONE_LIT
    new Column(Slice(list.expr, start.expr, length.expr))
  }

  /**
    * Alternative version of `array_contains` that takes a column as the value.
    */
  def array_contains(column: Column, value: Column): Column =
    new Column(ArrayContains(column.expr, value.expr))

  def hash64(columns: Column*): Column =
    new Column(new XxHash64(columns.map(_.expr)))

  def regex_match(text: Column, pattern: Column): Column = new Column(RLike(text.expr, pattern.expr))

  def get_array_item(array: Column, index: Int): Column = {
    new Column(GetArrayItem(array.expr, functions.lit(index).expr))
  }

  private val x: NamedLambdaVariable = NamedLambdaVariable("x", StructType(Seq(StructField("item", StringType), StructField("flag", BooleanType))), nullable = false)
  private val TRUE_EXPR: Expression = functions.lit(true).expr

  def filter_true[T: TypeTag](items: Seq[T], mask: Seq[Column]): Column = {
    filter_with_mask(items, mask, LambdaFunction(EqualTo(GetStructField(x, 1), TRUE_EXPR), Seq(x)))
  }

  def filter_not_null[T: TypeTag](items: Seq[T], mask: Seq[Column]): Column = {
    filter_with_mask(items, mask, LambdaFunction(IsNotNull(GetStructField(x, 1)), Seq(x)))
  }

  private def filter_with_mask[T: TypeTag](items: Seq[T], mask: Seq[Column], predicate: LambdaFunction): Column = {
    require(items.size == mask.size, s"Array filtering requires for the items and the mask to have the same length.")
    if (items.isEmpty) {
      functions.array()
    } else {
      val itemLiterals = functions.array(items.map(functions.typedLit): _*)
      val zippedArray = functions.arrays_zip(itemLiterals, functions.array(mask: _*))
      val filtered = ArrayFilter(zippedArray.expr, predicate)
      val transform = ArrayTransform(filtered, LambdaFunction(GetStructField(x, 0), Seq(x)))
      new Column(transform)
    }
  }

  // See: https://issues.apache.org/jira/browse/SPARK-20193
  def create_struct(structColumns: Seq[Column]): Column = {
    if (structColumns.isEmpty) EMPTY_STRUCT
    else struct(structColumns: _*)
  }

  def switch(branches: Seq[(Column, Column)], maybeDefault: Option[Column]): Column = {
    new Column(CaseWhen(branches.map { case (c, v) => c.expr -> v.expr } , maybeDefault.map(_.expr)))
  }

  /**
    * Alternative version of {{{org.apache.spark.sql.functions.translate}}} that takes {{{org.apache.spark.sql.Column}}}s for search and replace strings.
    */
  def translate(src: Column, matchingString: Column, replaceString: Column): Column = {
    new Column(StringTranslate(src.expr, matchingString.expr, replaceString.expr))
  }

  /**
    * Converts `expr` with the `withConvertedChildren` function, which is passed the converted child expressions as its
    * argument.
    *
    * Iff the expression has `expr.nullInNullOut == true`, then any child being mapped to `null` will also result in
    * the parent expression being mapped to null.
    *
    * For these expressions the `withConvertedChildren` function is guaranteed to not receive any `null`
    * values from the evaluated children.
    */
  def null_safe_conversion(expr: Expr)(withConvertedChildren: Seq[Column] => Column)
    (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
    if (expr.cypherType == CTNull) {
      NULL_LIT
    } else {
      val evaluatedArgs = expr.children.map(_.asSparkSQLExpr)
      val withConvertedChildrenResult = withConvertedChildren(evaluatedArgs).expr
      if (expr.children.nonEmpty && expr.nullInNullOut && expr.cypherType.isNullable) {
        val nullPropagationCases = evaluatedArgs.map(_.isNull.expr).zip(Seq.fill(evaluatedArgs.length)(NULL_LIT.expr))
        new Column(CaseWhen(nullPropagationCases, withConvertedChildrenResult))
      } else {
        new Column(withConvertedChildrenResult)
      }
    }
  }

  def column_for(expr: Expr)(implicit header: RecordHeader, df: DataFrame): Column = {
    val columnName = header.getColumn(expr).getOrElse(throw IllegalArgumentException(
      expected = s"Expression in ${header.expressions.mkString("[", ", ", "]")}",
      actual = expr)
    )
    if (df.columns.contains(columnName)) {
      df.col(columnName)
    } else {
      NULL_LIT
    }
  }

  implicit class CypherValueConversion(val v: CypherValue) extends AnyVal {

    def toSparkLiteral: Column = {
      v.cypherType.ensureSparkCompatible()
      v match {
        case list: CypherList => array(list.value.map(_.toSparkLiteral): _*)
        case map: CypherMap => create_struct(
          map.value.map { case (key, value) =>
            value.toSparkLiteral.as(key.toString)
          }.toSeq
        )
        case _ => lit(v.unwrap)
      }
    }

  }

}

