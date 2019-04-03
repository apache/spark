package org.apache.spark.cypher.conversions

import org.apache.spark.cypher.SparkCypherFunctions._
import org.apache.spark.cypher.conversions.TemporalConversions._
import org.apache.spark.cypher.conversions.TypeConversions._
import org.apache.spark.cypher.udfs.TemporalUdfs
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.functions.{array_contains => _, translate => _, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper._
import org.opencypher.okapi.impl.temporal.{Duration => DurationValue}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object ExprConversions {

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
  def nullSafeConversion(expr: Expr)(withConvertedChildren: Seq[Column] => Column)
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

  implicit class RichExpression(expr: Expr) {

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header     the header of the CAPSRecords in which the expression should be evaluated.
      * @param df         the dataframe containing the data over which the expression should be evaluated.
      * @param parameters query parameters
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
      val outCol = expr match {
        // Evaluate based on already present data; no recursion
        case _: Var | _: HasLabel | _: HasType | _: StartNode | _: EndNode => column_for(expr)
        // Evaluate bottom-up
        case _ => nullSafeConversion(expr)(convert)
      }
      header.getColumn(expr) match {
        case None => outCol
        case Some(colName) => outCol.as(colName)
      }
    }

    private def convert(convertedChildren: Seq[Column])
      (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {

      def child0: Column = convertedChildren.head

      def child1: Column = convertedChildren(1)

      def child2: Column = convertedChildren(2)

      expr match {
        case _: ListLit => array(convertedChildren: _*)
        case l: Lit[_] => lit(l.v)
        case _: AliasExpr => child0
        case Param(name) => parameters(name).toSparkLiteral

        // Predicates
        case _: Equals => child0 === child1
        case _: Not => !child0
        case Size(e) => {
          e.cypherType match {
            case CTString => length(child0)
            case _ => size(child0) // it's a list
          }
        }.cast(LongType)
        case _: Ands => convertedChildren.foldLeft(TRUE_LIT)(_ && _)
        case _: Ors => convertedChildren.foldLeft(FALSE_LIT)(_ || _)
        case _: IsNull => child0.isNull
        case _: IsNotNull => child0.isNotNull
        case _: Exists => child0.isNotNull
        case _: LessThan => child0 < child1
        case _: LessThanOrEqual => child0 <= child1
        case _: GreaterThanOrEqual => child0 >= child1
        case _: GreaterThan => child0 > child1

        case _: StartsWith => child0.startsWith(child1)
        case _: EndsWith => child0.endsWith(child1)
        case _: Contains => child0.contains(child1)
        case _: RegexMatch => regex_match(child0, child1)

        // Other
        case Explode(list) => list.cypherType match {
          case CTNull => explode(NULL_LIT.cast(ArrayType(NullType)))
          case _ => explode(child0)
        }

        case Property(e, PropertyKey(key)) =>
          // TODO: Convert property lookups into separate specific lookups instead of overloading
          e.cypherType.material match {
            case CTMap(inner) => if (inner.keySet.contains(key)) child0.getField(key) else NULL_LIT
            case CTDate => temporalAccessor[java.sql.Date](child0, key)
            case CTLocalDateTime => temporalAccessor[java.sql.Timestamp](child0, key)
            case CTDuration => TemporalUdfs.durationAccessor(key.toLowerCase).apply(child0)
            case _ =>
              if (!header.contains(expr)) {
                NULL_LIT
              } else {
                column_for(expr)
              }
          }
        case LocalDateTime(dateExpr) =>
          // TODO: Move code outside of expr mapper
          dateExpr match {
            case Some(e) =>
              val localDateTimeValue = resolveTemporalArgument(e)
                .map(parseLocalDateTime)
                .map(java.sql.Timestamp.valueOf)
                .map {
                  case ts if ts.getNanos % 1000 == 0 => ts
                  case _ => throw IllegalStateException("Spark does not support nanosecond resolution in 'localdatetime'")
                }
                .orNull

              lit(localDateTimeValue).cast(DataTypes.TimestampType)
            case None => current_timestamp()
          }

        case Date(dateExpr) =>
          // TODO: Move code outside of expr mapper
          dateExpr match {
            case Some(e) =>
              val dateValue = resolveTemporalArgument(e)
                .map(parseDate)
                .map(java.sql.Date.valueOf)
                .orNull

              lit(dateValue).cast(DataTypes.DateType)
            case None => current_timestamp()
          }

        case Duration(durationExpr) =>
          // TODO: Move code outside of expr mapper
          val durationValue = resolveTemporalArgument(durationExpr).map {
            case Left(m) => DurationValue(m.mapValues(_.toLong)).toCalendarInterval
            case Right(s) => DurationValue.parse(s).toCalendarInterval
          }.orNull
          lit(durationValue)

        case In(lhs, rhs) => rhs.cypherType.material match {
          case CTList(CTVoid) => FALSE_LIT
          case CTList(inner) if inner.couldBeSameTypeAs(lhs.cypherType) => array_contains(child1, child0)
          case _ => NULL_LIT
        }

        // Arithmetic
        case Add(lhs, rhs) =>
          val lhsCT = lhs.cypherType.material
          val rhsCT = rhs.cypherType.material
          lhsCT -> rhsCT match {
            case (CTList(lhInner), CTList(rhInner)) =>
              if (lhInner.material == rhInner.material || lhInner == CTVoid || rhInner == CTVoid) {
                concat(child0, child1)
              } else {
                throw NotImplementedException(s"Lists of different inner types are not supported (${lhInner.material}, ${rhInner.material})")
              }
            case (CTList(inner), nonListType) if nonListType == inner.material || inner.material == CTVoid => concat(child0, array(child1))
            case (nonListType, CTList(inner)) if inner.material == nonListType || inner.material == CTVoid => concat(array(child0), child1)
            case (CTString, _) if rhsCT.subTypeOf(CTNumber) => concat(child0, child1.cast(StringType))
            case (_, CTString) if lhsCT.subTypeOf(CTNumber) => concat(child0.cast(StringType), child1)
            case (CTString, CTString) => concat(child0, child1)
            case (CTDate, CTDuration) => TemporalUdfs.dateAdd(child0, child1)
            case _ => child0 + child1
          }

        case Subtract(lhs, rhs) if lhs.cypherType.material.subTypeOf(CTDate) && rhs.cypherType.material.subTypeOf(CTDuration) =>
          TemporalUdfs.dateSubtract(child0, child1)

        case _: Subtract => child0 - child1

        case _: Multiply => child0 * child1
        case div: Divide => (child0 / child1).cast(div.cypherType.getSparkType)

        // Id functions
        case _: Id => child0

        // Functions
        case _: MonotonicallyIncreasingId => monotonically_increasing_id()
        case Labels(e) =>
          val possibleLabels = header.labelsFor(e.owner.get).toSeq.sortBy(_.label.name)
          val labelBooleanFlagsCol = possibleLabels.map(_.asSparkSQLExpr)
          val nodeLabels = filter_true(possibleLabels.map(_.label.name), labelBooleanFlagsCol)
          nodeLabels

        case Type(e) =>
          val possibleRelTypes = header.typesFor(e.owner.get).toSeq.sortBy(_.relType.name)
          val relTypeBooleanFlagsCol = possibleRelTypes.map(_.asSparkSQLExpr)
          val relTypes = filter_true(possibleRelTypes.map(_.relType.name), relTypeBooleanFlagsCol)
          val relType = get_array_item(relTypes, index = 0)
          relType

        case Keys(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val possibleProperties = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyNames = possibleProperties.map(_.key.name)
              val propertyValues = possibleProperties.map(_.asSparkSQLExpr)
              filter_not_null(propertyNames, propertyValues)

            case CTMap(inner) =>
              val mapColumn = child0
              val (propertyKeys, propertyValues) = inner.keys.map { e =>
                // Whe have to make sure that every column has the same type (true or null)
                e -> when(mapColumn.getField(e).isNotNull, TRUE_LIT).otherwise(NULL_LIT)
              }.toSeq.unzip
              filter_not_null(propertyKeys, propertyValues)

            case other => throw IllegalArgumentException("an Expression with type CTNode, CTRelationship or CTMap", other)
          }

        case Properties(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val propertyExpressions = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyColumns = propertyExpressions
                .map(propertyExpression => propertyExpression.asSparkSQLExpr.as(propertyExpression.key.name))
              create_struct(propertyColumns)
            case _: CTMap => child0
            case other =>
              throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
          }

        case StartNodeFunction(e) => header.startNodeFor(e.owner.get).asSparkSQLExpr
        case EndNodeFunction(e) => header.endNodeFor(e.owner.get).asSparkSQLExpr

        case _: ToFloat => child0.cast(DoubleType)
        case _: ToInteger => child0.cast(IntegerType)
        case _: ToString => child0.cast(StringType)
        case _: ToBoolean => child0.cast(BooleanType)

        case _: Trim => trim(child0)
        case _: LTrim => ltrim(child0)
        case _: RTrim => rtrim(child0)
        case _: ToUpper => upper(child0)
        case _: ToLower => lower(child0)

        case _: Range => sequence(child0, child1, convertedChildren.lift(2).getOrElse(ONE_LIT))

        case _: Replace => translate(child0, child1, child2)

        case _: Substring => child0.substr(child1 + ONE_LIT, convertedChildren.lift(2).getOrElse(length(child0) - child1))

        // Mathematical functions
        case E => E_LIT
        case Pi => PI_LIT

        case _: Sqrt => sqrt(child0)
        case _: Log => log(child0)
        case _: Log10 => log(10.0, child0)
        case _: Exp => exp(child0)
        case _: Abs => abs(child0)
        case _: Ceil => ceil(child0).cast(DoubleType)
        case _: Floor => floor(child0).cast(DoubleType)
        case Rand => rand()
        case _: Round => round(child0).cast(DoubleType)
        case _: Sign => signum(child0).cast(IntegerType)

        case _: Acos => acos(child0)
        case _: Asin => asin(child0)
        case _: Atan => atan(child0)
        case _: Atan2 => atan2(child0, child1)
        case _: Cos => cos(child0)
        case Cot(e) => Divide(IntegerLit(1), Tan(e))(CTFloat).asSparkSQLExpr
        case _: Degrees => degrees(child0)
        case Haversin(e) => Divide(Subtract(IntegerLit(1), Cos(e))(CTFloat), IntegerLit(2))(CTFloat).asSparkSQLExpr
        case _: Radians => radians(child0)
        case _: Sin => sin(child0)
        case _: Tan => tan(child0)

        // Time functions
        case Timestamp => current_timestamp().cast(LongType)

        // Bit operations
        case _: BitwiseAnd => child0.bitwiseAND(child1)
        case _: BitwiseOr => child0.bitwiseOR(child1)
        case ShiftLeft(_, IntegerLit(shiftBits)) => shiftLeft(child0, shiftBits.toInt)
        case ShiftRightUnsigned(_, IntegerLit(shiftBits)) => shiftRightUnsigned(child0, shiftBits.toInt)

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          coalesce(columns: _*)

        case CaseExpr(_, maybeDefault) =>
          val (maybeConvertedDefault, convertedAlternatives) = if (maybeDefault.isDefined) {
            Some(convertedChildren.head) -> convertedChildren.tail
          } else {
            None -> convertedChildren
          }
          val indexed = convertedAlternatives.zipWithIndex
          val conditions = indexed.collect { case (c, i) if i % 2 == 0 => c}
          val values = indexed.collect { case (c, i) if i % 2 == 1 => c}
          val branches = conditions.zip(values)
          switch(branches, maybeConvertedDefault)

        case ContainerIndex(container, index) =>
          val indexCol = index.asSparkSQLExpr
          val containerCol = container.asSparkSQLExpr

          container.cypherType.material match {
            case _: CTList | _: CTMap => containerCol.get(indexCol)
            case other => throw NotImplementedException(s"Accessing $other by index is not supported")
          }

        case _: ListSliceFromTo => list_slice(child0, Some(child1), Some(child2))
        case _: ListSliceFrom => list_slice(child0, Some(child1), None)
        case _: ListSliceTo => list_slice(child0, None, Some(child1))

        case MapExpression(items) => expr.cypherType.material match {
          case CTMap(_) =>
            val innerColumns = items.map {
              case (key, innerExpr) => innerExpr.asSparkSQLExpr.as(key)
            }.toSeq
            create_struct(innerColumns)
          case other => throw IllegalArgumentException("an expression of type CTMap", other)
        }

        // Aggregators
        case Count(_, distinct) =>
          if (distinct) countDistinct(child0)
          else count(child0)

        case Collect(_, distinct) =>
          if (distinct) collect_set(child0)
          else collect_list(child0)

        case CountStar => count(ONE_LIT)
        case _: Avg => avg(child0)
        case _: Max => max(child0)
        case _: Min => min(child0)
        case _: Sum => sum(child0)

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }

  }
}

