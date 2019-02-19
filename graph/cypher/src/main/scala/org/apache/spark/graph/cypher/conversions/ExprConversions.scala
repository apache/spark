package org.apache.spark.graph.cypher.conversions

import org.apache.spark.graph.cypher.conversions.TemporalConversions._
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.graph.cypher.udfs.LegacyUdfs._
import org.apache.spark.graph.cypher.udfs.TemporalUdfs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper._
import org.opencypher.okapi.impl.temporal.{Duration => DurationValue}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object ExprConversions {

  private val NULL_LIT: Column = functions.lit(null)

  private val TRUE_LIT: Column = functions.lit(true)

  private val FALSE_LIT: Column = functions.lit(false)

  private val ONE_LIT: Column = functions.lit(1)

  private val E: Column = functions.lit(Math.E)

  private val PI: Column = functions.lit(Math.PI)

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.expressionsFor(expr).isEmpty) throw IllegalStateException(s"Expression $expr not in header:\n${header.pretty}")
    }

    /**
      * This is possible without violating Cypher semantics because
      *   - Spark SQL returns null when comparing across types (from initial investigation)
      *   - We never have multiple types per column in CAPS (yet)
      */
    def compare(comparator: Column => Column => Column, lhs: Expr, rhs: Expr)
      (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
      comparator(lhs.asSparkSQLExpr)(rhs.asSparkSQLExpr)
    }

    def lt(c: Column): Column => Column = c < _

    def lteq(c: Column): Column => Column = c <= _

    def gt(c: Column): Column => Column = c > _

    def gteq(c: Column): Column => Column = c >= _

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header     the header of the CAPSRecords in which the expression should be evaluated.
      * @param df         the dataframe containing the data over which the expression should be evaluated.
      * @param parameters query parameters
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {

      expr match {

        // context based lookups
        case p@Param(name) if p.cypherType.isInstanceOf[CTList] =>
          parameters(name) match {
            case CypherList(l) => functions.array(l.unwrap.map(functions.lit): _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }

        case Param(name) =>
          toSparkLiteral(parameters(name).unwrap)

        case Property(e, PropertyKey(key)) =>
          e.cypherType.material match {
            case CTMap(inner) =>
              if (inner.keySet.contains(key)) e.asSparkSQLExpr.getField(key) else functions.lit(null)

            case CTDate =>
              TemporalConversions.temporalAccessor[Date](e.asSparkSQLExpr, key)

            case CTLocalDateTime =>
              TemporalConversions.temporalAccessor[Timestamp](e.asSparkSQLExpr, key)

            case CTDuration =>
              TemporalUdfs.durationAccessor(key.toLowerCase).apply(e.asSparkSQLExpr)

            case _ if !header.contains(expr) || !df.columns.contains(header.column(expr)) =>
              NULL_LIT

            case _ =>
              verify

              df.col(header.column(expr))
          }

        // direct column lookup
        case _: Var | _: Param | _: HasLabel | _: HasType | _: StartNode | _: EndNode =>
          verify

          val colName = header.column(expr)
          if (df.columns.contains(colName)) {
            df.col(colName)
          } else {
            NULL_LIT
          }

        case AliasExpr(innerExpr, _) =>
          innerExpr.asSparkSQLExpr

        // Literals
        case ListLit(exprs) =>
          functions.array(exprs.map(_.asSparkSQLExpr): _*)

        case NullLit(ct) =>
          NULL_LIT.cast(ct.toSparkType.get)

        case LocalDateTime(dateExpr) =>
          dateExpr match {
            case Some(e) =>
              val localDateTimeValue = TemporalConversions.resolveTemporalArgument(e)
                .map(parseLocalDateTime)
                .map(java.sql.Timestamp.valueOf)
                .map {
                  case ts if ts.getNanos % 1000 == 0 => ts
                  case _ => throw IllegalStateException("Spark does not support nanosecond resolution in 'localdatetime'")
                }
                .orNull

              functions.lit(localDateTimeValue).cast(DataTypes.TimestampType)
            case None => functions.current_timestamp()
          }

        case Date(dateExpr) =>
          dateExpr match {
            case Some(e) =>
              val dateValue = TemporalConversions.resolveTemporalArgument(e)
                .map(parseDate)
                .map(java.sql.Date.valueOf)
                .orNull

              functions.lit(dateValue).cast(DataTypes.DateType)
            case None => functions.current_timestamp()
          }

        case Duration(durationExpr) =>
          val durationValue = TemporalConversions.resolveTemporalArgument(durationExpr).map {
            case Left(m) => DurationValue(m.mapValues(_.toLong)).toCalendarInterval
            case Right(s) => DurationValue.parse(s).toCalendarInterval
          }.orNull
          functions.lit(durationValue)

        case l: Lit[_] => functions.lit(l.v)

        // predicates
        case Equals(e1, e2) => e1.asSparkSQLExpr === e2.asSparkSQLExpr
        case Not(e) => !e.asSparkSQLExpr
        case IsNull(e) => e.asSparkSQLExpr.isNull
        case IsNotNull(e) => e.asSparkSQLExpr.isNotNull
        case Size(e) =>
          val col = e.asSparkSQLExpr
          e.cypherType match {
            case CTString => functions.length(col).cast(LongType)
            case _: CTList | _: CTListOrNull =>
              functions.when(
                col.isNotNull,
                functions.size(col).cast(LongType)
              )
            case CTNull => NULL_LIT
            case other => throw NotImplementedException(s"size() on values of type $other")
          }

        case Ands(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(TRUE_LIT)(_ && _)

        case Ors(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(FALSE_LIT)(_ || _)

        case In(lhs, rhs) =>
          if (rhs.cypherType == CTNull || lhs.cypherType == CTNull) {
            NULL_LIT.cast(BooleanType)
          } else {
            val element = lhs.asSparkSQLExpr
            val array = rhs.asSparkSQLExpr
            array_contains(array, element)
          }

        case LessThan(lhs, rhs) => compare(lt, lhs, rhs)
        case LessThanOrEqual(lhs, rhs) => compare(lteq, lhs, rhs)
        case GreaterThanOrEqual(lhs, rhs) => compare(gteq, lhs, rhs)
        case GreaterThan(lhs, rhs) => compare(gt, lhs, rhs)

        case StartsWith(lhs, rhs) =>
          lhs.asSparkSQLExpr.startsWith(rhs.asSparkSQLExpr)
        case EndsWith(lhs, rhs) =>
          lhs.asSparkSQLExpr.endsWith(rhs.asSparkSQLExpr)
        case Contains(lhs, rhs) =>
          lhs.asSparkSQLExpr.contains(rhs.asSparkSQLExpr)

        case RegexMatch(prop, Param(name)) =>
          val regex: String = parameters(name).unwrap.toString
          prop.asSparkSQLExpr.rlike(regex)

        // Arithmetics
        case Add(lhs, rhs) =>
          val lhsCT = lhs.cypherType
          val rhsCT = rhs.cypherType
          lhsCT.material -> rhsCT.material match {
            case (_: CTList, _) =>
              throw UnsupportedOperationException("List concatenation is not supported")

            case (_, _: CTList) =>
              throw UnsupportedOperationException("List concatenation is not supported")

            case (CTString, _) if rhsCT.subTypeOf(CTNumber).maybeTrue =>
              functions.concat(lhs.asSparkSQLExpr, rhs.asSparkSQLExpr.cast(StringType))

            case (_, CTString) if lhsCT.subTypeOf(CTNumber).maybeTrue =>
              functions.concat(lhs.asSparkSQLExpr.cast(StringType), rhs.asSparkSQLExpr)

            case (CTString, CTString) =>
              functions.concat(lhs.asSparkSQLExpr, rhs.asSparkSQLExpr)

            case (CTDate, CTDuration) =>
              TemporalUdfs.dateAdd(lhs.asSparkSQLExpr, rhs.asSparkSQLExpr)

            case _ =>
              lhs.asSparkSQLExpr + rhs.asSparkSQLExpr
          }

        case Subtract(lhs, rhs) if lhs.cypherType.material.subTypeOf(CTDate).isTrue && rhs.cypherType.material.subTypeOf(CTDuration).isTrue =>
          TemporalUdfs.dateSubtract(lhs.asSparkSQLExpr, rhs.asSparkSQLExpr)

        case Subtract(lhs, rhs) =>
          lhs.asSparkSQLExpr - rhs.asSparkSQLExpr

        case Multiply(lhs, rhs) => lhs.asSparkSQLExpr * rhs.asSparkSQLExpr
        case div@Divide(lhs, rhs) => (lhs.asSparkSQLExpr / rhs.asSparkSQLExpr).cast(div.cypherType.getSparkType)

        // Id functions

        case Id(e) => e.asSparkSQLExpr

        case PrefixId(idExpr, prefix) =>
//          idExpr.asSparkSQLExpr.addPrefix(functions.lit(prefix))
          ???

        case ToId(e) =>
          e.cypherType.material match {
            // TODO: Remove this call; we shouldn't have nodes or rels as concrete types here
            case _: CTNode | _: CTRelationship =>
              e.asSparkSQLExpr
            case CTInteger =>
//              e.asSparkSQLExpr.encodeLongAsCAPSId
              ???
            case CTIdentity =>
              e.asSparkSQLExpr
            case other =>
              throw IllegalArgumentException("a type that may be converted to an ID", other)
          }

        // Functions
        case _: MonotonicallyIncreasingId => functions.monotonically_increasing_id()
        case Exists(e) => e.asSparkSQLExpr.isNotNull
        case Labels(e) =>
          e.cypherType match {
            case _: CTNode | _: CTNodeOrNull =>
              val node = e.owner.get
              val labelExprs = header.labelsFor(node)
              val (labelNames, labelColumns) = labelExprs
                .toSeq
                .map(e => e.label.name -> e.asSparkSQLExpr)
                .sortBy(_._1)
                .unzip
              val booleanLabelFlagColumn = functions.array(labelColumns: _*)
              get_node_labels(labelNames)(booleanLabelFlagColumn)
            case CTNull => NULL_LIT
            case other => throw IllegalArgumentException("an expression with type CTNode, CTNodeOrNull, or CTNull", other)
          }

        case Keys(e) => e.cypherType.material match {
          case _: CTNode | _: CTRelationship =>
            val node = e.owner.get
            val propertyExprs = header.propertiesFor(node).toSeq.sortBy(_.key.name)
            val (propertyKeys, propertyColumns) = propertyExprs.map(e => e.key.name -> e.asSparkSQLExpr).unzip
            val valuesColumn = functions.array(propertyColumns: _*)
            get_property_keys(propertyKeys)(valuesColumn)

          case CTMap(innerTypes) =>
            val mapColumn = e.asSparkSQLExpr
            val (keys, valueColumns) = innerTypes.keys.map { e =>
              // Whe have to make sure that every column has the same type (true or null)
              e -> functions.when(mapColumn.getField(e).isNotNull, functions.lit(true)).otherwise(NULL_LIT)
            }.toSeq.unzip
            val valueColumn = functions.array(valueColumns: _*)
            get_property_keys(keys)(valueColumn)

          case other => throw IllegalArgumentException("an Expression with type CTNode, CTRelationship or CTMap", other)
        }

        case Properties(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val element = e.owner.get
              val propertyExprs = header.propertiesFor(element).toSeq.sortBy(_.key.name)
              val propertyColumns = propertyExprs.map(e => e.asSparkSQLExpr.as(e.key.name))
              createStructColumn(propertyColumns)
            case _: CTMap => e.asSparkSQLExpr
            case other =>
              throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
          }

        case Type(inner) =>
          inner match {
            case v: Var =>
              val typeExprs = header.typesFor(v)
              val (relTypeNames, relTypeColumn) = typeExprs.toSeq.map(e => e.relType.name -> e.asSparkSQLExpr).unzip
              val booleanLabelFlagColumn = functions.array(relTypeColumn: _*)
              get_rel_type(relTypeNames)(booleanLabelFlagColumn)
            case _ =>
              throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
          }

        case StartNodeFunction(e) =>
          val rel = e.owner.get
          header.startNodeFor(rel).asSparkSQLExpr

        case EndNodeFunction(e) =>
          val rel = e.owner.get
          header.endNodeFor(rel).asSparkSQLExpr

        case ToFloat(e) => e.asSparkSQLExpr.cast(DoubleType)

        case ToInteger(e) => e.asSparkSQLExpr.cast(IntegerType)

        case ToString(e) => e.asSparkSQLExpr.cast(StringType)

        case ToBoolean(e) => e.asSparkSQLExpr.cast(BooleanType)

        case Explode(list) => list.cypherType match {
          case CTList(_) | CTListOrNull(_) => functions.explode(list.asSparkSQLExpr)
          case CTNull => functions.explode(functions.lit(null).cast(ArrayType(NullType)))
          case other => throw IllegalArgumentException("CTList", other)
        }

        case Trim(str) => functions.trim(str.asSparkSQLExpr)
        case LTrim(str) => functions.ltrim(str.asSparkSQLExpr)
        case RTrim(str) => functions.rtrim(str.asSparkSQLExpr)

        case ToUpper(str) => functions.upper(str.asSparkSQLExpr)
        case ToLower(str) => functions.lower(str.asSparkSQLExpr)

        case Range(from, to, maybeStep) =>
          val stepCol = maybeStep.map(_.asSparkSQLExpr).getOrElse(ONE_LIT)
          rangeUdf(from.asSparkSQLExpr, to.asSparkSQLExpr, stepCol)

        case Replace(original, search, replacement) => translateColumn(original.asSparkSQLExpr, search.asSparkSQLExpr, replacement.asSparkSQLExpr)

        case Substring(original, start, maybeLength) =>
          val origCol = original.asSparkSQLExpr
          val startCol = start.asSparkSQLExpr + ONE_LIT
          val lengthCol = maybeLength.map(_.asSparkSQLExpr).getOrElse(functions.length(origCol) - startCol + ONE_LIT)
          origCol.substr(startCol, lengthCol)

        // Mathematical functions

        case _: E => E
        case _: Pi => PI

        case Sqrt(e) => functions.sqrt(e.asSparkSQLExpr)
        case Log(e) => functions.log(e.asSparkSQLExpr)
        case Log10(e) => functions.log(10.0, e.asSparkSQLExpr)
        case Exp(e) => functions.exp(e.asSparkSQLExpr)
        case Abs(e) => functions.abs(e.asSparkSQLExpr)
        case Ceil(e) => functions.ceil(e.asSparkSQLExpr).cast(DoubleType)
        case Floor(e) => functions.floor(e.asSparkSQLExpr).cast(DoubleType)
        case _: Rand => functions.rand()
        case Round(e) => functions.round(e.asSparkSQLExpr).cast(DoubleType)
        case Sign(e) => functions.signum(e.asSparkSQLExpr).cast(IntegerType)

        case Acos(e) => functions.acos(e.asSparkSQLExpr)
        case Asin(e) => functions.asin(e.asSparkSQLExpr)
        case Atan(e) => functions.atan(e.asSparkSQLExpr)
        case Atan2(e1, e2) => functions.atan2(e1.asSparkSQLExpr, e2.asSparkSQLExpr)
        case Cos(e) => functions.cos(e.asSparkSQLExpr)
        case Cot(e) => Divide(IntegerLit(1)(CTInteger), Tan(e)(CTFloat))(CTFloat).asSparkSQLExpr
        case Degrees(e) => functions.degrees(e.asSparkSQLExpr)
        case Haversin(e) => Divide(Subtract(IntegerLit(1)(CTInteger), Cos(e)(CTFloat))(CTFloat), IntegerLit(2)(CTInteger))(CTFloat).asSparkSQLExpr
        case Radians(e) => functions.radians(e.asSparkSQLExpr)
        case Sin(e) => functions.sin(e.asSparkSQLExpr)
        case Tan(e) => functions.tan(e.asSparkSQLExpr)


        // Time functions

        case Timestamp() => functions.current_timestamp().cast(LongType)

        // Bit operations

        case BitwiseAnd(lhs, rhs) =>
          lhs.asSparkSQLExpr.bitwiseAND(rhs.asSparkSQLExpr)

        case BitwiseOr(lhs, rhs) =>
          lhs.asSparkSQLExpr.bitwiseOR(rhs.asSparkSQLExpr)

        case ShiftLeft(value, IntegerLit(shiftBits)) =>
          functions.shiftLeft(value.asSparkSQLExpr, shiftBits.toInt)

        case ShiftRightUnsigned(value, IntegerLit(shiftBits)) =>
          functions.shiftRightUnsigned(value.asSparkSQLExpr, shiftBits.toInt)

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          functions.coalesce(columns: _*)

        case c: CaseExpr =>
          val alternatives = c.alternatives.map {
            case (predicate, action) => functions.when(predicate.asSparkSQLExpr, action.asSparkSQLExpr)
          }

          val alternativesWithDefault = c.default match {
            case Some(inner) => alternatives :+ inner.asSparkSQLExpr
            case None => alternatives
          }

          val reversedColumns = alternativesWithDefault.reverse

          val caseColumn = reversedColumns.tail.foldLeft(reversedColumns.head) {
            case (tmpCol, whenCol) => whenCol.otherwise(tmpCol)
          }
          caseColumn

        case ContainerIndex(container, index) =>
          val indexCol = index.asSparkSQLExpr
          val containerCol = container.asSparkSQLExpr

          container.cypherType.material match {
            case _: CTList | _: CTMap => containerCol.get(indexCol)
            case other => throw NotImplementedException(s"Accessing $other by index is not supported")
          }

        case MapExpression(items) => expr.cypherType.material match {
          case CTMap(inner) =>
            val innerColumns = items.map {
              case (key, innerExpr) =>
                val targetType = inner(key).toSparkType.get
                innerExpr.asSparkSQLExpr.cast(targetType).as(key)
            }.toSeq
            createStructColumn(innerColumns)
          case other => throw IllegalArgumentException("an expression of type CTMap", other)
        }


        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }
  }

  private def toSparkLiteral(value: Any): Column = value match {
    case map: Map[_, _] =>
      val columns = map.map {
        case (key, v) => toSparkLiteral(v).as(key.toString)
      }.toSeq
      createStructColumn(columns)
    case _ => functions.lit(value)
  }

  private def createStructColumn(structColumns: Seq[Column]): Column = {
    if (structColumns.isEmpty) {
      functions.lit(null).cast(new StructType())
    } else {
      functions.struct(structColumns: _*)
    }
  }

}

