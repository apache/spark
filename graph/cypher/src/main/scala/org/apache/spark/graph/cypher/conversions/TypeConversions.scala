package org.apache.spark.graph.cypher.conversions

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

object TypeConversions {

  // Spark data types that are supported within the Cypher type system
  val supportedTypes: Seq[DataType] = Seq(
    // numeric
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    // other
    StringType,
    BooleanType,
    DateType,
    TimestampType,
    NullType
  )

  implicit class CypherTypeOps(val ct: CypherType) extends AnyVal {

    def toStructField(column: String): StructField = {
      ct.toSparkType match {
        case Some(st) => StructField(column, st, ct.isNullable)
        case None => throw IllegalArgumentException("CypherType supported by CAPS", ct)
      }
    }

    def toSparkType: Option[DataType] = ct match {
      case CTNull | CTVoid => Some(NullType)
      case _ =>
        ct.material match {
          case CTString => Some(StringType)
          case CTInteger => Some(LongType)
          case CTBoolean => Some(BooleanType)
          case CTFloat => Some(DoubleType)
          case CTLocalDateTime => Some(TimestampType)
          case CTDate => Some(DateType)
          case CTDuration => Some(CalendarIntervalType)
          case CTIdentity => Some(BinaryType)
          case _: CTNode => Some(BinaryType)
          case _: CTRelationship => Some(BinaryType)
          case CTList(CTVoid) => Some(ArrayType(NullType, containsNull = true))
          case CTList(CTNull) => Some(ArrayType(NullType, containsNull = true))
          case CTList(elemType) =>
            elemType.toSparkType.map(ArrayType(_, elemType.isNullable))
          case CTMap(inner) =>
            val innerFields = inner.map {
              case (key, valueType) => valueType.toStructField(key)
            }.toSeq
            Some(StructType(innerFields))
          case _ =>
            None
        }
    }

    def getSparkType: DataType = toSparkType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"Mapping of CypherType $ct to Spark type")
    }

    def isSparkCompatible: Boolean = toSparkType.isDefined

  }

  implicit class StructTypeOps(val structType: StructType) {
    def toRecordHeader: RecordHeader = {

      val exprToColumn = structType.fields.map { field =>
        val cypherType = field.toCypherType match {
          case Some(ct) => ct
          case None => throw IllegalArgumentException("a supported Spark type", field.dataType)
        }
        Var(field.name)(cypherType) -> field.name
      }

      RecordHeader(exprToColumn.toMap)
    }

    def binaryColumns: Set[String] = structType.fields.filter(_.dataType == BinaryType).map(_.name).toSet

    def convertTypes(from: DataType, to: DataType): StructType = StructType(structType.map {
      case sf: StructField if sf.dataType == from => sf.copy(dataType = to)
      case sf: StructField => sf
    })
  }

  implicit class StructFieldOps(val field: StructField) extends AnyVal {
    def toCypherType: Option[CypherType] = field.dataType.toCypherType(field.nullable)
  }

  implicit class DataTypeOps(val dt: DataType) extends AnyVal {
    def toCypherType(nullable: Boolean = false): Option[CypherType] = {
      val result = dt match {
        case StringType => Some(CTString)
        case IntegerType => Some(CTInteger)
        case LongType => Some(CTInteger)
        case BooleanType => Some(CTBoolean)
        case DoubleType => Some(CTFloat)
        case TimestampType => Some(CTLocalDateTime)
        case DateType => Some(CTDate)
        case CalendarIntervalType => Some(CTDuration)
        case ArrayType(NullType, _) => Some(CTList(CTVoid))
        case BinaryType => Some(CTIdentity)
        case ArrayType(elemType, containsNull) =>
          elemType.toCypherType(containsNull).map(CTList)
        case NullType => Some(CTNull)
        case StructType(fields) =>
          val convertedFields = fields.map { field => field.name -> field.dataType.toCypherType(field.nullable) }.toMap
          val containsNone = convertedFields.exists {
            case (_, None) => true
            case _ => false
          }
          if (containsNone) None else Some(CTMap(convertedFields.mapValues(_.get)))
        case _ => None
      }

      if (nullable) result.map(_.nullable) else result.map(_.material)
    }

    /**
      * Checks if the given data type is supported within the Cypher type system.
      *
      * @return true, iff the data type is supported
      */
    def isCypherCompatible: Boolean = dt match {
      case ArrayType(internalType, _) => internalType.isCypherCompatible
      case StructType(fields) => fields.forall(_.dataType.isCypherCompatible)
      case other => supportedTypes.contains(other)
    }

    /**
      * Converts the given Spark data type into a Cypher type system compatible Spark data type.
      *
      * @return some Cypher-compatible Spark data type or none if not compatible
      */
    def cypherCompatibleDataType: Option[DataType] = dt match {
      case ByteType | ShortType | IntegerType => Some(LongType)
      case FloatType => Some(DoubleType)
      case compatible if dt.toCypherType().isDefined => Some(compatible)
      case _ => None
    }
  }

  implicit class RecordHeaderOps(header: RecordHeader) extends Serializable {

    def toStructType: StructType = {
      val structFields = header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ join _)
        assert(commonType.isSparkCompatible,
          s"""
             |Expressions $expressions with common super type $commonType mapped to column $column have no compatible data type.
         """.stripMargin)
        commonType.toStructField(column)
      }
      StructType(structFields)
    }

    def rowEncoder: ExpressionEncoder[Row] =
      RowEncoder(header.toStructType)
  }

  implicit class RowOps(row: Row) {

    def allNull: Boolean = allNull(row.size)

    def allNull(rowSize: Int): Boolean = (for (i <- 0 until rowSize) yield row.isNullAt(i)).reduce(_ && _)
  }

}

