package catalyst
package types

class DataType

case object StringType extends DataType
case object BinaryType extends DataType

case object IntegerType extends DataType
case object BooleanType extends DataType
case object FloatType extends DataType
case object DoubleType extends DataType
case object LongType extends DataType
case object ByteType extends DataType
case object ShortType extends DataType

case class ArrayType(elementType: DataType) extends DataType

case class StructField(name: String, dataType: DataType)
case class StructType(fields: Seq[StructField]) extends DataType

case class MapType(keyType: DataType, valueType: DataType) extends DataType