package catalyst
package types

sealed class DataType

case object IntegerType extends DataType
case object StringType extends DataType
case object BooleanType extends DataType