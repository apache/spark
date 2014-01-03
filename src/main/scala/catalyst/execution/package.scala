package catalyst

import types._

/**
 * A partial reimplementation of Shark, a Hive compatible SQL engine running on Spark, using Catalyst.
 *
 * This implementation uses the hive parser, metadata catalog and serdes, but performs all optimization and execution
 * using catalyst and spark.
 *
 * Currently functions that are not supported by this implementation are passed back to the original Shark
 * implementation for execution.
 */
package object execution {
  type Row = catalyst.expressions.Row

  implicit class typeInfoConversions(dt: DataType) {
    import org.apache.hadoop.hive.serde2.typeinfo._
    import TypeInfoFactory._

    def toTypeInfo: TypeInfo = dt match {
      case BooleanType => booleanTypeInfo
      case ByteType => byteTypeInfo
      case DoubleType => doubleTypeInfo
      case FloatType => floatTypeInfo
      case IntegerType => intTypeInfo
      case LongType => longTypeInfo
      case ShortType => shortTypeInfo
      case StringType => stringTypeInfo
    }
  }
}