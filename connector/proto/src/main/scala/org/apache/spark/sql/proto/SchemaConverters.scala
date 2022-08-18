package org.apache.spark.sql.proto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

/**
  * This object contains method that are used to convert sparkSQL schemas to proto schemas and vice
  * versa.
  */
@DeveloperApi
object SchemaConverters {
  private lazy val nullSchema = null

  /**
    * Internal wrapper for SQL data type and nullability.
    *
    * @since 2.4.0
    */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * Converts an Proto schema to a corresponding Spark SQL schema.
    *
    * @since 2.4.0
    */
  def toSqlType(protoSchema: Descriptor): SchemaType = {
    toSqlTypeHelper(protoSchema)
  }

  def toSqlTypeHelper(descriptor: Descriptor): SchemaType = ScalaReflectionLock.synchronized {
    import scala.collection.JavaConverters._
    SchemaType(StructType(descriptor.getFields.asScala.flatMap(structFieldFor).toSeq), nullable = true)
  }

  def structFieldFor(fd: FieldDescriptor): Option[StructField] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType = fd.getJavaType match {
      case INT => Some(IntegerType)
      case LONG => Some(LongType)
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => Some(StringType)
      case MESSAGE =>
        import collection.JavaConverters._
        Option(fd.getMessageType.getFields.asScala.flatMap(structFieldFor))
          .filter(_.nonEmpty)
          .map(StructType.apply)

    }
    dataType.map( dt => StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
      nullable = !fd.isRequired && !fd.isRepeated
    ))
  }

  private[proto] class IncompatibleSchemaException(
                                                   msg: String, ex: Throwable = null) extends Exception(msg, ex)

  private[proto] class UnsupportedAvroTypeException(msg: String) extends Exception(msg)
}
