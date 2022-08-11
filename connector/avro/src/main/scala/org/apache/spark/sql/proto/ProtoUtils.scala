package org.apache.spark.sql.proto

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.FileDescriptorProto

import java.util.Locale
import scala.collection.JavaConverters._
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.proto.ProtoOptions.ignoreExtensionKey
import org.apache.spark.sql.proto.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, NullType, StructField, StructType, UserDefinedType}
import org.apache.spark.util.Utils
import org.apache.spark.sql.catalyst.InternalRow

import java.io.{FileInputStream, FileNotFoundException, IOException}

private[sql] object ProtoUtils extends Logging {

  def inferSchema(
                   spark: SparkSession,
                   options: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new ProtoOptions(options, conf)

    if (parsedOptions.parameters.contains(ignoreExtensionKey)) {
      logWarning(s"Option $ignoreExtensionKey is deprecated. Please use the " +
        "general data source option pathGlobFilter for filtering file names.")
    }
    // User can specify an optional proto json schema.
    val protoSchema = parsedOptions.schema
      .getOrElse {
        inferProtoSchemaFromFiles(files, conf, new FileSourceOptions(CaseInsensitiveMap(options)).ignoreCorruptFiles)
      }

    SchemaConverters.toSqlType(protoSchema).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""Proto schema cannot be converted to a Spark SQL StructType:
           |
           |${protoSchema.toString()}
           |""".stripMargin)
    }
  }

  private def inferProtoSchemaFromFiles(
                                        files: Seq[FileStatus],
                                        conf: Configuration,
                                        ignoreCorruptFiles: Boolean): Descriptor = {
    // Schema evolution is not supported yet. Here we only pick first random readable sample file to
    // figure out the schema of the whole dataset.
    val protoReader = files.iterator.map { f =>
      val path = f.getPath
      if (!path.getName.endsWith(".proto")) {
        None
      } else {
        Utils.tryWithResource {
          new FileInputStream(path.toUri.toString)
        } { in =>
          try {
            Some(DescriptorProtos.DescriptorProto.parseFrom(in).getDescriptorForType)
          } catch {
            case e: IOException =>
              if (ignoreCorruptFiles) {
                logWarning(s"Skipped the footer in the corrupted file: $path", e)
                None
              } else {
                throw new SparkException(s"Could not read file: $path", e)
              }
          }
        }
      }
    }.collectFirst {
      case Some(reader) => reader
    }

    protoReader match {
      case Some(reader) =>
          reader.getContainingType
      case None =>
        throw new FileNotFoundException(
          "No Proto files found. If files don't have .proto extension, set ignoreExtension to true")
    }
  }

  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  // The trait provides iterator-like interface for reading records from an Proto file,
  // deserializing and returning them as internal rows.
  trait RowReader {
    protected val fileDescriptor: java.util.Iterator[FileDescriptorProto]
    protected val deserializer: ProtoDeserializer
    private[this] var completed = false
    private[this] var currentRow: Option[InternalRow] = None

    def hasNextRow: Boolean = {
      while (!completed && currentRow.isEmpty) {
        val r = fileDescriptor.hasNext
        if (!r) {
          completed = true
          currentRow = None
        } else {
          val record = fileDescriptor.next()
          // the row must be deserialized in hasNextRow, because AvroDeserializer#deserialize
          // potentially filters rows
          currentRow = deserializer.deserialize(record).asInstanceOf[Option[InternalRow]]
        }
      }
      currentRow.isDefined
    }

    def nextRow: InternalRow = {
      if (currentRow.isEmpty) {
        hasNextRow
      }
      val returnRow = currentRow
      currentRow = None // free up hasNextRow to consume more Avro records, if not exhausted
      returnRow.getOrElse {
        throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Proto field. */
  private[sql] case class ProtoMatchedField(
                                            catalystField: StructField,
                                            catalystPosition: Int,
                                            protoField: FieldDescriptor)

  /**
    * Helper class to perform field lookup/matching on Proto schemas.
    *
    * This will match `protoSchema` against `catalystSchema`, attempting to find a matching field in
    * the Proto schema for each field in the Catalyst schema and vice-versa, respecting settings for
    * case sensitivity. The match results can be accessed using the getter methods.
    *
    * @param protoSchema The schema in which to search for fields. Must be of type RECORD.
    * @param catalystSchema The Catalyst schema to use for matching.
    * @param protoPath The seq of parent field names leading to `protoSchema`.
    * @param catalystPath The seq of parent field names leading to `catalystSchema`.
    * @param positionalFieldMatch If true, perform field matching in a positional fashion
    *                             (structural comparison between schemas, ignoring names);
    *                             otherwise, perform field matching using field names.
    */
  class ProtoSchemaHelper(
                          protoSchema: Descriptor,
                          catalystSchema: StructType,
                          protoPath: Seq[String],
                          catalystPath: Seq[String],
                          positionalFieldMatch: Boolean) {
    println(protoSchema.getName)
    if (protoSchema.getName == null) {
      throw new IncompatibleSchemaException(
        s"Attempting to treat ${protoSchema.getName} as a RECORD, but it was: ${protoSchema.getContainingType}")
    }

    private[this] val protoFieldArray = protoSchema.getFields.asScala.toArray
    private[this] val fieldMap = protoSchema.getFields.asScala
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq) // toSeq needed for scala 2.13

    /** The fields which have matching equivalents in both Proto and Catalyst schemas. */
    val matchedFields: Seq[ProtoMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) =>
        getProtoField(sqlField.name, sqlPos).map(ProtoMatchedField(sqlField, sqlPos, _))
    }

    /**
      * Validate that there are no Catalyst fields which don't have a matching Proto field, throwing
      * [[IncompatibleSchemaException]] if such extra fields are found. If `ignoreNullable` is false,
      * consider nullable Catalyst fields to be eligible to be an extra field; otherwise,
      * ignore nullable Catalyst fields when checking for extras.
      */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.zipWithIndex.foreach { case (sqlField, sqlPos) =>
        if (getProtoField(sqlField.name, sqlPos).isEmpty &&
          (!ignoreNullable || !sqlField.nullable)) {
          if (positionalFieldMatch) {
            throw new IncompatibleSchemaException("Cannot find field at position " +
              s"$sqlPos of ${toFieldStr(protoPath)} from Proto schema (using positional matching)")
          } else {
            throw new IncompatibleSchemaException(
              s"Cannot find ${toFieldStr(catalystPath :+ sqlField.name)} in Proto schema")
          }
        }
      }

    /**
      * Validate that there are no Proto fields which don't have a matching Catalyst field, throwing
      * [[IncompatibleSchemaException]] if such extra fields are found. Only required (non-nullable)
      * fields are checked; nullable fields are ignored.
      */
    def validateNoExtraRequiredProtoFields(): Unit = {
      val extraFields = protoFieldArray.toSet -- matchedFields.map(_.protoField)
      extraFields.filterNot(isNullable).foreach { extraField =>
        if (positionalFieldMatch) {
          throw new IncompatibleSchemaException(s"Found field '${extraField.getName()}' at position " +
            s"${extraField.getIndex} of ${toFieldStr(protoPath)} from Proto schema but there is no " +
            s"match in the SQL schema at ${toFieldStr(catalystPath)} (using positional matching)")
        } else {
          throw new IncompatibleSchemaException(
            s"Found ${toFieldStr(protoPath :+ extraField.getName())} in Proto schema but there is no " +
              "match in the SQL schema")
        }
      }
    }

    /**
      * Extract a single field from the contained proto schema which has the desired field name,
      * performing the matching with proper case sensitivity according to SQLConf.resolver.
      *
      * @param name The name of the field to search for.
      * @return `Some(match)` if a matching Proto field is found, otherwise `None`.
      */
    private[proto] def getFieldByName(name: String): Option[FieldDescriptor] = {

      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.getName(), name)) match {
        case Seq(protoField) => Some(protoField)
        case Seq() => None
        case matches => throw new IncompatibleSchemaException(s"Searching for '$name' in Proto " +
          s"schema at ${toFieldStr(protoPath)} gave ${matches.size} matches. Candidates: " +
          matches.map(_.getName()).mkString("[", ", ", "]")
        )
      }
    }

    /** Get the Proto field corresponding to the provided Catalyst field name/position, if any. */
    def getProtoField(fieldName: String, catalystPos: Int): Option[FieldDescriptor] = {
      if (positionalFieldMatch) {
        protoFieldArray.lift(catalystPos)
      } else {
        getFieldByName(fieldName)
      }
    }


  }

  /**
    * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
    * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
    * "top-level record" is returned.
    */
  private[proto] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  /** Return true iff `protoField` is nullable, i.e. `UNION` type and has `NULL` as an option. */
  private[proto] def isNullable(protoField: FieldDescriptor): Boolean =
    protoField.getMessageType == null

}
