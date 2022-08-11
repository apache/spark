package org.apache.spark.sql.v2.proto

import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor

import java.io._
import java.net.URI
import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.proto.{ProtoDeserializer, ProtoOptions, ProtoUtils}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[sql] class ProtoFileFormat extends FileFormat
  with DataSourceRegister with Logging with Serializable {

  override def equals(other: Any): Boolean = other match {
    case _: ProtoFileFormat => true
    case _ => false
  }

  // Dummy hashCode() to appease ScalaStyle.
  override def hashCode(): Int = super.hashCode()

  override def inferSchema(
                            spark: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    ProtoUtils.inferSchema(spark, options, files)
  }

  override def shortName(): String = "proto"

  override def toString(): String = "Proto"

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = true

  override def prepareWrite(
                             spark: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = ???

  override def buildReader(
                            spark: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val parsedOptions = new ProtoOptions(options, hadoopConf)
    val datetimeRebaseModeInRead = parsedOptions.datetimeRebaseModeInRead

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      val userProvidedSchema = parsedOptions.schema

      // TODO Removes this check once `FileFormat` gets a general file filtering interface method.
      // Doing input file filtering is improper because we may generate empty tasks that process no
      // input files but stress the scheduler. We should probably add a more general input file
      // filtering mechanism for `FileFormat` data sources. See SPARK-16317.
      if (file.filePath.endsWith(".pb")) {
        var readerSchema: Descriptor = null
        val reader = {
          // val in = Files.readAllBytes(Paths.get(partitionedFile.filePath))
          val in = new FileInputStream(new Path(new URI(file.filePath)).toUri.toString)
          try {
            val fileDescriptorSet  = FileDescriptorSet.parseFrom(in)
            readerSchema = fileDescriptorSet.getDescriptorForType
            fileDescriptorSet.getFileList.iterator()
          } catch {
            case NonFatal(e) =>
              logError("Exception while opening DataFileReader", e)
              in.close()
              throw e
          }
        }

        val protoFilters = if (SQLConf.get.avroFilterPushDown) {
          new OrderedFilters(filters, requiredSchema)
        } else {
          new NoopFilters
        }

        new Iterator[InternalRow] with ProtoUtils.RowReader {
          override val fileDescriptor = reader
          override val deserializer = new ProtoDeserializer(
            userProvidedSchema.getOrElse(readerSchema),
            requiredSchema,
            parsedOptions.positionalFieldMatching,
            RebaseSpec(LegacyBehaviorPolicy.withName("Proto")),
            protoFilters)
          override def hasNext: Boolean = hasNextRow
          override def next(): InternalRow = nextRow
        }
      } else {
        Iterator.empty
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean = ProtoUtils.supportsDataType(dataType)

  override def supportFieldName(name: String): Boolean = {
    if (name.length == 0) {
      false
    } else {
      name.zipWithIndex.forall {
        case (c, 0) if !Character.isLetter(c) && c != '_' => false
        case (c, _) if !Character.isLetterOrDigit(c) && c != '_' => false
        case _ => true
      }
    }
  }
}

private[proto] object ProtoFileFormat {
  val IgnoreFilesWithoutExtensionProperty = "proto.mapred.ignore.inputs.without.extension"
}