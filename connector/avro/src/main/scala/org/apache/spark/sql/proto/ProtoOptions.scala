package org.apache.spark.sql.proto

import java.io.FileInputStream
import java.net.URI

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors.Descriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}
import org.apache.spark.sql.internal.SQLConf

/**
  * Options for Proto Reader and Writer stored in case insensitive manner.
  */
private[sql] class ProtoOptions(
                                @transient val parameters: CaseInsensitiveMap[String],
                                @transient val conf: Configuration)
  extends FileSourceOptions(parameters) with Logging {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  /**
   * The `compression` option allows to specify a compression codec used in write.
   * Currently supported codecs are `uncompressed`, `snappy`, `deflate`, `bzip2`, `xz` and
   * `zstandard`. If the option is not set, the `spark.sql.avro.compression.codec` config is
   * taken into account. If the former one is not set too, the `snappy` codec is used by default.
   */
  val compression: String = {
    parameters.get("compression").getOrElse(SQLConf.get.avroCompressionCodec)
  }

  /**
    * Optional schema provided by a user in schema file or in JSON format.
    *
    * When reading Proto, this option can be set to an evolved schema, which is compatible but
    * different with the actual Proto schema. The deserialization schema will be consistent with
    * the evolved schema. For example, if we set an evolved schema containing one additional
    * column with a default value, the reading result in Spark will contain the new column too.
    *
    * When writing Proto, this option can be set if the expected output Proto schema doesn't match the
    * schema converted by Spark. For example, the expected schema of one column is of "enum" type,
    * instead of "string" type in the default converted schema.
    */
  val schema: Option[Descriptor] = {
    parameters.get("protoSchema").map(a => DescriptorProtos.DescriptorProto.parseFrom(new FileInputStream(a)).getDescriptorForType).orElse({
      val protoUrlSchema = parameters.get("protoSchemaUrl").map(url => {
        log.debug("loading proto schema from url: " + url)
        val fs = FileSystem.get(new URI(url), conf)
        val in = fs.open(new Path(url))
        try {
          DescriptorProtos.DescriptorProto.parseFrom(in).getDescriptorForType
        } finally {
          in.close()
        }
      })
      protoUrlSchema
    })
  }

  /**
    * Iff true, perform Catalyst-to-Proto schema matching based on field position instead of field
    * name. This allows for a structurally equivalent Catalyst schema to be used with an Proto schema
    * whose field names do not match. Defaults to false.
    */
  val positionalFieldMatching: Boolean =
    parameters.get("positionalFieldMatching").exists(_.toBoolean)

  /**
    * Top level record name in write result, which is required in Proto spec.
    * See https://avro.apache.org/docs/1.11.0/spec.html#schema_record .
    * Default value is "topLevelRecord"
    */
  val recordName: String = parameters.getOrElse("recordName", "topLevelRecord")

  /**
    * Record namespace in write result. Default value is "".
    * See Proto spec for details: https://avro.apache.org/docs/1.11.0/spec.html#schema_record .
    */
  val recordNamespace: String = parameters.getOrElse("recordNamespace", "")

  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)

  /**
    * The rebasing mode for the DATE and TIMESTAMP_MICROS, TIMESTAMP_MILLIS values in reads.
    */
  val datetimeRebaseModeInRead: String = parameters
    .get(ProtoOptions.DATETIME_REBASE_MODE)
    .getOrElse(SQLConf.get.getConf(SQLConf.AVRO_REBASE_MODE_IN_READ))
}

private[sql] object ProtoOptions {
  def apply(parameters: Map[String, String]): ProtoOptions = {
    val hadoopConf = SparkSession
      .getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtoOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }

  val ignoreExtensionKey = "ignoreExtension"

  // The option controls rebasing of the DATE and TIMESTAMP values between
  // Julian and Proleptic Gregorian calendars. It impacts on the behaviour of the Proto
  // datasource similarly to the SQL config `spark.sql.proto.datetimeRebaseModeInRead`,
  // and can be set to the same values: `EXCEPTION`, `LEGACY` or `CORRECTED`.
  val DATETIME_REBASE_MODE = "datetimeRebaseMode"
}

