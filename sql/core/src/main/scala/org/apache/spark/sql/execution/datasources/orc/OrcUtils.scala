/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.orc

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.io.{BooleanWritable, ByteWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, ShortWritable, WritableComparable}
import org.apache.orc.{BooleanColumnStatistics, ColumnStatistics, DateColumnStatistics, DoubleColumnStatistics, IntegerColumnStatistics, OrcConf, OrcFile, Reader, TypeDescription, Writer}

import org.apache.spark.{SPARK_VERSION_SHORT, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.PATH
import org.apache.spark.sql.{SPARK_VERSION_METADATA_KEY, SparkSession}
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, SchemaMergeUtils}
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

object OrcUtils extends Logging {

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    OrcCompressionCodec.NONE.name() -> "",
    OrcCompressionCodec.SNAPPY.name() -> ".snappy",
    OrcCompressionCodec.ZLIB.name() -> ".zlib",
    OrcCompressionCodec.ZSTD.name() -> ".zstd",
    OrcCompressionCodec.LZ4.name() -> ".lz4",
    OrcCompressionCodec.LZO.name() -> ".lzo",
    OrcCompressionCodec.BROTLI.name() -> ".brotli")

  val CATALYST_TYPE_ATTRIBUTE_NAME = "spark.sql.catalyst.type"

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDirectory)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))
    paths
  }

  def readSchema(file: Path, conf: Configuration, ignoreCorruptFiles: Boolean)
      : Option[TypeDescription] = {
    val fs = file.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    try {
      val schema = Utils.tryWithResource(OrcFile.createReader(file, readerOptions)) { reader =>
        reader.getSchema
      }
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        Some(schema)
      }
    } catch {
      case e: org.apache.orc.FileFormatException =>
        if (ignoreCorruptFiles) {
          logWarning(log"Skipped the footer in the corrupted file: ${MDC(PATH, file)}", e)
          None
        } else {
          throw QueryExecutionErrors.cannotReadFooterForFileError(file, e)
        }
    }
  }

  def toCatalystSchema(schema: TypeDescription): StructType = {
    import TypeDescription.Category

    def toCatalystType(orcType: TypeDescription): DataType = {
      orcType.getCategory match {
        case Category.STRUCT => toStructType(orcType)
        case Category.LIST => toArrayType(orcType)
        case Category.MAP => toMapType(orcType)
        case _ =>
          val catalystTypeAttrValue = orcType.getAttributeValue(CATALYST_TYPE_ATTRIBUTE_NAME)
          if (catalystTypeAttrValue != null) {
            CatalystSqlParser.parseDataType(catalystTypeAttrValue)
          } else {
            CatalystSqlParser.parseDataType(orcType.toString)
          }
      }
    }

    def toStructType(orcType: TypeDescription): StructType = {
      val fieldNames = orcType.getFieldNames.asScala
      val fieldTypes = orcType.getChildren.asScala
      val fields = new ArrayBuffer[StructField]()
      fieldNames.zip(fieldTypes).foreach {
        case (fieldName, fieldType) =>
          val catalystType = toCatalystType(fieldType)
          fields += StructField(fieldName, catalystType)
      }
      StructType(fields.toArray)
    }

    def toArrayType(orcType: TypeDescription): ArrayType = {
      val elementType = orcType.getChildren.get(0)
      ArrayType(toCatalystType(elementType))
    }

    def toMapType(orcType: TypeDescription): MapType = {
      val Seq(keyType, valueType) = orcType.getChildren.asScala.toSeq
      val catalystKeyType = toCatalystType(keyType)
      val catalystValueType = toCatalystType(valueType)
      MapType(catalystKeyType, catalystValueType)
    }

    // The Spark query engine has not completely supported CHAR/VARCHAR type yet, and here we
    // replace the orc CHAR/VARCHAR with STRING type.
    CharVarcharUtils.replaceCharVarcharWithStringInSchema(toStructType(schema))
  }

  def readSchema(sparkSession: SparkSession, files: Seq[FileStatus], options: Map[String, String])
      : Option[StructType] = {
    val ignoreCorruptFiles = new FileSourceOptions(CaseInsensitiveMap(options)).ignoreCorruptFiles
    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    files.iterator.map(file => readSchema(file.getPath, conf, ignoreCorruptFiles)).collectFirst {
      case Some(schema) =>
        logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
        toCatalystSchema(schema)
    }
  }

  /**
   * Reads ORC file schemas in multi-threaded manner, using native version of ORC.
   * This is visible for testing.
   */
  def readOrcSchemasInParallel(
    files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean): Seq[StructType] = {
    ThreadUtils.parmap(files, "readingOrcSchemas", 8) { currentFile =>
      OrcUtils.readSchema(currentFile.getPath, conf, ignoreCorruptFiles).map(toCatalystSchema)
    }.flatten
  }

  def inferSchema(sparkSession: SparkSession, files: Seq[FileStatus], options: Map[String, String])
    : Option[StructType] = {
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)
    if (orcOptions.mergeSchema) {
      SchemaMergeUtils.mergeSchemasInParallel(
        sparkSession, options, files, OrcUtils.readOrcSchemasInParallel)
    } else {
      OrcUtils.readSchema(sparkSession, files, options)
    }
  }

  /**
   * @return Returns the combination of requested column ids from the given ORC file and
   *         boolean flag to find if the pruneCols is allowed or not. Requested Column id can be
   *         -1, which means the requested column doesn't exist in the ORC file. Returns None
   *         if the given ORC file is empty.
   */
  def requestedColumnIds(
      isCaseSensitive: Boolean,
      dataSchema: StructType,
      requiredSchema: StructType,
      orcSchema: TypeDescription,
      conf: Configuration): Option[(Array[Int], Boolean)] = {
    def checkTimestampCompatibility(orcCatalystSchema: StructType, dataSchema: StructType): Unit = {
      orcCatalystSchema.fields.map(_.dataType).zip(dataSchema.fields.map(_.dataType)).foreach {
        case (TimestampType, TimestampNTZType) =>
          throw QueryExecutionErrors.cannotConvertOrcTimestampToTimestampNTZError()
        case (TimestampNTZType, TimestampType) =>
          throw QueryExecutionErrors.cannotConvertOrcTimestampNTZToTimestampLTZError()
        case (t1: StructType, t2: StructType) => checkTimestampCompatibility(t1, t2)
        case _ =>
      }
    }

    checkTimestampCompatibility(toCatalystSchema(orcSchema), dataSchema)
    val orcFieldNames = orcSchema.getFieldNames.asScala
    val forcePositionalEvolution = OrcConf.FORCE_POSITIONAL_EVOLUTION.getBoolean(conf)
    if (orcFieldNames.isEmpty) {
      // SPARK-8501: Some old empty ORC files always have an empty schema stored in their footer.
      None
    } else {
      if (forcePositionalEvolution || orcFieldNames.forall(_.startsWith("_col"))) {
        // This is either an ORC file written by an old version of Hive and there are no field
        // names in the physical schema, or `orc.force.positional.evolution=true` is forced because
        // the file was written by a newer version of Hive where
        // `orc.force.positional.evolution=true` was set (possibly because columns were renamed so
        // the physical schema doesn't match the data schema).
        // In these cases we map the physical schema to the data schema by index.
        assert(orcFieldNames.length <= dataSchema.length, "The given data schema " +
          s"${dataSchema.catalogString} (length:${dataSchema.length}) " +
          s"has fewer ${orcFieldNames.length - dataSchema.length} fields than " +
          s"the actual ORC physical schema $orcSchema (length:${orcFieldNames.length}), " +
          "no idea which columns were dropped, fail to read.")
        // for ORC file written by Hive, no field names
        // in the physical schema, there is a need to send the
        // entire dataSchema instead of required schema.
        // So pruneCols is not done in this case
        Some(requiredSchema.fieldNames.map { name =>
          val index = dataSchema.fieldIndex(name)
          if (index < orcFieldNames.length) {
            index
          } else {
            -1
          }
        }, false)
      } else {
        if (isCaseSensitive) {
          Some(requiredSchema.fieldNames.zipWithIndex.map { case (name, idx) =>
            if (orcFieldNames.indexWhere(caseSensitiveResolution(_, name)) != -1) {
              idx
            } else {
              -1
            }
          }, true)
        } else {
          // Do case-insensitive resolution only if in case-insensitive mode
          val caseInsensitiveOrcFieldMap = orcFieldNames.groupBy(_.toLowerCase(Locale.ROOT))
          Some(requiredSchema.fieldNames.zipWithIndex.map { case (requiredFieldName, idx) =>
            caseInsensitiveOrcFieldMap
              .get(requiredFieldName.toLowerCase(Locale.ROOT))
              .map { matchedOrcFields =>
                if (matchedOrcFields.size > 1) {
                  // Need to fail if there is ambiguity, i.e. more than one field is matched.
                  val matchedOrcFieldsString = matchedOrcFields.mkString("[", ", ", "]")
                  throw QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
                    requiredFieldName, matchedOrcFieldsString)
                } else {
                  idx
                }
              }.getOrElse(-1)
          }, true)
        }
      }
    }
  }

  /**
   * Add a metadata specifying Spark version.
   */
  def addSparkVersionMetadata(writer: Writer): Unit = {
    writer.addUserMetadata(SPARK_VERSION_METADATA_KEY, UTF_8.encode(SPARK_VERSION_SHORT))
  }

  /**
   * Given a `StructType` object, this methods converts it to corresponding string representation
   * in ORC.
   */
  def getOrcSchemaString(dt: DataType): String = dt match {
    case s: StructType =>
      val fieldTypes = s.fields.map { f =>
        s"${quoteIdentifier(f.name)}:${getOrcSchemaString(f.dataType)}"
      }
      s"struct<${fieldTypes.mkString(",")}>"
    case a: ArrayType =>
      s"array<${getOrcSchemaString(a.elementType)}>"
    case m: MapType =>
      s"map<${getOrcSchemaString(m.keyType)},${getOrcSchemaString(m.valueType)}>"
    case _: DayTimeIntervalType | _: TimestampNTZType => LongType.catalogString
    case _: YearMonthIntervalType => IntegerType.catalogString
    case _ => dt.catalogString
  }

  def orcTypeDescription(dt: DataType): TypeDescription = {
    def getInnerTypeDecription(dt: DataType): Option[TypeDescription] = {
      dt match {
        case y: YearMonthIntervalType =>
          val typeDesc = new TypeDescription(TypeDescription.Category.INT)
          typeDesc.setAttribute(CATALYST_TYPE_ATTRIBUTE_NAME, y.typeName)
          Some(typeDesc)
        case d: DayTimeIntervalType =>
          val typeDesc = new TypeDescription(TypeDescription.Category.LONG)
          typeDesc.setAttribute(CATALYST_TYPE_ATTRIBUTE_NAME, d.typeName)
          Some(typeDesc)
        case n: TimestampNTZType =>
          val typeDesc = new TypeDescription(TypeDescription.Category.LONG)
          typeDesc.setAttribute(CATALYST_TYPE_ATTRIBUTE_NAME, n.typeName)
          Some(typeDesc)
        case t: TimestampType =>
          val typeDesc = new TypeDescription(TypeDescription.Category.TIMESTAMP)
          typeDesc.setAttribute(CATALYST_TYPE_ATTRIBUTE_NAME, t.typeName)
          Some(typeDesc)
        case _: StringType =>
          val typeDesc = new TypeDescription(TypeDescription.Category.STRING)
          typeDesc.setAttribute(CATALYST_TYPE_ATTRIBUTE_NAME, StringType.typeName)
          Some(typeDesc)
        case _ => None
      }
    }

    dt match {
      case s: StructType =>
        val result = new TypeDescription(TypeDescription.Category.STRUCT)
        s.fields.foreach { f =>
          getInnerTypeDecription(f.dataType) match {
            case Some(t) => result.addField(f.name, t)
            case None => result.addField(f.name, orcTypeDescription(f.dataType))
          }
        }
        result
      case a: ArrayType =>
        val result = new TypeDescription(TypeDescription.Category.LIST)
        getInnerTypeDecription(a.elementType) match {
          case Some(t) => result.addChild(t)
          case None => result.addChild(orcTypeDescription(a.elementType))
        }
        result
      case m: MapType =>
        val result = new TypeDescription(TypeDescription.Category.MAP)
        getInnerTypeDecription(m.keyType) match {
          case Some(t) => result.addChild(t)
          case None => result.addChild(orcTypeDescription(m.keyType))
        }
        getInnerTypeDecription(m.valueType) match {
          case Some(t) => result.addChild(t)
          case None => result.addChild(orcTypeDescription(m.valueType))
        }
        result
      case other =>
        TypeDescription.fromString(other.catalogString)
    }
  }

  /**
   * Returns the result schema to read from ORC file. In addition, It sets
   * the schema string to 'orc.mapred.input.schema' so ORC reader can use later.
   *
   * @param canPruneCols Flag to decide whether pruned cols schema is send to resultSchema
   *                     or to send the entire dataSchema to resultSchema.
   * @param dataSchema   Schema of the orc files.
   * @param resultSchema Result data schema created after pruning cols.
   * @param partitionSchema Schema of partitions.
   * @param conf Hadoop Configuration.
   * @return Returns the result schema as string.
   */
  def orcResultSchemaString(
      canPruneCols: Boolean,
      dataSchema: StructType,
      resultSchema: StructType,
      partitionSchema: StructType,
      conf: Configuration): String = {
    val resultSchemaString = if (canPruneCols) {
      OrcUtils.getOrcSchemaString(resultSchema)
    } else {
      OrcUtils.getOrcSchemaString(StructType(dataSchema.fields ++ partitionSchema.fields))
    }
    OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, resultSchemaString)
    resultSchemaString
  }

  /**
   * Checks if `dataType` supports columnar reads.
   *
   * @param dataType Data type of the orc files.
   * @param nestedColumnEnabled True if columnar reads is enabled for nested column types.
   * @return Returns true if data type supports columnar reads.
   */
  def supportColumnarReads(
      dataType: DataType,
      nestedColumnEnabled: Boolean): Boolean = {
    dataType match {
      case _: AtomicType => true
      case st: StructType if nestedColumnEnabled =>
        st.forall(f => supportColumnarReads(f.dataType, nestedColumnEnabled))
      case ArrayType(elementType, _) if nestedColumnEnabled =>
        supportColumnarReads(elementType, nestedColumnEnabled)
      case MapType(keyType, valueType, _) if nestedColumnEnabled =>
        supportColumnarReads(keyType, nestedColumnEnabled) &&
          supportColumnarReads(valueType, nestedColumnEnabled)
      case _ => false
    }
  }

  /**
   * When the partial aggregates (Max/Min/Count) are pushed down to ORC, we don't need to read data
   * from ORC and aggregate at Spark layer. Instead we want to get the partial aggregates
   * (Max/Min/Count) result using the statistics information from ORC file footer, and then
   * construct an InternalRow from these aggregate results.
   *
   * NOTE: if statistics is missing from ORC file footer, exception would be thrown.
   *
   * @return Aggregate results in the format of InternalRow
   */
  def createAggInternalRowFromFooter(
      reader: Reader,
      filePath: String,
      dataSchema: StructType,
      partitionSchema: StructType,
      aggregation: Aggregation,
      aggSchema: StructType,
      partitionValues: InternalRow): InternalRow = {
    var columnsStatistics: OrcColumnStatistics = null
    try {
      columnsStatistics = OrcFooterReader.readStatistics(reader)
    } catch { case e: Exception =>
      throw new SparkException(
        s"Cannot read columns statistics in file: $filePath. Please consider disabling " +
        s"ORC aggregate push down by setting 'spark.sql.orc.aggregatePushdown' to false.", e)
    }

    // Get column statistics with column name.
    def getColumnStatistics(columnName: String): ColumnStatistics = {
      val columnIndex = dataSchema.fieldNames.indexOf(columnName)
      columnsStatistics.get(columnIndex).getStatistics
    }

    // Get Min/Max statistics and store as ORC `WritableComparable` format.
    // Return null if number of non-null values is zero.
    def getMinMaxFromColumnStatistics(
        statistics: ColumnStatistics,
        dataType: DataType,
        isMax: Boolean): WritableComparable[_] = {
      if (statistics.getNumberOfValues == 0) {
        return null
      }

      statistics match {
        case s: BooleanColumnStatistics =>
          val value = if (isMax) s.getTrueCount > 0 else !(s.getFalseCount > 0)
          new BooleanWritable(value)
        case s: IntegerColumnStatistics =>
          val value = if (isMax) s.getMaximum else s.getMinimum
          dataType match {
            case ByteType => new ByteWritable(value.toByte)
            case ShortType => new ShortWritable(value.toShort)
            case IntegerType => new IntWritable(value.toInt)
            case LongType => new LongWritable(value)
            case _ => throw new IllegalArgumentException(
              s"getMinMaxFromColumnStatistics should not take type $dataType " +
              "for IntegerColumnStatistics")
          }
        case s: DoubleColumnStatistics =>
          val value = if (isMax) s.getMaximum else s.getMinimum
          dataType match {
            case FloatType => new FloatWritable(value.toFloat)
            case DoubleType => new DoubleWritable(value)
            case _ => throw new IllegalArgumentException(
              s"getMinMaxFromColumnStatistics should not take type $dataType " +
                "for DoubleColumnStatistics")
          }
        case s: DateColumnStatistics =>
          new DateWritable(
            if (isMax) s.getMaximumDayOfEpoch.toInt else s.getMinimumDayOfEpoch.toInt)
        case _ => throw new IllegalArgumentException(
          s"getMinMaxFromColumnStatistics should not take ${statistics.getClass.getName}: " +
            s"$statistics as the ORC column statistics")
      }
    }

    // if there are group by columns, we will build result row first,
    // and then append group by columns values (partition columns values) to the result row.
    val schemaWithoutGroupBy =
      AggregatePushDownUtils.getSchemaWithoutGroupingExpression(aggSchema, aggregation)

    val aggORCValues: Seq[WritableComparable[_]] =
      aggregation.aggregateExpressions.zipWithIndex.map {
        case (max: Max, index) if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          val columnName = V2ColumnUtils.extractV2Column(max.column).get
          val statistics = getColumnStatistics(columnName)
          val dataType = schemaWithoutGroupBy(index).dataType
          getMinMaxFromColumnStatistics(statistics, dataType, isMax = true)
        case (min: Min, index) if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          val columnName = V2ColumnUtils.extractV2Column(min.column).get
          val statistics = getColumnStatistics(columnName)
          val dataType = schemaWithoutGroupBy.apply(index).dataType
          getMinMaxFromColumnStatistics(statistics, dataType, isMax = false)
        case (count: Count, _) if V2ColumnUtils.extractV2Column(count.column).isDefined =>
          val columnName = V2ColumnUtils.extractV2Column(count.column).get
          val isPartitionColumn = partitionSchema.fields.map(_.name).contains(columnName)
          // NOTE: Count(columnName) doesn't include null values.
          // org.apache.orc.ColumnStatistics.getNumberOfValues() returns number of non-null values
          // for ColumnStatistics of individual column. In addition to this, ORC also stores number
          // of all values (null and non-null) separately.
          val nonNullRowsCount = if (isPartitionColumn) {
            columnsStatistics.getStatistics.getNumberOfValues
          } else {
            getColumnStatistics(columnName).getNumberOfValues
          }
          new LongWritable(nonNullRowsCount)
        case (_: CountStar, _) =>
          // Count(*) includes both null and non-null values.
          new LongWritable(columnsStatistics.getStatistics.getNumberOfValues)
        case (x, _) =>
          throw new IllegalArgumentException(
            s"createAggInternalRowFromFooter should not take $x as the aggregate expression")
      }.toImmutableArraySeq

    val orcValuesDeserializer = new OrcDeserializer(schemaWithoutGroupBy,
      (0 until schemaWithoutGroupBy.length).toArray)
    val resultRow = orcValuesDeserializer.deserializeFromValues(aggORCValues)
    if (aggregation.groupByExpressions.nonEmpty) {
      val reOrderedPartitionValues = AggregatePushDownUtils.reOrderPartitionCol(
        partitionSchema, aggregation, partitionValues)
      new JoinedRow(reOrderedPartitionValues, resultRow)
    } else {
      resultRow
    }
  }
}
