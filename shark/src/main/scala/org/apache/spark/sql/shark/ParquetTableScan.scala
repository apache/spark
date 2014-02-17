package org.apache.spark.sql
package shark

import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD

import parquet.io.api._
import parquet.schema.MessageType
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.api.ReadSupport
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.ReadSupport.ReadContext

import catalyst.expressions.{Attribute, GenericRow, Row, Expression}
import catalyst.plans.logical.LeafNode
import catalyst.types._

import org.apache.hadoop.mapreduce.Job
/**
 * Parquet table scan operator. Imports the file that backs the given [[org.apache.spark.sql.shark.ParquetRelation]]
 * as a RDD[Row]. Only a stub currently.
 */
case class ParquetTableScan(
                             attributes: Seq[Attribute],
                             relation: ParquetRelation,
                             partitionPruningPred: Option[Expression] // not used
                             )(
                             @transient val sc: SharkContext)
  extends LeafNode {

  private val _broadcastedHiveConf =
    sc.sparkContext.broadcast(new SerializableWritable(sc.hiveconf))

  def broadcastedHiveConf = _broadcastedHiveConf

  def hiveConf = _broadcastedHiveConf.value.value

  // TODO: currently this is not used
  private val _minSplitsPerRDD = math.min(
    math.max(sc.hiveconf.getInt("mapred.map.tasks", 1), sc. sparkContext.defaultMinSplits),
    relation.numberOfBlocks // we don't want to have more splits than blocks
  )

  /**
   * Runs this query returning the result as an RDD.
  */
  def execute(): RDD[Row] = {
    // TODO: for now we do not check whether the relation's schema matches the one of the
    // underlying Parquet file

    val job = new Job(sc.hiveconf)
    ParquetInputFormat.setReadSupportClass(job, classOf[org.apache.spark.sql.shark.RowReadSupport])
    // TODO: add record filters, etc.
    sc.sparkContext.newAPIHadoopFile(
      relation.path.toUri.toString,
      classOf[ParquetInputFormat[Row]],
      classOf[Void], classOf[Row],
      job.getConfiguration)
      .map(_._2)
  }

  override def output: Seq[Attribute] = attributes // right now we pass everything through, always
}

/**
 * A [[parquet.io.api.RecordMaterializer]] for Rows.
 *
 * @param root The root group converter for the record.
 */
class RowRecordMaterializer(root: CatalystGroupConverter) extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType) = this(new CatalystGroupConverter(ParquetTypesConverter.convertToAttributes(parquetSchema)))

  override def getCurrentRecord: Row = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root
}

/**
 * A [[parquet.hadoop.api.ReadSupport]] for Row objects.
 */
class RowReadSupport extends ReadSupport[Row] with Logging {
  override def prepareForRead(
                               conf: Configuration,
                               stringMap: java.util.Map[String, String],
                               fileSchema: MessageType,
                               readContext: ReadContext
                               ): RecordMaterializer[Row] = {
    logger.debug(s"preparing for read with schema ${fileSchema.toString}")
    new RowRecordMaterializer(fileSchema)
  }

  override def init(
                     configuration: Configuration,
                     keyValueMetaData: java.util.Map[String, String],
                     fileSchema: MessageType
                     ): ReadContext = {
    logger.debug(s"read support initialized for schema ${fileSchema.toString}")
    new ReadContext(fileSchema, keyValueMetaData)
  }
}

/**
 * A [[parquet.io.api.GroupConverter]] that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Shark schema in the form of a list of attributes.
 */
class CatalystGroupConverter(schema: Seq[Attribute]) extends GroupConverter {
  var current: GenericRow = new GenericRow(Seq())
  // initialization may not strictly be required
  val currentData: Array[Any] = new Array[Any](schema.length)

  val converters: Array[Converter] = schema.map {
    a => a.dataType match {
      case ctype: NativeType =>
        // note: for some reason matching for StringType fails so use this ugly if instead
        if (ctype == StringType) new CatalystPrimitiveStringConverter(this, schema.indexOf(a))
        else new CatalystPrimitiveConverter(this, schema.indexOf(a))
      case _ => throw new RuntimeException("unable to convert datatype in CatalystGroupConverter")
    }
  }.toArray

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: GenericRow = current

  override def start(): Unit = {
    for (i <- 0 until schema.length) {
      currentData.update(i, Nil)
    }
  }

  override def end(): Unit = {
    current = new GenericRow(currentData)
  }
}

/**
 * A [[parquet.io.api.PrimitiveConverter]] that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveConverter(parent: CatalystGroupConverter, fieldIndex: Int) extends PrimitiveConverter {
  // TODO: consider refactoring these together with ParquetTypesConverter
  override def addBinary(value: Binary): Unit = parent.currentData.update(fieldIndex, value.getBytes.asInstanceOf[BinaryType.JvmType])

  override def addBoolean(value: Boolean): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[BooleanType.JvmType])

  override def addDouble(value: Double): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[DoubleType.JvmType])

  override def addFloat(value: Float): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[FloatType.JvmType])

  override def addInt(value: Int): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[IntegerType.JvmType])

  override def addLong(value: Long): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[LongType.JvmType])
}

/**
 * A [[parquet.io.api.PrimitiveConverter]] that converts Parquet strings (fixed-length byte arrays) into Catalyst Strings.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveStringConverter(parent: CatalystGroupConverter, fieldIndex: Int) extends CatalystPrimitiveConverter(parent, fieldIndex) {
  override def addBinary(value: Binary): Unit = parent.currentData.update(fieldIndex, value.toStringUsingUTF8.asInstanceOf[StringType.JvmType])
}
