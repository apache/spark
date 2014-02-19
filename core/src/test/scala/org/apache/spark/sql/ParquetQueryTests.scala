package org.apache.spark.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File
import java.util.Arrays

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.column.ColumnDescriptor
import parquet.hadoop.metadata.{ParquetMetadata, CompressionCodecName}
import parquet.hadoop.{ParquetWriter, ParquetFileReader, ParquetFileWriter}
import parquet.bytes.BytesInput
import parquet.column.Encoding._
import parquet.column.page.PageReadStore

import org.apache.spark.sql.catalyst.expressions.{GenericRow, Attribute, Row}

object ParquetTestData {


  val testSchema = """message myrecord {
                      optional boolean myboolean;
                      optional int32 myint;
                      optional binary mystring;
                      optional int64 mylong;
                      optional float myfloat;
                      optional double mydouble;
                      }"""

  val testFile = new File("/tmp/testParquetFile").getAbsoluteFile

  val testData = ParquetRelation(testSchema, new Path(testFile.toURI))

  def writeFile = {
    testFile.delete
    val path: Path = new Path(testFile.toURI)
    val configuration: Configuration = new Configuration
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)

    val writeSupport = new RowWriteSupport()
    writeSupport.setSchema(schema, configuration)
    val writer = new ParquetWriter(path, writeSupport)
    for(i <- 0 until 15) {
      val data = new Array[Any](6)
      if(i % 3 ==0)
        data.update(0, true)
      else
        data.update(0, false)
      if(i % 5 == 0)
        data.update(1, 5)
      else
        data.update(1, null) // optional
      data.update(2, "abc")
      data.update(3, 1L<<33)
      data.update(4, 2.5F)
      data.update(5, 4.5D)
      writer.write(new GenericRow(data))
    }
    writer.close()
  }

  def writeFileAsPages = {
    testFile.delete
    val path: Path = new Path(testFile.toURI)
    val configuration: Configuration = new Configuration
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)

    val path1: Array[String] = Array("myboolean")
    val c1: ColumnDescriptor = schema.getColumnDescription(path1)
    val path2: Array[String] = Array("myint")
    val c2: ColumnDescriptor = schema.getColumnDescription(path2)
    val path3: Array[String] = Array("mystring")
    val c3: ColumnDescriptor = schema.getColumnDescription(path3)
    val path4: Array[String] = Array("mylong")
    val c4: ColumnDescriptor = schema.getColumnDescription(path4)
    val path5: Array[String] = Array("myfloat")
    val c5: ColumnDescriptor = schema.getColumnDescription(path5)
    val path6: Array[String] = Array("mydouble")
    val c6: ColumnDescriptor = schema.getColumnDescription(path6)

    val codec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED
    val w: ParquetFileWriter = new ParquetFileWriter(configuration, schema, path)

    w.start
    w.startBlock(3)
    w.startColumn(c1, 3, codec)
    // note to myself: repetition levels cannot be PLAIN encoded
    // boolean
    w.writeDataPage(3, 3, BytesInput.from(
      concat(serializeValue(true), serializeValue(false), serializeValue(false))
    ), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c2, 3, codec)
    // int
    w.writeDataPage(3, 12, BytesInput.from(
      concat(serializeValue(5), serializeValue(5), serializeValue(5))
    ), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c3, 3, codec)
    // string
    val bytes = serializeValue("abc".asInstanceOf[AnyVal])
    w.writeDataPage(3, 3*bytes.length, BytesInput.from(
      concat(bytes, bytes, bytes)
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c4, 3, codec)
    // long
    w.writeDataPage(3, 24, BytesInput.from(
      concat(serializeValue(1L<<33), serializeValue(1L<<33), serializeValue(1L<<33))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c5, 3, codec)
    // float
    w.writeDataPage(3, 12, BytesInput.from(
      concat(serializeValue(2.5F), serializeValue(2.5F), serializeValue(2.5F))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c6, 3, codec)
    // double
    w.writeDataPage(3, 24, BytesInput.from(
      concat(serializeValue(4.5D), serializeValue(4.5D), serializeValue(4.5D))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.endBlock
    w.startBlock(3)
    w.startColumn(c1, 3, codec)
    // note to myself: repetition levels cannot be PLAIN encoded
    // boolean
    w.writeDataPage(3, 3, BytesInput.from(
      concat(serializeValue(true), serializeValue(false), serializeValue(false))
    ), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c2, 3, codec)
    // int
    w.writeDataPage(3, 12, BytesInput.from(
      concat(serializeValue(5), serializeValue(5), serializeValue(5))
    ), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c3, 3, codec)
    // string
    w.writeDataPage(3, 3*bytes.length, BytesInput.from(
      concat(bytes, bytes, bytes)
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c4, 3, codec)
    // long
    w.writeDataPage(3, 24, BytesInput.from(
      concat(serializeValue(1L<<33), serializeValue(1L<<33), serializeValue(1L<<33))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c5, 3, codec)
    // float
    w.writeDataPage(3, 12, BytesInput.from(
      concat(serializeValue(2.5F), serializeValue(2.5F), serializeValue(2.5F))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.startColumn(c6, 3, codec)
    // double
    w.writeDataPage(3, 24, BytesInput.from(
      concat(serializeValue(4.5D), serializeValue(4.5D), serializeValue(4.5D))
    ), RLE, RLE, PLAIN)
    w.endColumn()
    w.endBlock
    w.end(new java.util.HashMap[String, String])
  }

  def readFile = {
    val configuration: Configuration = new Configuration
    val path = new Path(testFile.toURI)
    val schema: MessageType = MessageTypeParser.parseMessageType(ParquetTestData.testSchema)
    val path1: Array[String] = Array("myboolean")
    val c1: ColumnDescriptor = schema.getColumnDescription(path1)
    println(c1.toString)
    val path2: Array[String] = Array("myint")
    val readFooter: ParquetMetadata = ParquetFileReader.readFooter(configuration, path)
    println("this many blocks: " + readFooter.getBlocks.size())
    println("metadata: " + readFooter.getFileMetaData.toString)
    val r: ParquetFileReader = new ParquetFileReader(configuration, path, readFooter.getBlocks, Arrays.asList(schema.getColumnDescription(path1), schema.getColumnDescription(path2)))
    var pages: PageReadStore = r.readNextRowGroup
    println("number of rows first group " + pages.getRowCount)
    var pageReader = pages.getPageReader(c1)
    var page = pageReader.readPage()
    assert(page != null)
  }

  val complexSchema = "message m { required group a {required binary b;} required group c { required int64 d; }}"

  val complexTestFile: File = new File("/tmp/testComplexParquetFile").getAbsoluteFile

  val complexTestData = ParquetRelation(complexSchema, new Path(complexTestFile.toURI))

  // this second test is from TestParquetFileWriter
  def writeComplexFile = {
    complexTestFile.delete
    val path: Path = new Path(complexTestFile.toURI)
    val configuration: Configuration = new Configuration

    val schema: MessageType = MessageTypeParser.parseMessageType(ParquetTestData.complexSchema)
    val path1: Array[String] = Array("a", "b")
    val c1: ColumnDescriptor = schema.getColumnDescription(path1)
    val path2: Array[String] = Array("c", "d")
    val c2: ColumnDescriptor = schema.getColumnDescription(path2)

    val bytes1: Array[Byte] = Array(0, 1, 2, 3).map(_.toByte)
    val bytes2: Array[Byte] = Array(1, 2, 3, 4).map(_.toByte)
    val bytes3: Array[Byte] = Array(2, 3, 4, 5).map(_.toByte)
    val bytes4: Array[Byte] = Array(3, 4, 5, 6).map(_.toByte)

    val codec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED
    val w: ParquetFileWriter = new ParquetFileWriter(configuration, schema, path)

    w.start
    w.startBlock(3)
    w.startColumn(c1, 5, codec)
    val c1Starts: Long = w.getPos
    w.writeDataPage(2, 4, BytesInput.from(bytes1), RLE, RLE, PLAIN)
    w.writeDataPage(3, 4, BytesInput.from(bytes1), RLE, RLE, PLAIN)
    w.endColumn
    val c1Ends: Long = w.getPos
    w.startColumn(c2, 6, codec)
    val c2Starts: Long = w.getPos
    w.writeDataPage(2, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(3, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.endColumn
    val c2Ends: Long = w.getPos
    w.endBlock
    w.startBlock(4)
    w.startColumn(c1, 7, codec)
    w.writeDataPage(7, 4, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c2, 8, codec)
    w.writeDataPage(8, 4, BytesInput.from(bytes4), RLE, RLE, PLAIN)
    w.endColumn
    w.endBlock
    w.end(new java.util.HashMap[String, String])
  }

  /**
   * Serializes a given value so that it conforms to Parquet's uncompressed primitive value encoding.
   *
   * @param value The value to serialize.
   * @return A byte array that contains the serialized value.
   */
  private def serializeValue(value: Any) : Array[Byte] = {
    value match {
      case i: Int => {
        val bb = java.nio.ByteBuffer.allocate(4)
        bb.order(java.nio.ByteOrder.LITTLE_ENDIAN)
        bb.putInt(i)
        bb.array()
      }
      case l: Long => {
        val bb = java.nio.ByteBuffer.allocate(8)
        bb.order(java.nio.ByteOrder.LITTLE_ENDIAN)
        bb.putLong(l)
        bb.array()
      }
      case f: Float => serializeValue(java.lang.Float.floatToIntBits(f))
      case d: Double => serializeValue(java.lang.Double.doubleToLongBits(d))
      case s: String => {
        // apparently strings are encoded as their length as int followed by the string
        val bytes: Array[Byte] = s.getBytes("UTF-8")
        (serializeValue(bytes.length).toList ::: bytes.toList).toArray
      }
      case b: Boolean => {
        if(b)
          Array(1.toByte)
        else
          Array(0.toByte)
      }
    }
  }

  /**
   * Concatenates a sequence of byte arrays.
   *
   * @param values The sequence of byte arrays to be concatenated.
   * @return The concatenation of the given byte arrays.
   */
  private def concat(values: Array[Byte]*) : Array[Byte] = {
    var retval = List[Byte]()
    for(value <- values)
      retval = retval ::: value.toList
    retval.toArray
  }
}

class ParquetQueryTests extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    ParquetTestData.writeFile
    ParquetTestData.readFile
    ParquetTestData.writeComplexFile
  }

  test("Import of simple Parquet file") {
    val result = getRDD(ParquetTestData.testData).collect()
    val allChecks: Boolean = result.zipWithIndex.forall {
      case (row, index) => {
        val checkBoolean =
          if (index % 3 == 0)
            (row(0) == true)
          else
            (row(0) == false)
        val checkInt = ((index % 5) != 0) || (row(1) == 5)
        val checkString = (row(2) == "abc")
        val checkLong = (row(3) == (1L<<33))
        val checkFloat = (row(4) == 2.5F)
        val checkDouble = (row(5) == 4.5D)
        checkBoolean && checkInt && checkString && checkLong && checkFloat && checkDouble
      }
    }
    assert(allChecks)
  }

  /**
   * Computes the given [[org.apache.spark.sql.ParquetRelation]] and returns its RDD.
   *
   * @param parquetRelation The Parquet relation.
   * @return An RDD of Rows.
   */
  private def getRDD(parquetRelation: ParquetRelation): RDD[Row] = {
    val catalystSchema: List[Attribute] = ParquetTypesConverter.convertToAttributes(parquetRelation.parquetSchema)
    val scanner = new ParquetTableScan(catalystSchema, parquetRelation, None)(TestSqlContext.sparkContext)
    scanner.execute
  }
}