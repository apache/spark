package catalyst
package execution

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File
import java.util.Arrays

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.column.ColumnDescriptor
import parquet.hadoop.metadata.{ParquetMetadata, CompressionCodecName}
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import parquet.bytes.BytesInput
import parquet.column.Encoding._
import parquet.column.page.PageReadStore

import expressions._

/* Implicits */
import dsl._

object ParquetTestData {


  val testSchema = """message myrecord {
                      required boolean myboolean;
                      required int32 myint;
                      required binary mystring;
                      }"""

  val testFile = new File("/tmp/testParquetFile").getAbsoluteFile

  val testData = ParquetRelation(testSchema, new Path(testFile.toURI))

  def writeFile = {
    testFile.delete
    val path: Path = new Path(testFile.toURI)
    val configuration: Configuration = new Configuration

    val schema: MessageType = MessageTypeParser.parseMessageType(ParquetTestData.testSchema)
    val path1: Array[String] = Array("myboolean")
    val c1: ColumnDescriptor = schema.getColumnDescription(path1)
    val path2: Array[String] = Array("myint")
    val c2: ColumnDescriptor = schema.getColumnDescription(path2)
    val path3: Array[String] = Array("mystring")
    val c3: ColumnDescriptor = schema.getColumnDescription(path3)

    val bytes1: Array[Byte] = Array(1, 0, 0).map(_.toByte)
    val bytes2: Array[Byte] = Array(5, 0, 0, 0).map(_.toByte)

    // apparently strings are encoded as their length as int followed by the string
    val bb = java.nio.ByteBuffer.allocate(4)
    bb.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val tmpbytes3: Array[Byte] = "abc".getBytes("UTF-8")
    bb.putInt(tmpbytes3.length)
    val bytes3: Array[Byte] = (bb.array().toList ::: tmpbytes3.toList).toArray

    val codec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED
    val w: ParquetFileWriter = new ParquetFileWriter(configuration, schema, path)

    w.start
    w.startBlock(3)
    w.startColumn(c1, 3, codec)
    // note to myself: repetition levels cannot be PLAIN encoded
    w.writeDataPage(3, 3, BytesInput.from(bytes1), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c2, 3, codec)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c3, 3, codec)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.endColumn()
    w.endBlock
    w.startBlock(3)
    w.startColumn(c1, 3, codec)
    w.writeDataPage(3, 3, BytesInput.from(bytes1), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c2, 3, codec)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.writeDataPage(1, 4, BytesInput.from(bytes2), RLE, RLE, PLAIN)
    w.endColumn
    w.startColumn(c3, 3, codec)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
    w.writeDataPage(1, bytes3.length, BytesInput.from(bytes3), RLE, RLE, PLAIN)
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
    println("number of rows first group" + pages.getRowCount)
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
        val checkInt = (row(1) == 5)
        val checkString = (row(2) == "abc")
        checkBoolean && checkInt && checkString
      }
    }
    assert(allChecks)
  }

  private def getRDD(parquetRelation: ParquetRelation): RDD[Row] = {
    val sharkInstance = new TestSharkInstance
    val catalystSchema: List[Attribute] = ParquetTypesConverter.convertToAttributes(parquetRelation.parquetSchema)
    val scanner = new ParquetTableScan(catalystSchema, parquetRelation, Option(""))(sharkInstance.sc)
    scanner.execute()
  }
}