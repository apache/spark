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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.{Optional, PrimitiveIterator}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.parquet.column.{ColumnDescriptor, ParquetProperties}
import org.apache.parquet.column.impl.ColumnWriteStoreV1
import org.apache.parquet.column.page._
import org.apache.parquet.column.page.mem.MemPageStore
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.ParquetRowGroupReader
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * A test suite on the vectorized Parquet reader. Unlike `ParquetIOSuite`, this focuses on
 * low-level decoding logic covering column index, dictionary, different batch and page sizes, etc.
 */
class ParquetVectorizedSuite extends QueryTest with ParquetTest with SharedSparkSession {
  private val VALUES: Seq[String] = ('a' to 'z').map(_.toString)
  private val NUM_VALUES: Int = VALUES.length
  private val BATCH_SIZE_CONFIGS: Seq[Int] = Seq(1, 3, 5, 7, 10, 20, 40)
  private val PAGE_SIZE_CONFIGS: Seq[Seq[Int]] = Seq(Seq(6, 6, 7, 7), Seq(4, 9, 4, 9))

  implicit def toStrings(ints: Seq[Int]): Seq[String] = ints.map(i => ('a' + i).toChar.toString)

  test("primitive type - no column index") {
    BATCH_SIZE_CONFIGS.foreach { batchSize =>
      PAGE_SIZE_CONFIGS.foreach { pageSizes =>
        Seq(true, false).foreach { dictionaryEnabled =>
          testPrimitiveString(None, None, pageSizes, VALUES, batchSize,
            dictionaryEnabled = dictionaryEnabled)
        }
      }
    }
  }

  test("primitive type - column index with ranges") {
    BATCH_SIZE_CONFIGS.foreach { batchSize =>
      PAGE_SIZE_CONFIGS.foreach { pageSizes =>
        Seq(true, false).foreach { dictionaryEnabled =>
          var ranges = Seq((0L, 9L))
          testPrimitiveString(None, Some(ranges), pageSizes, 0 to 9, batchSize,
            dictionaryEnabled = dictionaryEnabled)

          ranges = Seq((30, 50))
          testPrimitiveString(None, Some(ranges), pageSizes, Seq.empty, batchSize,
            dictionaryEnabled = dictionaryEnabled)

          ranges = Seq((15, 25))
          testPrimitiveString(None, Some(ranges), pageSizes, 15 to 19, batchSize,
            dictionaryEnabled = dictionaryEnabled)

          ranges = Seq((19, 20))
          testPrimitiveString(None, Some(ranges), pageSizes, 19 to 20, batchSize,
            dictionaryEnabled = dictionaryEnabled)

          ranges = Seq((0, 3), (5, 7), (15, 18))
          testPrimitiveString(None, Some(ranges), pageSizes,
            toStrings(Seq(0, 1, 2, 3, 5, 6, 7, 15, 16, 17, 18)),
            batchSize, dictionaryEnabled = dictionaryEnabled)
        }
      }
    }
  }

  test("primitive type - column index with ranges and nulls") {
    BATCH_SIZE_CONFIGS.foreach { batchSize =>
      PAGE_SIZE_CONFIGS.foreach { pageSizes =>
        Seq(true, false).foreach { dictionaryEnabled =>
          val valuesWithNulls = VALUES.zipWithIndex.map {
            case (v, i) => if (i % 2 == 0) null else v
          }
          testPrimitiveString(None, None, pageSizes, valuesWithNulls, batchSize, valuesWithNulls,
            dictionaryEnabled)

          val ranges = Seq((5L, 7L))
          testPrimitiveString(None, Some(ranges), pageSizes, Seq("f", null, "h"),
            batchSize, valuesWithNulls, dictionaryEnabled)
        }
      }
    }
  }

  test("primitive type - column index with ranges and first row indexes") {
    BATCH_SIZE_CONFIGS.foreach { batchSize =>
      Seq(true, false).foreach { dictionaryEnabled =>
        // Single page
        val firstRowIndex = 10
        var ranges = Seq((0L, 9L))
        testPrimitiveString(Some(Seq(firstRowIndex)), Some(ranges), Seq(VALUES.length),
          Seq.empty, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 25))
        testPrimitiveString(Some(Seq(firstRowIndex)), Some(ranges), Seq(VALUES.length),
          5 to 15, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 35))
        testPrimitiveString(Some(Seq(firstRowIndex)), Some(ranges), Seq(VALUES.length),
          5 to 19, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 39))
        testPrimitiveString(Some(Seq(firstRowIndex)), Some(ranges), Seq(VALUES.length),
          5 to 19, batchSize, dictionaryEnabled = dictionaryEnabled)

        // Row indexes:  [ [10, 16), [20, 26), [30, 37), [40, 47) ]
        // Values:       [ [0,  6),  [6,  12), [12, 19), [19, 26) ]
        var pageSizes = Seq(6, 6, 7, 7)
        var firstRowIndexes = Seq(10L, 20, 30, 40)

        ranges = Seq((0L, 9L))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          Seq.empty, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 25))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          5 to 9, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 35))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          5 to 14, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((15, 60))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          5 to 19, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((12, 22), (28, 38))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          toStrings(Seq(2, 3, 4, 5, 6, 7, 8, 12, 13, 14, 15, 16, 17, 18)), batchSize,
          dictionaryEnabled = dictionaryEnabled)

        // Row indexes: [ [10, 11), [40, 52), [100, 112), [200, 201) ]
        // Values:      [ [0, 1),   [1, 13),  [13, 25),   [25, 26]   ]
        pageSizes = Seq(1, 12, 12, 1)
        firstRowIndexes = Seq(10L, 40, 100, 200)
        ranges = Seq((0L, 9L))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          Seq.empty, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((300, 350))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          Seq.empty, batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((50, 80))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          (11 to 12), batchSize, dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((0, 150))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          0 to 24, batchSize, dictionaryEnabled = dictionaryEnabled)

        // with nulls
        val valuesWithNulls = VALUES.zipWithIndex.map {
          case (v, i) => if (i % 2 == 0) null else v
        }
        ranges = Seq((20, 45)) // select values in [1, 5]
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          Seq("b", null, "d", null, "f"), batchSize, valuesWithNulls,
          dictionaryEnabled = dictionaryEnabled)

        ranges = Seq((8, 12), (80, 104))
        testPrimitiveString(Some(firstRowIndexes), Some(ranges), pageSizes,
          Seq(null, "n", null, "p", null, "r"), batchSize, valuesWithNulls,
          dictionaryEnabled = dictionaryEnabled)
      }
    }
  }

  private def testPrimitiveString(
      firstRowIndexesOpt: Option[Seq[Long]],
      rangesOpt: Option[Seq[(Long, Long)]],
      pageSizes: Seq[Int],
      expectedValues: Seq[String],
      batchSize: Int,
      inputValues: Seq[String] = VALUES,
      dictionaryEnabled: Boolean = false): Unit = {
    assert(pageSizes.sum == inputValues.length)
    firstRowIndexesOpt.foreach(a => assert(pageSizes.length == a.length))

    val isRequiredStr = if (!expectedValues.contains(null)) "required" else "optional"
    val parquetSchema: MessageType = MessageTypeParser.parseMessageType(
      s"""message root {
         | $isRequiredStr binary a(UTF8);
         |}
         |""".stripMargin
    )
    val maxDef = if (inputValues.contains(null)) 1 else 0
    val ty = parquetSchema.asGroupType().getType("a").asPrimitiveType()
    val cd = new ColumnDescriptor(Seq("a").toArray, ty, 0, maxDef)
    val repetitionLevels = Array.fill[Int](inputValues.length)(0)
    val definitionLevels = inputValues.map(v => if (v == null) 0 else 1)

    val memPageStore = new MemPageStore(expectedValues.length)

    var i = 0
    val pageFirstRowIndexes = ArrayBuffer.empty[Long]
    pageSizes.foreach { size =>
      pageFirstRowIndexes += i
      writeDataPage(cd, memPageStore, repetitionLevels.slice(i, i + size),
        definitionLevels.slice(i, i + size), inputValues.slice(i, i + size), maxDef,
        dictionaryEnabled)
      i += size
    }

    checkAnswer(expectedValues.length, parquetSchema,
      TestPageReadStore(memPageStore, firstRowIndexesOpt.getOrElse(pageFirstRowIndexes).toSeq,
        rangesOpt), expectedValues.map(i => Row(i)), batchSize)
  }

  /**
   * Write a single data page using repetition levels, definition levels and values provided.
   *
   * Note that this requires `repetitionLevels`, `definitionLevels` and `values` to have the same
   * number of elements. For null values, the corresponding slots in `values` will be skipped.
   */
  private def writeDataPage(
      columnDesc: ColumnDescriptor,
      pageWriteStore: PageWriteStore,
      repetitionLevels: Seq[Int],
      definitionLevels: Seq[Int],
      values: Seq[Any],
      maxDefinitionLevel: Int,
      dictionaryEnabled: Boolean = false): Unit = {
    val columnWriterStore = new ColumnWriteStoreV1(pageWriteStore,
      ParquetProperties.builder()
          .withPageSize(4096)
          .withDictionaryEncoding(dictionaryEnabled)
          .build())
    val columnWriter = columnWriterStore.getColumnWriter(columnDesc)

    repetitionLevels.zip(definitionLevels).zipWithIndex.foreach { case ((rl, dl), i) =>
      if (dl < maxDefinitionLevel) {
        columnWriter.writeNull(rl, dl)
      } else {
        columnDesc.getPrimitiveType.getPrimitiveTypeName match {
          case PrimitiveTypeName.INT32 =>
            columnWriter.write(values(i).asInstanceOf[Int], rl, dl)
          case PrimitiveTypeName.INT64 =>
            columnWriter.write(values(i).asInstanceOf[Long], rl, dl)
          case PrimitiveTypeName.BOOLEAN =>
            columnWriter.write(values(i).asInstanceOf[Boolean], rl, dl)
          case PrimitiveTypeName.FLOAT =>
            columnWriter.write(values(i).asInstanceOf[Float], rl, dl)
          case PrimitiveTypeName.DOUBLE =>
            columnWriter.write(values(i).asInstanceOf[Double], rl, dl)
          case PrimitiveTypeName.BINARY =>
            columnWriter.write(Binary.fromString(values(i).asInstanceOf[String]), rl, dl)
          case _ =>
            throw new IllegalStateException(s"Unexpected type: " +
                s"${columnDesc.getPrimitiveType.getPrimitiveTypeName}")
        }
      }
      columnWriterStore.endRecord()
    }
    columnWriterStore.flush()
  }

  private def checkAnswer(
      totalRowCount: Int,
      fileSchema: MessageType,
      readStore: PageReadStore,
      expected: Seq[Row],
      batchSize: Int = NUM_VALUES): Unit = {
    import collection.JavaConverters._

    val recordReader = new VectorizedParquetRecordReader(
      DateTimeUtils.getZoneId("EST"),
      "CORRECTED",
      "UTC",
      "CORRECTED",
      "UTC",
      true,
      batchSize)
    recordReader.initialize(fileSchema, fileSchema,
      TestParquetRowGroupReader(Seq(readStore)), totalRowCount)

    // convert both actual and expected rows into collections
    val schema = recordReader.sparkSchema
    val expectedRowIt = ColumnVectorUtils.toBatch(
      schema, MemoryMode.ON_HEAP, expected.iterator.asJava).rowIterator()

    val rowOrdering = RowOrdering.createNaturalAscendingOrdering(schema.map(_.dataType))
    var i = 0
    while (expectedRowIt.hasNext && recordReader.nextKeyValue()) {
      val expectedRow = expectedRowIt.next()
      val actualRow = recordReader.getCurrentValue.asInstanceOf[InternalRow]
      assert(rowOrdering.compare(expectedRow, actualRow) == 0, {
        val expectedRowStr = toDebugString(schema, expectedRow)
        val actualRowStr = toDebugString(schema, actualRow)
        s"at index $i, expected row: $expectedRowStr doesn't match actual row: $actualRowStr"
      })
      i += 1
    }
  }

  private def toDebugString(schema: StructType, row: InternalRow): String = {
    if (row == null) "null"
    else {
      val fieldStrings = schema.fields.zipWithIndex.map { case (f, i) =>
        f.dataType match {
          case IntegerType =>
            row.getInt(i).toString
          case StringType =>
            val utf8Str = row.getUTF8String(i)
            if (utf8Str == null) "null"
            else utf8Str.toString
          case ArrayType(_, _) =>
            val elements = row.getArray(i)
            if (elements == null) "null"
            else elements.array.mkString("[", ", ", "]")
          case _ =>
            throw new IllegalArgumentException(s"Unsupported data type: ${f.dataType}")
        }
      }
      fieldStrings.mkString(", ")
    }
  }

  case class TestParquetRowGroupReader(groups: Seq[PageReadStore]) extends ParquetRowGroupReader {
    private var index: Int = 0

    override def readNextRowGroup(): PageReadStore = {
      if (index == groups.length) {
        null
      } else {
        val res = groups(index)
        index += 1
        res
      }
    }

    override def close(): Unit = {}
  }

  private case class TestPageReadStore(
      wrapped: PageReadStore,
      firstRowIndexes: Seq[Long],
      rowIndexRangesOpt: Option[Seq[(Long, Long)]] = None) extends PageReadStore {

    override def getPageReader(descriptor: ColumnDescriptor): PageReader = {
      val originalReader = wrapped.getPageReader(descriptor)
      TestPageReader(originalReader, firstRowIndexes)
    }

    override def getRowCount: Long = wrapped.getRowCount

    override def getRowIndexes: Optional[PrimitiveIterator.OfLong] = {
      rowIndexRangesOpt.map { ranges =>
        Optional.of(new PrimitiveIterator.OfLong {
          private var currentRangeIdx: Int = 0
          private var currentRowIdx: Long = -1

          override def nextLong(): Long = {
            if (!hasNext) throw new NoSuchElementException("No more element")
            val res = currentRowIdx
            currentRowIdx += 1
            res
          }

          override def hasNext: Boolean = {
            while (currentRangeIdx < ranges.length) {
              if (currentRowIdx > ranges(currentRangeIdx)._2) {
                // we've exhausted the current range - move to the next range
                currentRangeIdx += 1
                currentRowIdx = -1
              } else {
                if (currentRowIdx == -1) {
                  currentRowIdx = ranges(currentRangeIdx)._1
                }
                return true
              }
            }
            false
          }
        })
      }.getOrElse(Optional.empty())
    }
  }

  private case class TestPageReader(
      wrapped: PageReader,
      firstRowIndexes: Seq[Long]) extends PageReader {
    private var index = 0

    override def readDictionaryPage(): DictionaryPage = wrapped.readDictionaryPage()
    override def getTotalValueCount: Long = wrapped.getTotalValueCount
    override def readPage(): DataPage = {
      val wrappedPage = try {
        wrapped.readPage()
      } catch {
        case _: ParquetDecodingException =>
          null
      }
      if (wrappedPage == null) {
        wrappedPage
      } else {
        val res = new TestDataPage(wrappedPage, firstRowIndexes(index))
        index += 1
        res
      }
    }
  }
}
