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

package org.apache.spark.sql.streaming

import java.io.{BufferedWriter, DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream, FileWriter}

import scala.io.Source
import scala.util.Random

import com.google.common.io.ByteStreams

import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, FloatType, IntegerType, StringType, StructType}

class StreamingQueryHashPartitionVerifySuite extends StreamTest {
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.SHUFFLE_PARTITIONS.key, numShufflePartitions.toString)

  // Configs for golden file
  private val goldenFileURI =
    this.getClass.getResource("/structured-streaming/partition-tests/").toURI

  private val schemaFileName = "randomSchemas" // files for storing random input schemas
  private val rowAndPartIdFilename =
    "rowsAndPartIds" // files for storing random input rows and resulting partition ids
  private val codec = CompressionCodec.createCodec(
    sparkConf) // Used for compressing output to rowAndPartId file

  // Configs for random schema generation
  private val variableTypes = Seq(IntegerType, DoubleType, FloatType, BinaryType, StringType)
  private val numSchemaTypes = 1
  private val maxNumFields = 20

  // Configs for shuffle
  private val numRows = 10000
  private val numShufflePartitions = 100
  private def saveSchemas(schemas: Seq[StructType]) = {
    val writer = new BufferedWriter(new FileWriter(new File(goldenFileURI.getPath, schemaFileName)))
    schemas.foreach { schema =>
      writer.write(schema.toDDL)
      writer.newLine()
    }
    writer.close()
  }

  private def readSchemas(): Seq[StructType] = {
    val source = Source.fromFile(new File(goldenFileURI.getPath, schemaFileName))
    try {
      source
        .getLines()
        .map { ddl =>
          StructType.fromDDL(ddl)
        }
        .toArray // Avoid Stream lazy materialization
        .toSeq
    } finally source.close()
  }

  private def saveRowsAndPartIds(rows: Seq[UnsafeRow], partIds: Seq[Int], os: DataOutputStream) = {
    // Save the total number of rows
    os.writeInt(rows.length)
    // Save all rows
    rows.foreach { row =>
      // Save the row's total number of bytes
      val rowBytes = row.getBytes()
      // Save the row's actual bytes
      os.writeInt(rowBytes.size)
      os.write(rowBytes)
    }
    // Save all partIds, which should be in the same order as rows
    partIds.foreach { id =>
      os.writeInt(id)
    }
  }

  private def readRowsAndPartIds(is: DataInputStream): (Seq[UnsafeRow], Seq[Int]) = {
    val numRows = is.readInt()
    val rows = (1 to numRows).map { _ =>
      val rowSize = is.readInt()
      val rowBuffer = new Array[Byte](rowSize)
      ByteStreams.readFully(is, rowBuffer, 0, rowSize)
      val row = new UnsafeRow(1)
      row.pointTo(rowBuffer, rowSize)
      row
    }
    val partIds = (1 to numRows).map(_ => is.readInt()).toArray.toSeq
    (rows, partIds)
  }

  private def getRandomRows(schema: StructType, numRows: Int, rand: Random): Seq[UnsafeRow] = {
    val generator = RandomDataGenerator
      .forType(
        schema,
        rand = new Random(rand.nextInt(RandomDataGenerator.MAX_ARR_SIZE)),
        maxStringLength = 100,
        maxArrayLength = 4
      )
      .get

    // Create the converters needed to convert from external row to internal
    // row and to UnsafeRows. Projection itself costs a lot on initialization
    // (codegen and compile), so initialize it once.
    val internalConverter = CatalystTypeConverters.createToCatalystConverter(schema)
    val unsafeConverter = UnsafeProjection.create(Array(schema).asInstanceOf[Array[DataType]])

    (1 to numRows).map { _ =>
      val row = generator().asInstanceOf[Row]

      val internalRow = new GenericInternalRow(1)
      internalRow.update(0, internalConverter(row).asInstanceOf[GenericInternalRow])
      val unsafeRow = unsafeConverter.apply(internalRow)

      // UnsafeProjection returns the same UnsafeRow instance intentionally, so
      // unless doing deep copy, the hash partitions below will evaluate on the
      // same row thus return same value.
      unsafeRow.copy()
    }
  }

  private def getRandomSchemas(rand: Random): Seq[StructType] = {
    (1 to numSchemaTypes).map { _ =>
      RandomDataGenerator.randomNestedSchema(rand, maxNumFields, variableTypes)
    }
  }

  private def getPartitionId(rows: Seq[UnsafeRow], hash: HashPartitioning): Seq[Int] = {
    val partIdExpr = hash.partitionIdExpression
    rows.map { row =>
      partIdExpr.eval(row).asInstanceOf[Int]
    }
  }

  test("SPARK-47788: Ensure the same hash function is used across batches.") {
    val rowAndPartIdFile = new File(goldenFileURI.getPath, rowAndPartIdFilename)

    if (regenerateGoldenFiles) {
      val seed = Random.nextInt()
      val random = new Random(seed)
      logInfo(s"Get random inputs with seed $seed")

      val schemas = getRandomSchemas(random)

      val os = new DataOutputStream(
        codec.compressedOutputStream(new FileOutputStream(rowAndPartIdFile))
      )

      saveSchemas(schemas)

      schemas.foreach { schema =>
        // Streaming stateful ops rely on this distribution to partition the data.
        // Spark should make sure this class's partition dependency remain unchanged.
        val hash = StatefulOpClusteredDistribution(
          Seq(BoundReference(0, schema, nullable = true)),
          numShufflePartitions
        ).createPartitioning(numShufflePartitions)

        assert(
          hash.isInstanceOf[HashPartitioning],
          "StatefulOpClusteredDistribution should " +
          "rely on HashPartitioning to ensure partitions remain the same for streaming " +
          "stateful operators."
        )
        val rows = getRandomRows(schema, numRows, random)
        val partitions = getPartitionId(rows, hash.asInstanceOf[HashPartitioning])
        saveRowsAndPartIds(rows, partitions, os)
      }
      os.close()
    } else {
      val schemas = readSchemas()
      val is = new DataInputStream(
        codec.compressedInputStream(new FileInputStream(rowAndPartIdFile))
      )
      schemas.foreach { schema =>
        val hash = StatefulOpClusteredDistribution(
          Seq(BoundReference(0, schema, nullable = true)),
          numShufflePartitions
        ).createPartitioning(numShufflePartitions)

        assert(
          hash.isInstanceOf[HashPartitioning],
          "StatefulOpClusteredDistribution should " +
          "rely on HashPartitioning to ensure partitions remain the same for streaming " +
          "stateful operators."
        )

        val (rows, expectedPartitions) = readRowsAndPartIds(is)
        val partitions = getPartitionId(rows, hash.asInstanceOf[HashPartitioning])
        assert(
          partitions === expectedPartitions,
          "The partition ids do not match the expected partitions generated before."
        )
      }
      is.close()
    }
  }
}
