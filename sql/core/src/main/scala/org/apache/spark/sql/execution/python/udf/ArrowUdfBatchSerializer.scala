package org.apache.spark.sql.execution.python.udf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

/**
 * Serializer for converting between Spark's internal row format and Arrow IPC
 * byte arrays, used by the SPARK-55278 external UDF protocol.
 *
 * The protocol embeds Arrow IPC streaming format bytes within gRPC protobuf
 * messages. This class handles the conversion at the executor side:
 *   - Serialize: InternalRow iterator -> Arrow IPC bytes (for sending to worker)
 *   - Deserialize: Arrow IPC bytes -> InternalRow iterator (for receiving results)
 */
object ArrowUdfBatchSerializer {

  /**
   * Create a new Arrow allocator scoped to a specific operation.
   * Callers should close the allocator when done to release memory.
   * For production use, integrate with Spark's TaskMemoryManager.
   */
  def newAllocator(name: String = "spark-udf-arrow"): BufferAllocator = {
    new RootAllocator(Long.MaxValue)
  }

  /**
   * Serialize a batch of InternalRows to Arrow IPC streaming format bytes.
   *
   * @param rows Iterator of InternalRows to serialize
   * @param schema Spark StructType schema for the rows
   * @param maxBatchRows Maximum number of rows per Arrow RecordBatch
   * @param allocator Arrow memory allocator (caller must provide and close when done)
   * @return Array of (Arrow IPC bytes, row count) tuples, one per batch
   */
  def serializeBatch(
      rows: Iterator[InternalRow],
      schema: StructType,
      maxBatchRows: Int = 8192,
      allocator: BufferAllocator): Array[(Array[Byte], Long)] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId = "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    val batches = scala.collection.mutable.ArrayBuffer[(Array[Byte], Long)]()

    try {
      var rowCount = 0L
      val out = new ByteArrayOutputStream()

      rows.foreach { row =>
        arrowWriter.write(row)
        rowCount += 1

        if (rowCount >= maxBatchRows) {
          arrowWriter.finish()
          out.reset()
          val writer = new ArrowStreamWriter(root, null, out)
          writer.start()
          writer.writeBatch()
          writer.end()
          writer.close()
          batches += ((out.toByteArray, rowCount))

          arrowWriter.reset()
          rowCount = 0
        }
      }

      // Flush remaining rows
      if (rowCount > 0) {
        arrowWriter.finish()
        out.reset()
        val writer = new ArrowStreamWriter(root, null, out)
        writer.start()
        writer.writeBatch()
        writer.end()
        writer.close()
        batches += ((out.toByteArray, rowCount))
      }

      batches.toArray
    } finally {
      arrowWriter.reset()
      root.close()
    }
  }

  /**
   * Deserialize Arrow IPC streaming format bytes to an iterator of InternalRows.
   *
   * @param arrowBytes Arrow IPC bytes from the worker
   * @param schema Expected Spark StructType schema
   * @return Iterator of InternalRows
   */
  def deserializeBatch(
      arrowBytes: Array[Byte],
      schema: StructType): Iterator[InternalRow] = {

    ArrowConverters.fromBatchIterator(
      Iterator(arrowBytes),
      schema,
      timeZoneId = "UTC",
      errorOnDuplicatedFieldNames = true,
      context = null)
  }

  /**
   * Serialize an Arrow Schema to IPC bytes for schema negotiation.
   * Used in the InitializeUdf RPC.
   *
   * @param schema Spark StructType to convert
   * @return Arrow IPC Schema message bytes
   */
  def serializeSchema(schema: StructType): Array[Byte] = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId = "UTC")
    val out = new ByteArrayOutputStream()
    val allocator = newAllocator("schema-serialize")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      val writer = new ArrowStreamWriter(root, null, out)
      writer.start()
      writer.end()
      writer.close()
      out.toByteArray
    } finally {
      root.close()
      allocator.close()
    }
  }

  /**
   * Deserialize Arrow IPC Schema bytes back to a Spark StructType.
   * Used to validate the worker's actual_output_schema in InitializeUdfResult.
   *
   * @param schemaBytes Arrow IPC Schema message bytes
   * @return Spark StructType
   */
  def deserializeSchema(schemaBytes: Array[Byte]): StructType = {
    val allocator = newAllocator("schema-deserialize")

    val in = new ByteArrayInputStream(schemaBytes)
    val reader = new ArrowStreamReader(in, allocator)
    try {
      val arrowSchema = reader.getVectorSchemaRoot.getSchema
      ArrowUtils.fromArrowSchema(arrowSchema)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  /**
   * Validate that two schemas are compatible for the UDF protocol.
   * Compatible means: same number of fields, compatible types (widening OK),
   * nullable to non-nullable OK.
   *
   * @param expected The schema declared by the executor
   * @param actual The schema returned by the worker
   * @return None if compatible, Some(error message) if incompatible
   */
  def validateSchemaCompatibility(
      expected: StructType,
      actual: StructType): Option[String] = {

    if (expected.length != actual.length) {
      return Some(
        s"Schema field count mismatch: expected ${expected.length} fields, " +
        s"got ${actual.length}")
    }

    val mismatches = expected.zip(actual).zipWithIndex.flatMap {
      case ((expectedField, actualField), idx) =>
        if (!expectedField.dataType.sameType(actualField.dataType)) {
          Some(s"Field $idx '${expectedField.name}': expected " +
            s"${expectedField.dataType.simpleString}, got ${actualField.dataType.simpleString}")
        } else {
          None
        }
    }

    if (mismatches.nonEmpty) {
      Some("Schema incompatible:\n" + mismatches.mkString("\n"))
    } else {
      None
    }
  }
}
