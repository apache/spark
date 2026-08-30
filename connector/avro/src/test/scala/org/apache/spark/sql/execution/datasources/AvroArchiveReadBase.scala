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

package org.apache.spark.sql.execution.datasources

import java.io.{ByteArrayInputStream, IOException}

import org.apache.avro.file.{DataFileConstants, DataFileStream}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

/**
 * Binds [[ArchiveReadSuiteBase]]'s hooks to Avro, adding the streaming-reader regression tests
 * that have no format-agnostic analogue.
 */
trait AvroArchiveReadBase extends ArchiveReadSuiteBase {

  override protected def format: String = "avro"

  override protected def fileExtension: String = "avro"

  // Avro is self-describing; no read options needed.
  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // Avro takes a single file's writer schema for the whole dataset rather than unioning fields
  // across entries, so it opts out of the shared schema-merge tests.
  override protected def supportsSchemaMerge: Boolean = false

  // Avro's header types are fixed; it does not widen types across entries like content inference.
  override protected def excluded: Seq[String] = super.excluded ++ Seq(
    "archive inference widens a column's type across entries like a directory")

  // ----- Avro-specific tests -------------------------------------------------

  test("Avro: a truncated entry fails fast instead of spinning") {
    // A DataFileStream must throw on a truncated entry rather than loop forever. Cut at the header
    // and mid-file; the per-test timeout catches a regression to a spin.
    val full = encodeFile(sampleDf((1, "Alice"), (2, "Bob")))
    Seq(0, 2, DataFileConstants.MAGIC.length, full.length / 2).foreach { len =>
      val ex = intercept[Exception] {
        val stream = new DataFileStream[GenericRecord](
          new ByteArrayInputStream(full.take(len)), new GenericDatumReader[GenericRecord]())
        try while (stream.hasNext) stream.next() finally stream.close()
      }
      assert(ex.isInstanceOf[IOException] || ex.isInstanceOf[RuntimeException],
        s"truncated entry of length $len should fail fast, got $ex")
    }
  }

  test("Avro: a drip-fed entry reads fully under partial reads") {
    // Partial reads (count < requested) must make progress rather than spin. One byte per read
    // still reads every record.
    val full = encodeFile(sampleDf((1, "Alice"), (2, "Bob"), (3, "Carol")))
    val dripFed = new ByteArrayInputStream(full) {
      override def read(b: Array[Byte], off: Int, len: Int): Int =
        super.read(b, off, math.min(1, len))
    }
    val stream = new DataFileStream[GenericRecord](dripFed, new GenericDatumReader[GenericRecord]())
    try {
      var count = 0
      while (stream.hasNext) { stream.next(); count += 1 }
      assert(count == 3, s"expected all 3 records read through 1-byte reads, got $count")
    } finally stream.close()
  }
}

class AvroTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with AvroArchiveReadBase
  with TarArchiveReadBase

class AvroZipArchiveReadSuite
  extends ArchiveReadSuiteBase
  with AvroArchiveReadBase
  with ZipArchiveReadBase

class AvroSevenZArchiveReadSuite
  extends ArchiveReadSuiteBase
  with AvroArchiveReadBase
  with SevenZArchiveReadBase
