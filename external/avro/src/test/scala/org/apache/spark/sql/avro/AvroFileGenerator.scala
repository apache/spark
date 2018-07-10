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

package org.apache.spark.sql.avro

import java.io.File

import scala.util.Random

import org.apache.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic._
import org.apache.commons.io.FileUtils

// scalastyle:off println

/**
 * This object allows you to generate large avro files that can be used for speed benchmarking.
 * See README on how to use it.
 */
object AvroFileGenerator {

  val defaultNumberOfRecords = 1000000
  val defaultNumberOfFiles = 1
  val outputDir = "target/avroForBenchmark/"
  val schemaPath = "src/test/resources/benchmarkSchema.avsc"
  val objectSize = 100 // Maps, arrays and strings in our generated file have this size

  private[avro] def generateAvroFile(numberOfRecords: Int, fileIdx: Int) = {
    val schema = new Schema.Parser().parse(new File(schemaPath))
    val outputFile = new File(outputDir + "part" + fileIdx + ".avro")
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, outputFile)

    // Create data that we will put into the avro file
    val avroRec = new GenericData.Record(schema)
    val innerRec = new GenericData.Record(schema.getField("inner_record").schema())
    innerRec.put("value_field", "Inner string")
    val rand = new Random()

    for (idx <- 0 until numberOfRecords) {
      avroRec.put("string", rand.nextString(objectSize))
      avroRec.put("simple_map", TestUtils.generateRandomMap(rand, objectSize))
      avroRec.put("union_int_long_null", rand.nextInt())
      avroRec.put("union_float_double", rand.nextDouble())
      avroRec.put("inner_record", innerRec)
      avroRec.put("array_of_boolean", TestUtils.generateRandomArray(rand, objectSize))
      avroRec.put("bytes", TestUtils.generateRandomByteBuffer(rand, objectSize))

      dataFileWriter.append(avroRec)
    }

    dataFileWriter.close()
  }

  def main(args: Array[String]) {
    var numberOfRecords = defaultNumberOfRecords
    var numberOfFiles = defaultNumberOfFiles

    if (args.size > 0) {
      numberOfRecords = args(0).toInt
    }

    if (args.size > 1) {
      numberOfFiles = args(1).toInt
    }

    println(s"Generating $numberOfFiles avro files with $numberOfRecords records each")

    FileUtils.deleteDirectory(new File(outputDir))
    new File(outputDir).mkdirs() // Create directory for output files

    for (fileIdx <- 0 until numberOfFiles) {
      generateAvroFile(numberOfRecords, fileIdx)
    }

    println("Generation finished")
  }
}
// scalastyle:on println
