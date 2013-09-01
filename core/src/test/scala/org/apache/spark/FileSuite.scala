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

package org.apache.spark

import java.io.{FileWriter, PrintWriter, File}

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec, GzipCodec}


import SparkContext._

class FileSuite extends FunSuite with LocalSparkContext {

  test("text files") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsTextFile(outputDir)
    // Read the plain text file and check it's OK
    val outputFile = new File(outputDir, "part-00000")
    val content = Source.fromFile(outputFile).mkString
    assert(content === "1\n2\n3\n4\n")
    // Also try reading it in as a text file RDD
    assert(sc.textFile(outputDir).collect().toList === List("1", "2", "3", "4"))
  }

  test("text files (compressed)") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val normalDir = new File(tempDir, "output_normal").getAbsolutePath
    val compressedOutputDir = new File(tempDir, "output_compressed").getAbsolutePath
    val codec = new DefaultCodec()

    val data = sc.parallelize("a" * 10000, 1)
    data.saveAsTextFile(normalDir)
    data.saveAsTextFile(compressedOutputDir, classOf[DefaultCodec])

    val normalFile = new File(normalDir, "part-00000")
    val normalContent = sc.textFile(normalDir).collect
    assert(normalContent === Array.fill(10000)("a"))

    val compressedFile = new File(compressedOutputDir, "part-00000" + codec.getDefaultExtension)
    val compressedContent = sc.textFile(compressedOutputDir).collect
    assert(compressedContent === Array.fill(10000)("a"))

    assert(compressedFile.length < normalFile.length)
  }

  test("SequenceFiles") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile (compressed)") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val normalDir = new File(tempDir, "output_normal").getAbsolutePath
    val compressedOutputDir = new File(tempDir, "output_compressed").getAbsolutePath
    val codec = new DefaultCodec()

    val data = sc.parallelize(Seq.fill(100)("abc"), 1).map(x => (x, x))
    data.saveAsSequenceFile(normalDir)
    data.saveAsSequenceFile(compressedOutputDir, Some(classOf[DefaultCodec]))

    val normalFile = new File(normalDir, "part-00000")
    val normalContent = sc.sequenceFile[String, String](normalDir).collect
    assert(normalContent === Array.fill(100)("abc", "abc"))

    val compressedFile = new File(compressedOutputDir, "part-00000" + codec.getDefaultExtension)
    val compressedContent = sc.sequenceFile[String, String](compressedOutputDir).collect
    assert(compressedContent === Array.fill(100)("abc", "abc"))

    assert(compressedFile.length < normalFile.length)
  }

  test("SequenceFile with writable key") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), "a" * x)) 
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile with writable value") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile with writable key and value") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("implicit conversions in reading SequenceFiles") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Similar to the tests above, we read a SequenceFile, but this time we pass type params
    // that are convertable to Writable instead of calling sequenceFile[IntWritable, Text]
    val output1 = sc.sequenceFile[Int, String](outputDir)
    assert(output1.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
    // Also try having one type be a subclass of Writable and one not
    val output2 = sc.sequenceFile[Int, Text](outputDir)
    assert(output2.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    val output3 = sc.sequenceFile[IntWritable, String](outputDir)
    assert(output3.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("object files of ints") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    val output = sc.objectFile[Int](outputDir)
    assert(output.collect().toList === List(1, 2, 3, 4))
  }

  test("object files of complex types") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x))
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    val output = sc.objectFile[(Int, String)](outputDir)
    assert(output.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
  }

  test("write SequenceFile using new Hadoop API") {
    import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[IntWritable, Text]](
        outputDir)
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("read SequenceFile using new Hadoop API") {
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    val output =
        sc.newAPIHadoopFile[IntWritable, Text, SequenceFileInputFormat[IntWritable, Text]](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("file caching") {
    sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val out = new FileWriter(tempDir + "/input")
    out.write("Hello world!\n")
    out.write("What's up?\n")
    out.write("Goodbye\n")
    out.close()
    val rdd = sc.textFile(tempDir + "/input").cache()
    assert(rdd.count() === 3)
    assert(rdd.count() === 3)
    assert(rdd.count() === 3)
  }
}
