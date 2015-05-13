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

package org.apache.spark.sql.hive.orc

import java.io.File

import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

case class TestRDDEntry(key: Int, value: String)

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    booleanField: Option[Boolean])

case class Nested(i: Int, s: String)

case class Data(array: Seq[Int], nested: Nested)

case class AllDataTypes(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean)

case class AllDataTypesWithNonPrimitiveType(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapValueContainsNull: Map[Int, Option[Long]],
    data: Data)

case class BinaryData(binaryData: Array[Byte])

case class Contact(name: String, phone: String)

case class Person(name: String, age: Int, contacts: Seq[Contact])

class OrcQuerySuite extends QueryTest with FunSuiteLike with BeforeAndAfterAll {

  def getTempFilePath(prefix: String, suffix: String = ""): File = {
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.delete()
    tempFile
  }

  test("Read/Write All Types") {
      val tempDir = getTempFilePath("orcTest").getCanonicalPath
      val range = (0 to 255)
      val data = sparkContext.parallelize(range)
        .map(x =>
        AllDataTypes(s"$x", x, x.toLong, x.toFloat,x.toDouble, x.toShort, x.toByte, x % 2 == 0))
      data.toDF().saveAsOrcFile(tempDir)
      checkAnswer(
        TestHive.orcFile(tempDir),
        data.toDF().collect().toSeq)
      Utils.deleteRecursively(new File(tempDir))
    }

    test("read/write binary data") {
      val tempDir = getTempFilePath("orcTest").getCanonicalPath
      sparkContext.parallelize(BinaryData("test".getBytes("utf8")) :: Nil)
        .toDF().saveAsOrcFile(tempDir)
      TestHive.orcFile(tempDir)
        .map(r => new String(r(0).asInstanceOf[Array[Byte]], "utf8"))
        .collect().toSeq == Seq("test")
      Utils.deleteRecursively(new File(tempDir))
    }

    test("Read/Write All Types with non-primitive type") {
      val tempDir = getTempFilePath("orcTest").getCanonicalPath
      val range = (0 to 255)
      val data = sparkContext.parallelize(range)
        .map(x => AllDataTypesWithNonPrimitiveType(
        s"$x", x, x.toLong, x.toFloat, x.toDouble, x.toShort, x.toByte, x % 2 == 0,
        (0 until x),
        (0 until x).map(Option(_).filter(_ % 3 == 0)),
        (0 until x).map(i => i -> i.toLong).toMap,
        (0 until x).map(i => i -> Option(i.toLong)).toMap + (x -> None),
        Data((0 until x), Nested(x, s"$x"))))
      data.toDF().saveAsOrcFile(tempDir)

      checkAnswer(
        TestHive.orcFile(tempDir),
        data.toDF().collect().toSeq)
      Utils.deleteRecursively(new File(tempDir))
    }

    test("Creating case class RDD table") {
      sparkContext.parallelize((1 to 100))
        .map(i => TestRDDEntry(i, s"val_$i"))
        .toDF().registerTempTable("tmp")
      val rdd = sql("SELECT * FROM tmp").collect().sortBy(_.getInt(0))
      var counter = 1
      rdd.foreach {
        // '===' does not like string comparison?
        row: Row => {
          assert(row.getString(1).equals(s"val_$counter"),
            s"row $counter value ${row.getString(1)} does not match val_$counter")
          counter = counter + 1
        }
      }
    }

    test("Simple selection form orc table") {
      val tempDir = getTempFilePath("orcTest").getCanonicalPath
      val data = sparkContext.parallelize((1 to 10))
        .map(i => Person(s"name_$i", i,  (0 until 2).map{ m=>
        Contact(s"contact_$m", s"phone_$m") }))
      data.toDF().saveAsOrcFile(tempDir)
      val f = TestHive.orcFile(tempDir)
      f.registerTempTable("tmp")
      var rdd = sql("SELECT name FROM tmp where age <= 5")
      assert(rdd.count() == 5)

      rdd = sql("SELECT name, contacts FROM tmp where age > 5")
      assert(rdd.count() == 5)
      val contacts = rdd.flatMap(t=>t(1).asInstanceOf[Seq[_]])
      assert(contacts.count() == 10)
      Utils.deleteRecursively(new File(tempDir))
    }

    test("save and load case class RDD with Nones as orc") {
      val data = OptionalReflectData(None, None, None, None, None)
      val rdd = sparkContext.parallelize(data :: Nil)
      val tempDir = getTempFilePath("orcTest").getCanonicalPath
      rdd.toDF().saveAsOrcFile(tempDir)
      val readFile = TestHive.orcFile(tempDir)
      val rdd_saved = readFile.collect()
      assert(rdd_saved(0).toSeq === Seq.fill(5)(null))
      Utils.deleteRecursively(new File(tempDir))
    }

    //  We only support zlib in hive0.12.0 now
    test("Default Compression options for writing to an Orcfile") {
      // TODO: support other compress codec
      var tempDir = getTempFilePath("orcTest").getCanonicalPath
      val rdd = sparkContext.parallelize((1 to 100))
        .map(i => TestRDDEntry(i, s"val_$i"))
      rdd.toDF().saveAsOrcFile(tempDir)
      var actualCodec = OrcFileOperator.getFileReader(tempDir).getCompression
      assert(actualCodec == CompressionKind.ZLIB)
      Utils.deleteRecursively(new File(tempDir))
    }

    // Following codec is supported in hive-0.13.1, ignore it now
    ignore("Other Compression options for writing to an Orcfile - 0.13.1 and above") {
      TestHive.sparkContext.hadoopConfiguration.set(orcDefaultCompressVar, "SNAPPY")
      var tempDir = getTempFilePath("orcTest").getCanonicalPath
      val rdd = sparkContext.parallelize((1 to 100))
        .map(i => TestRDDEntry(i, s"val_$i"))
      rdd.toDF().saveAsOrcFile(tempDir)
      var actualCodec = OrcFileOperator.getFileReader(tempDir).getCompression
      assert(actualCodec == CompressionKind.SNAPPY)
      Utils.deleteRecursively(new File(tempDir))

      TestHive.sparkContext.hadoopConfiguration.set(orcDefaultCompressVar, "NONE")
      tempDir = getTempFilePath("orcTest").getCanonicalPath
      rdd.toDF().saveAsOrcFile(tempDir)
      actualCodec = OrcFileOperator.getFileReader(tempDir).getCompression
      assert(actualCodec == CompressionKind.NONE)
      Utils.deleteRecursively(new File(tempDir))

      TestHive.sparkContext.hadoopConfiguration.set(orcDefaultCompressVar, "LZO")
      tempDir = getTempFilePath("orcTest").getCanonicalPath
      rdd.toDF().saveAsOrcFile(tempDir)
      actualCodec = OrcFileOperator.getFileReader(tempDir).getCompression
      assert(actualCodec == CompressionKind.LZO)
      Utils.deleteRecursively(new File(tempDir))
    }
}
