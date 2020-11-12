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

package org.apache.spark.ml.source.image

import java.net.URI
import java.nio.file.Paths

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.image.ImageSchema._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, substring_index}

class ImageFileFormatSuite extends SparkFunSuite with MLlibTestSparkContext {

  // Single column of images named "image"
  private lazy val imagePath = "../data/mllib/images/partitioned"
  private lazy val recursiveImagePath = "../data/mllib/images"

  test("Smoke test: create basic ImageSchema dataframe") {
    val origin = "path"
    val width = 1
    val height = 1
    val nChannels = 3
    val data = Array[Byte](0, 0, 0)
    val mode = ocvTypes("CV_8UC3")

    // Internal Row corresponds to image StructType
    val rows = Seq(Row(Row(origin, height, width, nChannels, mode, data)),
      Row(Row(null, height, width, nChannels, mode, data)))
    val rdd = sc.makeRDD(rows)
    val df = spark.createDataFrame(rdd, imageSchema)

    assert(df.count === 2, "incorrect image count")
    assert(df.schema("image").dataType == columnSchema, "data do not fit ImageSchema")
  }

  test("image datasource count test") {
    val df1 = spark.read.format("image").load(imagePath)
    assert(df1.count === 9)

    val df2 = spark.read.format("image").option("dropInvalid", true).load(imagePath)
    assert(df2.count === 8)
  }

  test("image datasource test: read jpg image") {
    val df = spark.read.format("image").load(imagePath + "/cls=kittens/date=2018-02/DP153539.jpg")
    assert(df.count() === 1)
  }

  test("image datasource test: read png image") {
    val df = spark.read.format("image").load(imagePath + "/cls=multichannel/date=2018-01/BGRA.png")
    assert(df.count() === 1)
  }

  test("image datasource test: read non image") {
    val filePath = imagePath + "/cls=kittens/date=2018-01/not-image.txt"
    val df = spark.read.format("image").option("dropInvalid", true)
      .load(filePath)
    assert(df.count() === 0)

    val df2 = spark.read.format("image").option("dropInvalid", false)
      .load(filePath)
    assert(df2.count() === 1)
    val result = df2.head()

    val resultOrigin = result.getStruct(0).getString(0)
    // covert `origin` to `java.net.URI` object and then compare.
    // because `file:/path` and `file:///path` are both valid URI-ifications
    assert(new URI(resultOrigin) === Paths.get(filePath).toAbsolutePath().normalize().toUri())

    // Compare other columns in the row to be the same with the `invalidImageRow`
    assert(result === invalidImageRow(resultOrigin))
  }

  test("image datasource partition test") {
    val result = spark.read.format("image")
      .option("dropInvalid", true).load(imagePath)
      .select(substring_index(col("image.origin"), "/", -1).as("origin"), col("cls"), col("date"))
      .collect()

    assert(Set(result: _*) === Set(
      Row("29.5.a_b_EGDP022204.jpg", "kittens", "2018-01"),
      Row("54893.jpg", "kittens", "2018-02"),
      Row("DP153539.jpg", "kittens", "2018-02"),
      Row("DP802813.jpg", "kittens", "2018-02"),
      Row("BGRA.png", "multichannel", "2018-01"),
      Row("BGRA_alpha_60.png", "multichannel", "2018-01"),
      Row("chr30.4.184.jpg", "multichannel", "2018-02"),
      Row("grayscale.jpg", "multichannel", "2018-02")
    ))
  }

  // Images with the different number of channels
  test("readImages pixel values test") {
    val images = spark.read.format("image").option("dropInvalid", true)
      .load(imagePath + "/cls=multichannel/").collect()

    val firstBytes20Set = images.map { rrow =>
      val row = rrow.getAs[Row]("image")
      val filename = Paths.get(getOrigin(row)).getFileName().toString()
      val mode = getMode(row)
      val bytes20 = getData(row).slice(0, 20).toList
      filename -> Tuple2(mode, bytes20) // Cannot remove `Tuple2`, otherwise `->` operator
                                        // will match 2 arguments
    }.toSet

    assert(firstBytes20Set === expectedFirstBytes20Set)
  }

  // number of channels and first 20 bytes of OpenCV representation
  // - default representation for 3-channel RGB images is BGR row-wise:
  //   (B00, G00, R00,      B10, G10, R10,      ...)
  // - default representation for 4-channel RGB images is BGRA row-wise:
  //   (B00, G00, R00, A00, B10, G10, R10, A10, ...)
  private val expectedFirstBytes20Set = Set(
    "grayscale.jpg" ->
      ((0, List[Byte](-2, -33, -61, -60, -59, -59, -64, -59, -66, -67, -73, -73, -62,
        -57, -60, -63, -53, -49, -55, -69))),
    "chr30.4.184.jpg" -> ((16,
      List[Byte](-9, -3, -1, -43, -32, -28, -75, -60, -57, -78, -59, -56, -74, -59, -57,
        -71, -58, -56, -73, -64))),
    "BGRA.png" -> ((24,
      List[Byte](-128, -128, -8, -1, -128, -128, -8, -1, -128,
        -128, -8, -1, 127, 127, -9, -1, 127, 127, -9, -1))),
    "BGRA_alpha_60.png" -> ((24,
      List[Byte](-128, -128, -8, 60, -128, -128, -8, 60, -128,
        -128, -8, 60, 127, 127, -9, 60, 127, 127, -9, 60)))
  )
}
