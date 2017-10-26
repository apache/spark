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

package org.apache.spark.ml.image

import java.awt.Color
import java.awt.color.ColorSpace
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

@Experimental
@Since("2.3.0")
object ImageSchema {

  val undefinedImageType = "Undefined"

  val imageFields = Array("origin", "height", "width", "nChannels", "mode", "data")

  val ocvTypes = Map(
    undefinedImageType -> -1,
    "CV_8U" -> 0, "CV_8UC1" -> 0, "CV_8UC3" -> 16, "CV_8UC4" -> 24
  )

  /**
   * Schema for the image column: Row(String, Int, Int, Int, Int, Array[Byte])
   */
  val columnSchema = StructType(
    StructField(imageFields(0), StringType, true) ::
    StructField(imageFields(1), IntegerType, false) ::
    StructField(imageFields(2), IntegerType, false) ::
    StructField(imageFields(3), IntegerType, false) ::
    // OpenCV-compatible type: CV_8UC3 in most cases
    StructField(imageFields(4), IntegerType, false) ::
    // Bytes in OpenCV-compatible order: row-wise BGR in most cases
    StructField(imageFields(5), BinaryType, false) :: Nil)

  /**
   * DataFrame with a single column of images named "image" (nullable)
   */
  val imageSchema = StructType(StructField("image", columnSchema, true) :: Nil)

  /**
   * :: Experimental ::
   * Gets the origin of the image
   *
   * @return The origin of the image
   */
  def getOrigin(row: Row): String = row.getString(0)

  /**
   * :: Experimental ::
   * Gets the height of the image
   *
   * @return The height of the image
   */
  def getHeight(row: Row): Int = row.getInt(1)

  /**
   * :: Experimental ::
   * Gets the width of the image
   *
   * @return The width of the image
   */
  def getWidth(row: Row): Int = row.getInt(2)

  /**
   * :: Experimental ::
   * Gets the number of channels in the image
   *
   * @return The number of channels in the image
   */
  def getNChannels(row: Row): Int = row.getInt(3)

  /**
   * :: Experimental ::
   * Gets the OpenCV representation as an int
   *
   * @return The OpenCV representation as an int
   */
  def getMode(row: Row): Int = row.getInt(4)

  /**
   * :: Experimental ::
   * Gets the image data
   *
   * @return The image data
   */
  def getData(row: Row): Array[Byte] = row.getAs[Array[Byte]](5)

  /**
   * :: Experimental ::
   * Check if the DataFrame column contains images (i.e. has ImageSchema)
   *
   * @param df DataFrame
   * @param column Column name
   * @return True if the given column matches the image schema
   */
  def isImageColumn(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType == columnSchema

  /**
   * Default values for the invalid image
   *
   * @param origin Origin of the invalid image
   * @return Row with the default values
   */
  private def invalidImageRow(origin: String): Row =
    Row(Row(origin, -1, -1, -1, ocvTypes(undefinedImageType), Array.ofDim[Byte](0)))

  /**
   * Convert the compressed image (jpeg, png, etc.) into OpenCV
   * representation and store it in DataFrame Row
   *
   * @param origin Arbitrary string that identifies the image
   * @param bytes Image bytes (for example, jpeg)
   * @return DataFrame Row or None (if the decompression fails)
   */
  private[spark] def decode(origin: String, bytes: Array[Byte]): Option[Row] = {

    val img = ImageIO.read(new ByteArrayInputStream(bytes))

    if (img == null) {
      None
    } else {
      val isGray = img.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
      val hasAlpha = img.getColorModel.hasAlpha

      val height = img.getHeight
      val width = img.getWidth
      val (nChannels, mode) = if (isGray) {
        (1, ocvTypes("CV_8UC1"))
      } else if (hasAlpha) {
        (4, ocvTypes("CV_8UC4"))
      } else {
        (3, ocvTypes("CV_8UC3"))
      }

      val imageSize = height * width * nChannels
      assert(imageSize < 1e9, "image is too large")
      val decoded = Array.ofDim[Byte](imageSize)

      // Grayscale images in Java require special handling to get the correct intensity
      if (isGray) {
        var offset = 0
        val raster = img.getRaster
        for (h <- 0 until height) {
          for (w <- 0 until width) {
            decoded(offset) = raster.getSample(w, h, 0).toByte
            offset += 1
          }
        }
      } else {
        var offset = 0
        for (h <- 0 until height) {
          for (w <- 0 until width) {
            val color = new Color(img.getRGB(w, h))

            decoded(offset) = color.getBlue.toByte
            decoded(offset + 1) = color.getGreen.toByte
            decoded(offset + 2) = color.getRed.toByte
            if (nChannels == 4) {
              decoded(offset + 3) = color.getAlpha.toByte
            }
            offset += nChannels
          }
        }
      }

      // the internal "Row" is needed, because the image is a single DataFrame column
      Some(Row(Row(origin, height, width, nChannels, mode, decoded)))
    }
  }

  /**
   * :: Experimental ::
   * Read the directory of images from the local or remote source
   *
   * @param path Path to the image directory
   * @param sparkSession Spark Session
   * @param recursive Recursive path search flag
   * @param numPartitions Number of the DataFrame partitions
   * @param dropImageFailures Drop the files that are not valid images from the result
   * @param sampleRatio Fraction of the files loaded
   * @return DataFrame with a single column "image" of images;
   *         see ImageSchema for the details
   */
  def readImages(
      path: String,
      sparkSession: SparkSession = null,
      recursive: Boolean = false,
      numPartitions: Int = 0,
      dropImageFailures: Boolean = false,
      sampleRatio: Double = 1.0): DataFrame = {
    require(sampleRatio <= 1.0 && sampleRatio >= 0, "sampleRatio should be between 0 and 1")

    val session = if (sparkSession != null) sparkSession else SparkSession.builder().getOrCreate
    val partitions =
      if (numPartitions > 0) {
        numPartitions
      } else {
        session.sparkContext.defaultParallelism
      }

    val oldRecursiveFlag = RecursiveFlag.setRecursiveFlag(Some(recursive.toString), session)
    val sampleImages = sampleRatio < 1
    val oldPathFilter: Option[Class[_]] =
      if (sampleImages) {
        SamplePathFilter.setPathFilter(Some(classOf[SamplePathFilter]), sampleRatio, session)
      } else {
        None
      }

    try {
      val streams = session.sparkContext.binaryFiles(path, partitions)
        .repartition(partitions)

      val convert = (stream: (String, PortableDataStream)) =>
        decode(stream._1, stream._2.toArray())
      val images = if (dropImageFailures) {
        streams.flatMap { convert(_) }
      } else {
        streams.map { stream => convert(stream).getOrElse(invalidImageRow(stream._1)) }
      }
      session.createDataFrame(images, imageSchema)
    } finally {
      // return Hadoop flags to the original values
      RecursiveFlag.setRecursiveFlag(oldRecursiveFlag, session)
      if (sampleImages) {
        SamplePathFilter.unsetPathFilter(oldPathFilter, session)
      }
    }
  }
}
