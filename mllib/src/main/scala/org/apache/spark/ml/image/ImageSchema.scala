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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Since
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Defines the image schema and methods to read and manipulate images.
 */
@Since("2.3.0")
object ImageSchema {

  val undefinedImageType = "Undefined"

  /**
   * (Scala-specific) OpenCV type mapping supported
   */
  val ocvTypes: Map[String, Int] = Map(
    undefinedImageType -> -1,
    "CV_8U" -> 0, "CV_8UC1" -> 0, "CV_8UC3" -> 16, "CV_8UC4" -> 24
  )

  /**
   * (Java-specific) OpenCV type mapping supported
   */
  val javaOcvTypes: java.util.Map[String, Int] = ocvTypes.asJava

  /**
   * Schema for the image column: Row(String, Int, Int, Int, Int, Array[Byte])
   */
  val columnSchema = StructType(
    StructField("origin", StringType, true) ::
    StructField("height", IntegerType, false) ::
    StructField("width", IntegerType, false) ::
    StructField("nChannels", IntegerType, false) ::
    // OpenCV-compatible type: CV_8UC3 in most cases
    StructField("mode", IntegerType, false) ::
    // Bytes in OpenCV-compatible order: row-wise BGR in most cases
    StructField("data", BinaryType, false) :: Nil)

  val imageFields: Array[String] = columnSchema.fieldNames

  /**
   * DataFrame with a single column of images named "image" (nullable)
   */
  val imageSchema = StructType(StructField("image", columnSchema, true) :: Nil)

  /**
   * Gets the origin of the image
   *
   * @return The origin of the image
   */
  def getOrigin(row: Row): String = row.getString(0)

  /**
   * Gets the height of the image
   *
   * @return The height of the image
   */
  def getHeight(row: Row): Int = row.getInt(1)

  /**
   * Gets the width of the image
   *
   * @return The width of the image
   */
  def getWidth(row: Row): Int = row.getInt(2)

  /**
   * Gets the number of channels in the image
   *
   * @return The number of channels in the image
   */
  def getNChannels(row: Row): Int = row.getInt(3)

  /**
   * Gets the OpenCV representation as an int
   *
   * @return The OpenCV representation as an int
   */
  def getMode(row: Row): Int = row.getInt(4)

  /**
   * Gets the image data
   *
   * @return The image data
   */
  def getData(row: Row): Array[Byte] = row.getAs[Array[Byte]](5)

  /**
   * Default values for the invalid image
   *
   * @param origin Origin of the invalid image
   * @return Row with the default values
   */
  private[spark] def invalidImageRow(origin: String): Row =
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

    val img = try {
      ImageIO.read(new ByteArrayInputStream(bytes))
    } catch {
      // Note that:
      // - At this point, the files are already read from the files as bytes. Therefore,
      //   no real I/O exceptions are expected.
      // - `ImageIO.read` can throw `javax.imageio.IIOException` that is technically
      //   a runtime exception but it inherits IOException.
      case _: Throwable => null
    }

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
            val color = new Color(img.getRGB(w, h), hasAlpha)
            decoded(offset) = color.getBlue.toByte
            decoded(offset + 1) = color.getGreen.toByte
            decoded(offset + 2) = color.getRed.toByte
            if (hasAlpha) {
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
}
