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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * :: Experimental ::
 * Defines the image schema and methods to read and manipulate images.
 */
@Experimental
@Since("2.3.0")
object ImageSchema {

  /**
   * OpenCv type representation
   * @param mode ordinal for the type
   * @param dataType open cv data type
   * @param nChannels number of color channels
   */
  case class OpenCvType(mode: Int, dataType: String, nChannels: Int) {
    def name: String = if (mode == -1) { "Undefined" } else { s"CV_$dataType" + s"C$nChannels" }
    override def toString: String = s"OpenCvType(mode = $mode, name = $name)"
  }

  def ocvTypeByName(name: String): OpenCvType = {
    ocvTypes.find(x => x.name == name).getOrElse(
      throw new IllegalArgumentException("Unknown open cv type " + name))
  }

  def ocvTypeByMode(mode: Int): OpenCvType = {
    ocvTypes.find(x => x.mode == mode).getOrElse(
      throw new IllegalArgumentException("Unknown open cv mode " + mode))
  }

  val undefinedImageType = OpenCvType(-1, "N/A", -1)

  /**
   * A Mapping of Type to Numbers in OpenCV
   *
   *        C1 C2  C3  C4
   * CV_8U   0  8  16  24
   * CV_8S   1  9  17  25
   * CV_16U  2 10  18  26
   * CV_16S  3 11  19  27
   * CV_32S  4 12  20  28
   * CV_32F  5 13  21  29
   * CV_64F  6 14  22  30
   */
  val ocvTypes = {
    val types =
      for (nc <- Array(1, 2, 3, 4);
           dt <- Array("8U", "8S", "16U", "16S", "32S", "32F", "64F"))
        yield (dt, nc)
    val ordinals = for (i <- 0 to 3; j <- 0 to 6) yield ( i * 8 + j)
    undefinedImageType +: (ordinals zip types).map(x => OpenCvType(x._1, x._2._1, x._2._2))
  }

  /**
   *  (Java Specific) list of OpenCv types
   */
  val javaOcvTypes = ocvTypes.asJava

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
    Row(Row(origin, -1, -1, -1, undefinedImageType.mode, Array.ofDim[Byte](0)))

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
      val (nChannels, mode: Int) = if (isGray) {
        (1, ocvTypeByName("CV_8UC1").mode)
      } else if (hasAlpha) {
        (4, ocvTypeByName("CV_8UC4").mode)
      } else {
        (3, ocvTypeByName("CV_8UC3").mode)
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
   * Read the directory of images from the local or remote source
   *
   * @note If multiple jobs are run in parallel with different sampleRatio or recursive flag,
   * there may be a race condition where one job overwrites the hadoop configs of another.
   * @note If sample ratio is less than 1, sampling uses a PathFilter that is efficient but
   * potentially non-deterministic.
   *
   * @param path Path to the image directory
   * @return DataFrame with a single column "image" of images;
   *         see ImageSchema for the details
   */
  def readImages(path: String): DataFrame = readImages(path, null, false, -1, false, 1.0, 0)

  /**
   * Read the directory of images from the local or remote source
   *
   * @note If multiple jobs are run in parallel with different sampleRatio or recursive flag,
   * there may be a race condition where one job overwrites the hadoop configs of another.
   * @note If sample ratio is less than 1, sampling uses a PathFilter that is efficient but
   * potentially non-deterministic.
   *
   * @param path Path to the image directory
   * @param sparkSession Spark Session, if omitted gets or creates the session
   * @param recursive Recursive path search flag
   * @param numPartitions Number of the DataFrame partitions,
   *                      if omitted uses defaultParallelism instead
   * @param dropImageFailures Drop the files that are not valid images from the result
   * @param sampleRatio Fraction of the files loaded
   * @return DataFrame with a single column "image" of images;
   *         see ImageSchema for the details
   */
  def readImages(
      path: String,
      sparkSession: SparkSession,
      recursive: Boolean,
      numPartitions: Int,
      dropImageFailures: Boolean,
      sampleRatio: Double,
      seed: Long): DataFrame = {
    require(sampleRatio <= 1.0 && sampleRatio >= 0, "sampleRatio should be between 0 and 1")

    val session = if (sparkSession != null) sparkSession else SparkSession.builder().getOrCreate
    val partitions =
      if (numPartitions > 0) {
        numPartitions
      } else {
        session.sparkContext.defaultParallelism
      }

    RecursiveFlag.withRecursiveFlag(recursive, session) {
      SamplePathFilter.withPathFilter(sampleRatio, session, seed) {
        val binResult = session.sparkContext.binaryFiles(path, partitions)
        val streams = if (numPartitions == -1) binResult else binResult.repartition(partitions)
        val convert = (origin: String, bytes: PortableDataStream) =>
          decode(origin, bytes.toArray())
        val images = if (dropImageFailures) {
          streams.flatMap { case (origin, bytes) => convert(origin, bytes) }
        } else {
          streams.map { case (origin, bytes) =>
            convert(origin, bytes).getOrElse(invalidImageRow(origin))
          }
        }
        session.createDataFrame(images, imageSchema)
      }
    }
  }
}
