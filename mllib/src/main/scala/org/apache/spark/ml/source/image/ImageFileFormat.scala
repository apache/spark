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

import java.awt.Color
import java.awt.color.ColorSpace
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import scala.collection.JavaConverters._

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

private[image] class ImageFileFormat extends FileFormat with DataSourceRegister {
  import ImageFileFormat._

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(schema)

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for image data source")
  }

  override def shortName(): String = "image"

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Image data source only produces a single data column named \"image\".")

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val imageSourceOptions = new ImageOptions(options)

    (file: PartitionedFile) => {
      val emptyUnsafeRow = new UnsafeRow(0)
      if (!imageSourceOptions.dropInvalid && requiredSchema.isEmpty) {
        Iterator(emptyUnsafeRow)
      } else {
        val origin = file.filePath
        val path = new Path(origin)
        val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
        val stream = fs.open(path)
        val bytes = try {
          ByteStreams.toByteArray(stream)
        } finally {
          Closeables.close(stream, true)
        }
        val resultOpt = decode(origin, bytes)
        val filteredResult = if (imageSourceOptions.dropInvalid) {
          resultOpt.toIterator
        } else {
          Iterator(resultOpt.getOrElse(invalidImageRow(origin)))
        }

        if (requiredSchema.isEmpty) {
          filteredResult.map(_ => emptyUnsafeRow)
        } else {
          val converter = RowEncoder(requiredSchema)
          filteredResult.map(row => converter.toRow(row))
        }
      }
    }
  }
}

object ImageFileFormat {

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
    StructField("height", IntegerType, true) ::
    StructField("width", IntegerType, true) ::
    StructField("nChannels", IntegerType, true) ::
    // OpenCV-compatible type: CV_8UC3 in most cases
    StructField("mode", IntegerType, true) ::
    // Bytes in OpenCV-compatible order: row-wise BGR in most cases
    StructField("data", BinaryType, true) :: Nil)

  val imageFields: Array[String] = columnSchema.fieldNames

  /**
   * DataFrame with a single column of images named "image" (nullable)
   */
  val schema = StructType(StructField("image", columnSchema, true) :: Nil)

  /**
   * Default values for the invalid image
   *
   * @param origin Origin of the invalid image
   * @return Row with the default values
   */
  private[image] def invalidImageRow(origin: String): Row =
    Row(Row(origin, -1, -1, -1, ocvTypes(undefinedImageType),
      Array.ofDim[Byte](0)))

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
   * Convert the compressed image (jpeg, png, etc.) into OpenCV
   * representation and store it in DataFrame Row
   *
   * @param origin Arbitrary string that identifies the image
   * @param bytes Image bytes (for example, jpeg)
   * @return DataFrame Row or None (if the decompression fails)
   */
  def decode(origin: String, bytes: Array[Byte]): Option[Row] = {

    val img = try {
      ImageIO.read(new ByteArrayInputStream(bytes))
    } catch {
      // Catch runtime exception because `ImageIO` may throw unexcepted `RuntimeException`.
      // But do not catch the declared `IOException` (regarded as FileSystem failure)
      case _: RuntimeException => null
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
