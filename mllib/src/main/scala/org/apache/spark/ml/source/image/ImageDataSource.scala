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

/**
 * `image` package implements Spark SQL data source API for loading image data as `DataFrame`.
 * The loaded `DataFrame` has one `StructType` column: `image`.
 * The schema of the `image` column is:
 *  - origin: String (represents the file path of the image)
 *  - height: Int (height of the image)
 *  - width: Int (width of the image)
 *  - nChannels: Int (number of the image channels)
 *  - mode: Int (OpenCV-compatible type)
 *  - data: BinaryType (Image bytes in OpenCV-compatible order: row-wise BGR in most cases)
 *
 * To use image data source, you need to set "image" as the format in `DataFrameReader` and
 * optionally specify the data source options, for example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("image")
 *     .option("dropInvalid", true)
 *     .load("data/mllib/images/partitioned")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("image")
 *     .option("dropInvalid", true)
 *     .load("data/mllib/images/partitioned");
 * }}}
 *
 * Image data source supports the following options:
 *  - "dropInvalid": Whether to drop the files that are not valid images from the result.
 *
 * @note This IMAGE data source does not support saving images to files.
 *
 * @note This class is public for documentation purpose. Please don't use this class directly.
 * Rather, use the data source API as illustrated above.
 */
class ImageDataSource private() {}
