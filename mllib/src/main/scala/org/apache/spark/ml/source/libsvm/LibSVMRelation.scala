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

package org.apache.spark.ml.source.libsvm

import com.google.common.base.Objects

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * LibSVMRelation provides the DataFrame constructed from LibSVM format data.
 * @param path File path of LibSVM format
 * @param numFeatures The number of features
 * @param vectorType The type of vector. It can be 'sparse' or 'dense'
 * @param sqlContext The Spark SQLContext
 */
private[libsvm] class LibSVMRelation(val path: String, val numFeatures: Int, val vectorType: String)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging with Serializable {

  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false) ::
      StructField("features", new VectorUDT(), nullable = false) :: Nil
  )

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)
    val sparse = vectorType == "sparse"
    baseRdd.map { pt =>
      val features = if (sparse) pt.features.toSparse else pt.features.toDense
      Row(pt.label, features)
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(path, Double.box(numFeatures), vectorType)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LibSVMRelation =>
      path == that.path &&
        numFeatures == that.numFeatures &&
        vectorType == that.vectorType
    case _ =>
      false
  }
}

/**
 * This is used for creating DataFrame from LibSVM format file.
 * The LibSVM file path must be specified to DefaultSource.
 */
private[spark] class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "libsvm"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
    : BaseRelation = {
    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("'path' must be specified"))
    val numFeatures = parameters.getOrElse("numFeatures", "-1").toInt
    val vectorType = parameters.getOrElse("vectorType", "sparse")
    new LibSVMRelation(path, numFeatures, vectorType)(sqlContext)
  }
}
