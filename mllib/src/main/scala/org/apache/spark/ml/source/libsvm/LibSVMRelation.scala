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
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._

/**
 * LibSVMRelation provides the DataFrame constructed from LibSVM format data.
 * @param path File path of LibSVM format
 * @param numFeatures The number of features
 * @param vectorType The type of vector. It can be 'sparse' or 'dense'
 * @param sqlContext The Spark SQLContext
 */
private[ml] class LibSVMRelation(val path: String, val numFeatures: Int, val vectorType: String)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging with Serializable {

  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false) ::
      StructField("features", new VectorUDT(), nullable = false) :: Nil
  )

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)

    baseRdd.map { pt =>
      val features = if (vectorType == "dense") pt.features.toDense else pt.features.toSparse
      Row(pt.label, features)
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(path, schema)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LibSVMRelation => (this.path == that.path) && this.schema.equals(that.schema)
    case _ => false
  }

}

/**
 * This is used for creating DataFrame from LibSVM format file.
 * The LibSVM file path must be specified to DefaultSource.
 */
@Since("1.6.0")
class DefaultSource extends RelationProvider with DataSourceRegister {

  @Since("1.6.0")
  override def shortName(): String = "libsvm"

  private def checkPath(parameters: Map[String, String]): String = {
    require(parameters.contains("path"), "'path' must be specified")
    parameters.get("path").get
  }

  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
    : BaseRelation = {
    val path = checkPath(parameters)
    val numFeatures = parameters.getOrElse("numFeatures", "-1").toInt
    /**
     * featuresType can be selected "dense" or "sparse".
     * This parameter decides the type of returned feature vector.
     */
    val vectorType = parameters.getOrElse("vectorType", "sparse")
    new LibSVMRelation(path, numFeatures, vectorType)(sqlContext)
  }
}
