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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, PrunedScan, BaseRelation, RelationProvider}


class LibSVMRelation(val path: String, val numFeatures: Int, val featuresType: String)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with Logging {

  private final val vectorType: DataType
    = classOf[Vector].getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance()


  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false) ::
      StructField("features", vectorType, nullable = false) :: Nil
  )

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)

    val rowBuilders = requiredColumns.map {
      case "label" => (pt: LabeledPoint) => Seq(pt.label)
      case "features" if featuresType == "sparse" => (pt: LabeledPoint) => Seq(pt.features.toSparse)
      case "features" if featuresType == "dense" => (pt: LabeledPoint) => Seq(pt.features.toDense)
    }

    baseRdd.map(pt => {
      Row.fromSeq(rowBuilders.map(_(pt)).reduceOption(_ ++ _).getOrElse(Seq.empty))
    })
  }

  override def hashCode(): Int = {
    Objects.hashCode(path, schema)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LibSVMRelation => (this.path == that.path) && this.schema.equals(that.schema)
    case _ => false
  }

}

class DefaultSource extends RelationProvider with DataSourceRegister {

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def format(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "libsvm"

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified"))
  }

  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):
       BaseRelation = {
    val path = checkPath(parameters)
    val numFeatures = parameters.getOrElse("numFeatures", "-1").toInt
    /**
     * featuresType can be selected "dense" or "sparse".
     * This parameter decides the type of returned feature vector.
     */
    val featuresType = parameters.getOrElse("featuresType", "sparse")
    new LibSVMRelation(path, numFeatures, featuresType)(sqlContext)
  }
}
