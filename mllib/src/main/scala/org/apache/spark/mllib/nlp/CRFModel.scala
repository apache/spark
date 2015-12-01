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
package org.apache.spark.mllib.nlp

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, StructType, StructField}
import org.json4s.{DefaultFormats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.{Row, SQLContext}

@Since("1.6.0")
class CRFModel(val CRFSeries: RDD[String])
  extends Saveable with Serializable with PMMLExportable {
  override def save(sc: SparkContext, path: String): Unit = {
    CRFModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}


object CRFModel extends Loader[CRFModel] {
  override def load(sc: SparkContext, path: String): CRFModel = {
    CRFModel.SaveLoadV1_0.load(sc, path)
  }

  private[CRFModel] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[CRFModel]
    val thisClassName = "CRFModel"

    def save(sc: SparkContext, model: CRFModel, path: String): Unit = {
      val sqlContext = new SQLContext(sc)
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))

      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))
      val itemType = ScalaReflection.schemaFor[Double].dataType
      val fields = Array(StructField("", ArrayType(itemType)))
      val schema = StructType(fields)
      val dataRDD = model.CRFSeries.map { x => Row(x) }
      sqlContext.createDataFrame(dataRDD, schema).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): CRFModel = {
      implicit val formats = DefaultFormats
      val sqlContext = new SQLContext(sc)
      val (className, formatVersion, _) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val crfSeries = sqlContext.read.parquet(Loader.dataPath(path))
      val dataRDD = crfSeries.map { x => x.toString() }
      new CRFModel(dataRDD)
    }
  }

}
