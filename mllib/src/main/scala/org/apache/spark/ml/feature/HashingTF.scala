package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{HasInputCol, HasOutputCol, IntParam, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._

class HashingTF extends Transformer with HasInputCol with HasOutputCol {

  def setInputCol(value: String) = { set(inputCol, value); this }
  def setOutputCol(value: String) = { set(outputCol, value); this }

  val numFeatures = new IntParam(this, "numFeatures", "number of features", Some(1 << 18))
  def setNumFeatures(value: Int) = { set(numFeatures, value); this }
  def getNumFeatures: Int = get(numFeatures)

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val hashingTF = new feature.HashingTF(map(numFeatures))
    val t: Iterable[_] => Vector = (doc) => {
      hashingTF.transform(doc)
    }
    dataset.select(Star(None), t.call(map(inputCol).attr) as map(outputCol))
  }
}
