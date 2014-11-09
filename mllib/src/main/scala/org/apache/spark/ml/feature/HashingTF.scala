package org.apache.spark.ml.feature

import org.apache.spark.ml.SimpleTransformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.Vector

class HashingTF extends SimpleTransformer[Iterable[_], Vector, HashingTF] {

  val numFeatures = new IntParam(this, "numFeatures", "number of features", Some(1 << 18))
  def setNumFeatures(value: Int) = { set(numFeatures, value); this }
  def getNumFeatures: Int = get(numFeatures)

  override protected def createTransformFunc(paramMap: ParamMap): Iterable[_] => Vector = {
    val hashingTF = new feature.HashingTF(paramMap(numFeatures))
    hashingTF.transform
  }
}
