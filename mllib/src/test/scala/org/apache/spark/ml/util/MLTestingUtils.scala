package org.apache.spark.ml.util

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap

object MLTestingUtils {
  def checkCopy(model: Model[_]): Unit = {
    val copied = model.copy(ParamMap.empty)
      .asInstanceOf[Model[_]]
    assert(copied.parent == model.parent)
  }
}
