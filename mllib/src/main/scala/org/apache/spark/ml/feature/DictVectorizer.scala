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

package org.apache.spark.ml.feature

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class DictVectorizer(override val uid: String)
	extends Estimator[DictVectorizerModel]
    with HasInputCols with HasOutputCol with DefaultParamsWritable{
	def this() = this(Identifiable.randomUID("dictVec"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)


  /**
    * Fits a model to the input data.
    */
  override def fit(dataset: Dataset[_]): DictVectorizerModel = {
    new DictVectorizerModel("x", Array("xx"))
  }

  override def copy(extra: ParamMap): Estimator[DictVectorizerModel] = defaultCopy(extra)

  /**
    * :: DeveloperApi ::
    *
    * Check transform validity and derive the output schema from the input schema.
    *
    * We check validity for interactions between parameters during `transformSchema` and
    * raise an exception if any parameter value is invalid. Parameter value checks which
    * do not depend on other parameters are handled by `Param.validate()`.
    *
    * Typical implementation should first conduct verification on schema change and parameter
    * validity, including complex parameter interaction checks.
    */
  override def transformSchema(schema: StructType): StructType = {

  }
}

class DictVectorizerModel( val uid: String, val vocabulary: Array[String],
                          val sep: String = "=") {
  def this() = this(Identifiable.randomUID("dictVec"))

}
