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

package org.apache.spark.mllib.pmml.export

// Scala Array is conflict with Array imported in PMML.
import scala.{Array => SArray}

import org.dmg.pmml._

import org.apache.spark.mllib.classification.{NaiveBayesModel => SNaiveBayesModel}

/**
 * PMML Model Export for Naive Bayes abstract class
 */
private[mllib] class NaiveBayesPMMLModelExport(model: SNaiveBayesModel, description: String)
  extends PMMLModelExport {

  populateNaiveBayesPMML(model)

  /**
   * Export the input Naive Bayes model to PMML format.
   */
  private def populateNaiveBayesPMML(model: SNaiveBayesModel): Unit = {
    pmml.getHeader.setDescription(description)

    val nbModel = new NaiveBayesModel()

    nbModel.setAlgorithmName(model.modelType)
    nbModel.setFunctionName(MiningFunctionType.CLASSIFICATION)
    nbModel.setModelName(description)

    val fields = new SArray[FieldName](model.theta(0).length)
    val dataDictionary = new DataDictionary()
    val miningSchema = new MiningSchema()
    val bayesInputs = new BayesInputs()
    val bayesOutput = new BayesOutput()

    val labelIndices = model.pi.indices
    val featureIndices = model.theta.head.indices

    // add Bayes input
    for (i <- featureIndices) {
      fields(i) = FieldName.create("field_" + i)
      dataDictionary.withDataFields(new DataField(fields(i), OpType.CATEGORICAL, DataType.DOUBLE)
        .withValues(SArray(new Value().withValue(i.toDouble.toString)): _*))
      miningSchema.withMiningFields(new MiningField(fields(i)).withUsageType(FieldUsageType.ACTIVE))

      val pairs = labelIndices.map { label =>
        new TargetValueCount().withValue(label.toDouble.toString).withCount(model.theta(label)(i))
      }

      val bayesInput = new BayesInput()
      val pairCounts = new PairCounts()
        .withTargetValueCounts(new TargetValueCounts().withTargetValueCounts(pairs: _*))
        .withValue(i.toDouble.toString)
      bayesInput.withFieldName(fields(i)).withPairCounts(pairCounts)
      bayesInputs.withBayesInputs(bayesInput)
    }

    // add Bayes output
    val targetValueCounts = model.pi.zipWithIndex.map { case (x, i) =>
      new TargetValueCount().withValue(i.toDouble.toString).withCount(x) }
    bayesOutput
      .withTargetValueCounts(new TargetValueCounts().withTargetValueCounts(targetValueCounts: _*))

    // add target field
    val targetField = FieldName.create("class")
    dataDictionary.withDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.DOUBLE)
      .withValues(labelIndices.map { x => new Value().withValue(x.toDouble.toString)}: _*))
    miningSchema.withMiningFields(new MiningField(targetField).withUsageType(FieldUsageType.PREDICTED))

    nbModel.setMiningSchema(miningSchema)
    nbModel.setBayesInputs(bayesInputs)
    nbModel.setBayesOutput(bayesOutput)

    dataDictionary.withNumberOfFields(dataDictionary.getDataFields.size)

    pmml.setDataDictionary(dataDictionary)
    pmml.withModels(nbModel)
  }
}
