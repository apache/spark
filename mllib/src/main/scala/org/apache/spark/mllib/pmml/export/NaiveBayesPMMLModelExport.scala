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

import org.apache.spark.mllib.classification.{NaiveBayesModel => SNaiveBayesModel, NaiveBayes}

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
    if (model.modelType == NaiveBayes.Bernoulli) {
      for (i <- featureIndices) {
        fields(i) = FieldName.create("field_" + i)
        dataDictionary
          .addDataFields(new DataField(fields(i), OpType.CATEGORICAL, DataType.DOUBLE)
          .addValues(new Value("0.0"), new Value("1.0")))
        miningSchema
          .addMiningFields(new MiningField(fields(i)).setUsageType(FieldUsageType.ACTIVE))

        val pairsExist = labelIndices.map { label =>
          new TargetValueCount()
            .setValue(label.toDouble.toString).setCount(math.exp(model.theta(label)(i)))
        }

        val pairCountsExist = new PairCounts()
          .setTargetValueCounts(new TargetValueCounts().addTargetValueCounts(pairsExist: _*))
          .setValue("1.0")

        val pairsAbsent = labelIndices.map { label =>
          new TargetValueCount()
            .setValue(label.toDouble.toString).setCount(1.0 - math.exp(model.theta(label)(i)))
        }

        val pairCountsAbsent = new PairCounts()
          .setTargetValueCounts(new TargetValueCounts().addTargetValueCounts(pairsAbsent: _*))
          .setValue("0.0")

        val bayesInput = new BayesInput()

        bayesInput.setFieldName(fields(i)).addPairCounts(pairCountsExist, pairCountsAbsent)
        bayesInputs.addBayesInputs(bayesInput)
      }
    } else {
      throw new IllegalArgumentException(
        "Naive Bayes model PMML export only supports Bernoulli model type for now.")
    }

    // add target field
    val targetField = FieldName.create("class")
    dataDictionary
      .addDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.DOUBLE)
      .addValues(labelIndices.map { x => new Value().setValue(x.toDouble.toString)}: _*))
    miningSchema
      .addMiningFields(new MiningField(targetField).setUsageType(FieldUsageType.PREDICTED))

    // add Bayes output
    val targetValueCounts = model.pi.zipWithIndex.map { case (x, i) =>
      new TargetValueCount().setValue(i.toDouble.toString).setCount(math.exp(x)) }
    bayesOutput
      .setTargetValueCounts(new TargetValueCounts().addTargetValueCounts(targetValueCounts: _*))
      .setFieldName(targetField)

    // add output
    val output = new Output()
    output.addOutputFields(
      new OutputField()
        .setName(FieldName.create("Predicted_class"))
        .setFeature(FeatureType.PREDICTED_VALUE))
    output.addOutputFields(labelIndices.map { label =>
      new OutputField()
        .setName(FieldName.create(s"Probability_${label.toDouble}"))
        .setOpType(OpType.CONTINUOUS)
        .setDataType(DataType.DOUBLE)
        .setFeature(FeatureType.PROBABILITY)
        .setValue(s"${label.toDouble}")
      }: _*)

    nbModel.setMiningSchema(miningSchema)
    nbModel.setBayesInputs(bayesInputs)
    nbModel.setBayesOutput(bayesOutput)
    nbModel.setOutput(output)

    dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

    pmml.setDataDictionary(dataDictionary)
    pmml.addModels(nbModel)
  }
}
