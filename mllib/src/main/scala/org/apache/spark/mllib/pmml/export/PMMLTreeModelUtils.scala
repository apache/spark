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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.dmg.pmml.{Node => PMMLNode, Value => PMMLValue, _}

import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}

private[mllib] object PMMLTreeModelUtils {

  val FieldNamePrefix = "field_"

  def toPMMLTree(dtModel: DecisionTreeModel, modelName: String): (TreeModel, List[DataField]) = {

    val miningFunctionType = dtModel.algo match {
      case Algo.Classification => MiningFunctionType.CLASSIFICATION
      case Algo.Regression => MiningFunctionType.REGRESSION
    }

    val treeModel = new TreeModel()
      .setModelName(modelName)
      .setFunctionName(miningFunctionType)
      .setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)

    var (rootNode, miningFields, dataFields, classes) = buildStub(dtModel.topNode, dtModel.algo)

    // adding predicted classes for classification and target field for regression for completeness
    dtModel.algo match {

      case Algo.Classification =>
        miningFields = miningFields :+ new MiningField()
          .setName(FieldName.create("class"))
          .setUsageType(FieldUsageType.PREDICTED)

        val dataField = new DataField()
          .setName(FieldName.create("class"))
          .setOpType(OpType.CATEGORICAL)
          .addValues(classes: _*)
          .setDataType(DataType.DOUBLE)

        dataFields = dataFields :+ dataField

      case Algo.Regression =>
        val targetField = FieldName.create("target")
        val dataField = new DataField(targetField, OpType.CONTINUOUS, DataType.DOUBLE)
        dataFields = dataFields :+ dataField

        miningFields = miningFields :+ new MiningField()
          .setName(targetField)
          .setUsageType(FieldUsageType.TARGET)

    }

    val miningSchema = new MiningSchema().addMiningFields(miningFields: _*)

    treeModel.setNode(rootNode).setMiningSchema(miningSchema)

    (treeModel, dataFields)
  }

  /** Build a pmml tree stub given the root mllib node. */
  private def buildStub(rootDTNode: Node, algo: Algo):
    (PMMLNode, List[MiningField], List[DataField], List[PMMLValue]) = {

    val miningFields = mutable.MutableList[MiningField]()
    val dataFields = mutable.HashMap[String, DataField]()
    val classes = mutable.MutableList[Double]()

    def buildStubInternal(rootNode: Node, predicate: Predicate): PMMLNode = {

      // get rootPMML node for the MLLib node
      val rootPMMLNode = new PMMLNode()
        .setId(rootNode.id.toString)
        .setScore(rootNode.predict.predict.toString)
        .setPredicate(predicate)

      var leftPredicate: Predicate = new True()
      var rightPredicate: Predicate = new True()

      if (rootNode.split.isDefined) {
        val fieldName = FieldName.create(FieldNamePrefix + rootNode.split.get.feature)
        val dataField = getDataField(rootNode, fieldName).get

        if (dataFields.get(dataField.getName.getValue).isEmpty) {
          dataFields.put(dataField.getName.getValue, dataField)
          miningFields += new MiningField()
            .setName(dataField.getName)
            .setUsageType(FieldUsageType.ACTIVE)

        } else if (dataField.getOpType != OpType.CONTINUOUS) {
          appendCategories(
            dataFields.get(dataField.getName.getValue).get,
            dataField.getValues.asScala.toList)
        }

        leftPredicate = getPredicate(rootNode, Some(dataField.getName), true)
        rightPredicate = getPredicate(rootNode, Some(dataField.getName), false)
      }
      // if left node exist, add the node
      if (rootNode.leftNode.isDefined) {
        val leftNode = buildStubInternal(rootNode.leftNode.get, leftPredicate)
        rootPMMLNode.addNodes(leftNode)
      }
      // if right node exist, add the node
      if (rootNode.rightNode.isDefined) {
        val rightNode = buildStubInternal(rootNode.rightNode.get, rightPredicate)
        rootPMMLNode.addNodes(rightNode)
      }

      // add to the list of classes
      if (rootNode.isLeaf && (algo == Algo.Classification)) {
        classes += rootNode.predict.predict
      }

      rootPMMLNode
    }
    val pmmlTreeRootNode = buildStubInternal(rootDTNode, new True())
    val pmmlValues = classes.toList.distinct.map(doubleVal => new PMMLValue(doubleVal.toString))
    val result = (pmmlTreeRootNode,
      sortMiningFields(miningFields.toList),
      sortedDataFields(dataFields.values.toList),
      pmmlValues)
    result
  }

  private def sortMiningFields(miningFields: List[MiningField]): List[MiningField] = {
    miningFields
      .sortBy { case mField => mField.getName.getValue.replace(FieldNamePrefix, "").toInt }
  }

  private def sortedDataFields(dataFields: List[DataField]): List[DataField] = {
    dataFields.sortBy { case dField => dField.getName.getValue.replace(FieldNamePrefix, "").toInt }
  }

  private def appendCategories(dtField: DataField, values: List[PMMLValue]): DataField = {
    if (dtField.getOpType == OpType.CATEGORICAL) {

      val existingValues = dtField.getValues.asScala
        .groupBy { case category => category.getValue }

      values.foreach(category => {
        if (existingValues.get(category.getValue).isEmpty) {
          dtField.addValues(category)
        }
      })
    }

    dtField
  }

  /** Get pmml Predicate for a given mlLib tree node. */
  private def getPredicate(node: Node, fieldName: Option[FieldName], isLeft: Boolean): Predicate = {
    // compound predicate if classification and categories list length > 0

    if (node.split.isDefined) {
      require(fieldName.isDefined, "fieldName should not be None, it should be defined.")
      val field = fieldName.get
      val split = node.split.get
      val featureType = split.featureType

      featureType match {
        case FeatureType.Continuous =>
          val value = split.threshold.toString
          if (isLeft) {
            new SimplePredicate(field, SimplePredicate.Operator.LESS_OR_EQUAL)
              .setValue(value)
          }
          else {
            new SimplePredicate(field, SimplePredicate.Operator.GREATER_THAN)
              .setValue(value)
          }

        case FeatureType.Categorical =>
          if (split.categories.length > 1) {
            if (isLeft) {
              val predicates: List[Predicate] =
                for (category <- split.categories)
                  yield
                  new SimplePredicate(field, SimplePredicate.Operator.EQUAL)
                    .setValue(category.toString)

              val compoundPredicate = new CompoundPredicate()
                .setBooleanOperator(CompoundPredicate.BooleanOperator.OR)
                .addPredicates(predicates: _*)

              compoundPredicate
            } else {
              new True()
            }
          }
          else {
            val value = split.categories(0).toString

            if (isLeft) {
              new SimplePredicate(field, SimplePredicate.Operator.EQUAL)
                .setValue(value)
            }
            else {
              new True()
            }
          }
      }
    }
    else {
      new True()
    }

  }

  /** Get PMML datafield based on the mllib split feature. */
  private def getDataField(mllibNode: Node, fieldName: FieldName): Option[DataField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get
      val dataField = new DataField()
        .setName(fieldName)
        .setDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
        .setOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match {
        case FeatureType.Continuous => dataField.setOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical =>
          dataField.setOpType(OpType.CATEGORICAL)
          val categories = split.categories
            .map(category => new PMMLValue(category.toString))
            dataField.addValues(categories: _*)
      }

      Some(dataField)
    }
    else {
      None
    }
  }
}
