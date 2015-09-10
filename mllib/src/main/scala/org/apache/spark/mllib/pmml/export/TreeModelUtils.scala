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

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import org.dmg.pmml.{Node => PMMLNode, Value => PMMLValue, _}

import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}

private[mllib] object TreeModelUtils {

  val FieldNamePrefix = "field_"
  val ClassNamePrefix = "class_"


  def toPMMLTree(dtModel: DecisionTreeModel, modelName: String): (TreeModel, List[DataField]) = {

    val miningFunctionType = getPMMLMiningFunctionType(dtModel.algo)

    val treeModel = new org.dmg.pmml.TreeModel()
      .withModelName(modelName)
      .withFunctionName(miningFunctionType)
      .withSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)

    var (rootNode, miningFields, dataFields, classes) = buildStub(dtModel.topNode, dtModel.algo)

    if (dtModel.algo == Algo.Classification) {
      miningFields = miningFields :+ new MiningField()
        .withName(FieldName.create("class"))
        .withUsageType(FieldUsageType.PREDICTED)

      val dataField = new DataField()
        .withName(FieldName.create("class"))
        .withOpType(OpType.CATEGORICAL).withValues(classes.asJava)
        .withDataType(DataType.STRING)

      dataFields = dataFields :+ dataField
    }

    val miningSchema = new MiningSchema()
      .withMiningFields(miningFields.asJava)

    treeModel.withNode(rootNode)
      .withMiningSchema(miningSchema)

    (treeModel, dataFields)

  }

  /** Build a pmml tree stub given the root mllib node. */
  private def buildStub(
              rootDTNode: Node,
              algo: Algo): (PMMLNode, List[MiningField], List[DataField], List[PMMLValue]) = {

    val miningFields = MutableList[MiningField]()
    val dataFields = mutable.HashMap[String, DataField]()
    val classes = mutable.MutableList[Double]()

    def buildStubInternal(rootNode: Node, predicate: Predicate): PMMLNode = {

      // get rootPMML node for the MLLib node
      val rootPMMLNode = createNode(rootNode)
      rootPMMLNode.withPredicate(predicate)

      var leftPredicate: Predicate = new True()
      var rightPredicate: Predicate = new True()

      if (rootNode.split.isDefined) {
        val fieldName = FieldName.create(FieldNamePrefix + rootNode.split.get.feature)
        val dataField = getDataField(rootNode, fieldName).get

        if (!dataFields.get(dataField.getName.getValue).isDefined) {
          dataFields.put(dataField.getName.getValue, dataField)
          miningFields += new MiningField()
            .withName(dataField.getName)
            .withUsageType(FieldUsageType.ACTIVE)

        } else if (dataField.getOpType != OpType.CONTINUOUS.value()) {
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
        rootPMMLNode.withNodes(leftNode)
      }
      // if right node exist, add the node
      if (rootNode.rightNode.isDefined) {
        val rightNode = buildStubInternal(rootNode.rightNode.get, rightPredicate)
        rootPMMLNode.withNodes(rightNode)
      }

      //add to the list of classes
      if (rootNode.isLeaf && (algo == Algo.Classification)) {
        classes += rootNode.predict.predict
      }

      rootPMMLNode
    }

    val pmmlTreeRootNode = buildStubInternal(rootDTNode, new True())

    val pmmlValues = classes.toList.distinct.map(doubleVal => doubleVal.toInt)
      .map(classInt => ClassNamePrefix + classInt.toString).map(strVal => new PMMLValue(strVal))

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
        .groupBy { case category => category.getValue }.toMap

      values.foreach(category => {
        if (!existingValues.get(category.getValue).isDefined) {
          dtField.withValues(category)
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
        case FeatureType.Continuous => {
          val value = split.threshold.toString
          if (isLeft) {
            new SimplePredicate(field, SimplePredicate.Operator.LESS_OR_EQUAL)
              .withValue(value)
          }
          else {
            new SimplePredicate(field, SimplePredicate.Operator.GREATER_THAN)
              .withValue(value)
          }
        }
        case FeatureType.Categorical => {
          if (split.categories.length > 1) {
            if (isLeft) {
              val predicates: List[Predicate] =
                for (category <- split.categories)
                  yield
                  new SimplePredicate(field, SimplePredicate.Operator.EQUAL)
                    .withValue(category.toString)

              val compoundPredicate = new CompoundPredicate()
                .withBooleanOperator(CompoundPredicate.BooleanOperator.OR)
                .withPredicates(predicates.asJava)

              compoundPredicate
            } else {
              new True()
            }
          }
          else {
            val value = split.categories(0).toString

            if (isLeft) {
              new SimplePredicate(field, SimplePredicate.Operator.EQUAL)
                .withValue(value)
            }
            else {
              new True()
            }
          }
        }
      }
    }
    else {
      new True()
    }

  }

  /** Create equivalent PMML node for given mlLib node. */
  private def createNode(mlLibNode: Node): PMMLNode = {
    val node = new PMMLNode()
      .withId(mlLibNode.id.toString)
      .withScore(mlLibNode.predict.predict.toString)

    node
  }

  /** Get PMML datafield based on the mllib split feature. */
  private def getDataField(mllibNode: Node, fieldName: FieldName): Option[DataField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get
      val dataField = new DataField()
        .withName(fieldName)
        .withDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
        .withOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match {
        case FeatureType.Continuous => dataField.withOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical => {
          dataField.withOpType(OpType.CATEGORICAL)
          val categories = split.categories
            .map(category => new PMMLValue(category.toString)).asJava
          dataField.withValues(categories)
        }
      }

      Some(dataField)
    }
    else {
      None
    }
  }

  /** Get PMML mining function type for given mlLib Algo. */
  private def getPMMLMiningFunctionType(mlLibAlgo: Algo): MiningFunctionType = {
    mlLibAlgo match {
      case Algo.Classification => MiningFunctionType.CLASSIFICATION
      case Algo.Regression => MiningFunctionType.REGRESSION
    }
  }
}
