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

import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.dmg.pmml.{Node => PMMLNode, _}

import scala.collection.JavaConverters._
import scala.collection.mutable

private[mllib] object TreeModelUtils{

  private def buildPMMLTree(topNode: Node): PMMLNode = {
    // For now we support only Continuous FeatureType.
    buildNodes(topNode)
  }

  private def buildNodes(rootNode: Node): PMMLNode = {

    // get predicate for the node
    val predicate: Option[Predicate] =
      if (rootNode.split.isDefined) {
        val fieldName = FieldName.create(rootNode.split.get.feature.toString)
        Some(createSimplePredicate(fieldName, SimplePredicate.Operator.LESS_OR_EQUAL,
          rootNode.split.get.threshold.toString))

      }else
        None

    val rootPMMLNode = createNode(rootNode, predicate)

    if (!rootNode.isLeaf) {
      // if left node exist, add the node
      if (rootNode.leftNode.isDefined) {
        val leftNode = buildNodes(rootNode.leftNode.get)
        rootPMMLNode.withNodes(leftNode)
      }
      // if right node exist, add the node
      if (rootNode.rightNode.isDefined) {
        val rightNode = buildNodes(rootNode.rightNode.get)
        rootPMMLNode.withNodes(rightNode)
      }

    }
    rootPMMLNode
  }

  // create new node
  private def createNode(mlLibNode: Node, predicate: Option[Predicate] = None): PMMLNode = {
    val node = new PMMLNode()
      .withId(mlLibNode.id.toString)
      .withScore(mlLibNode.predict.predict.toString)

    predicate match {
      case Some(p) => node.withPredicate(p)
      case None =>
    }

    node
  }

  // Simple Predicate
  private def createSimplePredicate(name: FieldName, operator: SimplePredicate.Operator, value: String): SimplePredicate = {
    new SimplePredicate(name, operator)
      .withValue(value)
  }

  // Compound Predicate
  private def createCompoundPredicate(operator: CompoundPredicate.BooleanOperator, predicate: Predicate): CompoundPredicate = {
    new CompoundPredicate(operator)
      .withPredicates(predicate)
  }

  def getPMMLTreeFromMLLib(mllibTreeModel: DecisionTreeModel, modelName: String): TreeModel = {

    val miningFunctionType = getPMMLMiningFunctionType(mllibTreeModel.algo)

    val miningSchema = new MiningSchema()
      .withMiningFields(getMiningFieldsForTree(mllibTreeModel).asJava)

    val treeModel = new org.dmg.pmml.TreeModel() //DecisionTree()
      .withModelName(modelName)
      .withFunctionName(miningFunctionType)
      .withNode(buildPMMLTree(mllibTreeModel.topNode))
      .withMiningSchema(miningSchema)

    treeModel

  }

  def getPMMLMiningFunctionType(mlLibAlgo: Algo): MiningFunctionType = {
    MiningFunctionType.fromValue(mlLibAlgo.toString.toLowerCase)
  }


  //get PMML datafield from MLLib node
  private def getDataField(mllibNode: Node): Option[DataField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get
      val dataField = new DataField()
        .withName(FieldName.create(split.feature.toString))
        .withDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
        .withOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match{
        case FeatureType.Continuous => dataField.withOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical => dataField.withOpType(OpType.CATEGORICAL)
          dataField.withValues(new Value(split.threshold.toString))
        case _ => throw new RuntimeException("Only continuous and catigorical datatypes are supported now.")
      }

      Some(dataField)
    } else
      None
  }

  //get PMML datafield from MLLib node
  private def getMiningField(mllibNode: Node): Option[MiningField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get

      val miningField = new MiningField()
        .withName(FieldName.create(split.feature.toString))
        .withUsageType(FieldUsageType.ACTIVE)

      Some(miningField)
    } else
      None
  }

  def getMiningFieldsForTree(treeModel: DecisionTreeModel): List[MiningField] = {
    val miningFields = new mutable.MutableList[MiningField]()
    def appendMiningFields(node: Node) {

      val miningField = getMiningField(node)

      if (miningField.isDefined && !miningFields.contains(miningField.get))
        miningFields += miningField.get

      if (node.leftNode.isDefined) {
        appendMiningFields(node.leftNode.get)
      }
      if (node.rightNode.isDefined) {
        appendMiningFields(node.rightNode.get)
      }
    }

    appendMiningFields(treeModel.topNode)
    miningFields.toList
  }

  def getDataFieldsForTree(treeModel: DecisionTreeModel): List[DataField] = {
    val dataFields = new mutable.MutableList[DataField]()

    def appendDataFields(node: Node) {
      val dataField = getDataField(node)
      if (dataField.isDefined) {
        if (!dataFields.contains(dataField.get))
          dataFields += dataField.get

        if (node.leftNode.isDefined) {
          appendDataFields(node.leftNode.get)
        }
        if (node.rightNode.isDefined) {
          appendDataFields(node.rightNode.get)
        }
      }
    }
    appendDataFields(treeModel.topNode)

    val distinctFields = dataFields.groupBy(dataField => dataField.getName.getValue).map{case ( fieldName, dataFieldsArr) => {
      val finalDataField = dataFieldsArr.head.getOpType match {
        case OpType.CONTINUOUS => dataFieldsArr.head
        case OpType.CATEGORICAL => dataFieldsArr.reduce{case(a,b) => a.withValues(b.getValues); a}
        case _=>  throw new RuntimeException("Only continuous and catigorical datatypes are supported now.")
      }
      finalDataField
    }}

    distinctFields.toList
  }

  def buildSegmentForTree(treeModel: DecisionTreeModel, modelName: String, weight: Option[Double] = None): Segment = {

    val segment = new Segment()
      .withId(treeModel.topNode.id.toString)
      .withModel(getPMMLTreeFromMLLib(treeModel, modelName))

    if (weight.isDefined)
      segment.withWeight(weight.get)

    segment
  }
}