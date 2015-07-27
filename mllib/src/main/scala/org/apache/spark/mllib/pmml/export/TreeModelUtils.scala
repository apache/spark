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
import org.dmg.pmml.{Node => PMMLNode, Value => PMMLValue, _}

import scala.collection.JavaConverters._
import scala.collection.mutable

private[mllib] object TreeModelUtils{

  private def buildPMMLTree(topNode: Node): PMMLNode = {
    // For now we support only Continuous FeatureType.
    buildNodes(topNode)
  }

  private def buildNodes(rootNode: Node): PMMLNode = {

    // get rootPMML node for the MLLib node
    val rootPMMLNode = createNode(rootNode)

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

  private def getPredicate(node : Node): Option[Predicate] ={
    // compound predicate if classification and categories list length > 0

    if(node.split.isDefined){
      val split = node.split.get
      val featureType = split.featureType
      val fieldName = FieldName.create("field_" +split.feature.toString)
      featureType match{
        case FeatureType.Continuous => {
          val value = split.threshold.toString
          Some(new SimplePredicate(fieldName, SimplePredicate.Operator.LESS_OR_EQUAL)
            .withValue(value))
        }
        case FeatureType.Categorical => {
          if (split.categories.length > 1) {
            val predicates: List[Predicate] =
              for (category <- split.categories)
                yield
                new SimplePredicate(fieldName, SimplePredicate.Operator.EQUAL)
                  .withValue(category.toString)

            val compoundPredicate = new CompoundPredicate()
              .withBooleanOperator(CompoundPredicate.BooleanOperator.OR)
              .withPredicates(predicates.asJava)

            Some(compoundPredicate)

          } else {
            val value = split.categories(0).toString
            Some(new SimplePredicate(fieldName, SimplePredicate.Operator.EQUAL)
              .withValue(value))
          }
        }
      }
    }else
      None
  }

  // create new node
  private def createNode(mlLibNode: Node): PMMLNode = {
    val node = new PMMLNode()
      .withId(mlLibNode.id.toString)
      .withScore(mlLibNode.predict.predict.toString)

    val predicate = getPredicate(mlLibNode)
    if(predicate.isDefined)
      node.withPredicate(predicate.get)

    node
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
        .withName(FieldName.create("field_" +split.feature.toString))
        .withDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
        .withOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match{
        case FeatureType.Continuous => dataField.withOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical =>{
          dataField.withOpType(OpType.CATEGORICAL)
          val categories = split.categories.map(category => new org.dmg.pmml.Value(category.toString)).asJava
          dataField.withValues(categories)
        }
      }

      Some(dataField)
    } else
      None
  }

  //get PMML mining field for MLLib node
  private def getMiningField(mllibNode: Node): Option[MiningField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get

      val miningField = new MiningField()
        .withName(FieldName.create("field_" +split.feature.toString))
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

    val distinctFields = miningFields.groupBy(dField => dField.getName.getValue).map {
      case (name, fields) => fields.get(0)
    }.filter(_.isDefined).map(_.get).toList

    distinctFields.toList
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
      val finalDataFields: DataField = dataFieldsArr.head.getOpType match {
        case OpType.CONTINUOUS => dataFieldsArr.head
        case OpType.CATEGORICAL => dataFieldsArr.toList.reduce((a,b) => {a.withValues(b.getValues); a})
        case _=>  throw new RuntimeException("Only continuous and catigorical datatypes are supported now.")
      }
      finalDataFields
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