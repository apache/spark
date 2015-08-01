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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node, Predict, Split}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.dmg.pmml.CompoundPredicate.BooleanOperator
import org.dmg.pmml._
import org.scalatest.PrivateMethodTester

import scala.collection.JavaConverters._

class DecisionTreePMMLModelExportSuite extends SparkFunSuite
  with MLlibTestSparkContext
  with PrivateMethodTester {

  test("PMML export should work as expected for DecisionTree model with regressor") {

    // instantiate a MLLib DecisionTreeModel with Regression and with 3 nodes with continuous
    // feature type
    val mlLeftNode = new Node(2, new Predict(0.5, 0.5), 0.2, true, None, None, None, None)
    val mlRightNode = new Node(3, new Predict(1.0, 0.5), 0.2, true, None, None, None, None)
    val split = new Split(100, 10.00, FeatureType.Continuous, Nil)
    val mlTopNode = new Node(1, new Predict(0.0, 0.1), 0.2, false,
      Some(split), Some(mlLeftNode), Some(mlRightNode), None)

    val decisionTreeModel = new DecisionTreeModel(mlTopNode, Algo.Regression)

    // get the pmml exporter for the DT and verify its the right exporter
    val pmmlExporterForDT = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel)
    assert(pmmlExporterForDT.isInstanceOf[DecisionTreePMMLModelExport])

    // get the pmmlwrapper object for DT and verify the inner model is of type TreeModel
    // and basic fields are populated as expected
    val pmmlWrapperForDT = pmmlExporterForDT.getPmml
    assert(pmmlWrapperForDT.getHeader.getDescription == "decision tree")
    assert(!pmmlWrapperForDT.getModels.isEmpty)
    assert(pmmlWrapperForDT.getModels.size() == 1)
    val pmmlModelForDT = pmmlWrapperForDT.getModels.get(0)
    assert(pmmlModelForDT.isInstanceOf[TreeModel])

    // validate the inner tree model fields are populated as expected
    val pmmlTreeModel = pmmlModelForDT.asInstanceOf[TreeModel]
    assert(pmmlTreeModel.getFunctionName == MiningFunctionType.REGRESSION)

    // validate the root PMML node is populated as expected
    val pmmlRootNode = pmmlTreeModel.getNode
    assert(pmmlRootNode != null)
    assert(pmmlRootNode.getNodes != null && pmmlRootNode.getNodes.size() == 2)
    assert(pmmlRootNode.getId === "1")
    // validate the root node predicate is populated as expected
    val predicate = pmmlRootNode.getPredicate()
    assert(predicate != null)
    assert(predicate.isInstanceOf[SimplePredicate])
    assert(predicate.asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicate.asInstanceOf[SimplePredicate].getValue === "10.0")
    assert(predicate.asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator
    .LESS_OR_EQUAL)

    // validate the left node is populated as expected
    val pmmlLeftNode = pmmlRootNode.getNodes.get(0)
    assert(pmmlLeftNode != null)
    assert(!pmmlLeftNode.hasNodes)
    assert(pmmlLeftNode.getId === "2")
    assert(pmmlLeftNode.getScore == "0.5")

    // validate the right node is populated as expected
    val pmmlRightNode = pmmlRootNode.getNodes.get(1)
    assert(pmmlRightNode != null)
    assert(!pmmlRightNode.hasNodes)
    assert(pmmlRightNode.getId === "3")
    assert(pmmlRightNode.getScore == "1.0")

    // validate the mining schema is populated as expected
    assert(pmmlModelForDT.getMiningSchema != null)
    val miningSchema = pmmlModelForDT.getMiningSchema
    assert(miningSchema.getMiningFields != null && miningSchema.getMiningFields.size() == 1)
    val miningFields = miningSchema.getMiningFields
    assert(miningFields.get(0).getName.getValue === "field_100")

    // validate the data dictionay is populated as expected
    val dataDictionary = pmmlWrapperForDT.getDataDictionary
    assert(dataDictionary != null)
    val dataFields = dataDictionary.getDataFields
    assert(dataFields != null && dataFields.size() == 1)
    assert(dataFields.get(0).getName.getValue == "field_100")
    assert(dataFields.get(0).getOpType == OpType.CONTINUOUS)

  }

  test("PMML export should work as expected for DecisionTree model with classifier") {

    // instantiate MLLIb DecisionTreeModel with Classification algo ,5 nodes, 2 levels
    val mlLeftNode_L2 = new Node(4, new Predict(1.0, 0.5), 0.2, true, None, None, None, None)
    val mlRightNode_L2 = new Node(5, new Predict(2.0, 0.5), 0.2, true, None, None, None, None)
    val splitForL2 = new Split(100, 10.00, FeatureType.Categorical, List(1, 4))
    val mlLeftNode_L1 = new Node(2, new Predict(3.0, 0.5), 0.2, false,
      Some(splitForL2), Some(mlLeftNode_L2), Some(mlRightNode_L2), None)
    val mlRightNode_L1 = new Node(3, new Predict(4.0, 0.5), 0.2, true, None, None, None, None)
    val split = new Split(200, 10.00, FeatureType.Categorical, List(10, 20))
    val mlTopNode = new Node(1, new Predict(5.0, 0.1), 0.2, false, Some(split),
      Some(mlLeftNode_L1), Some(mlRightNode_L1), None)
    val decisionTreeModel = new DecisionTreeModel(mlTopNode, Algo.Classification)


    // get the pmml exporter for the DT and verify its the right exporter
    val pmmlExporterForDT = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel)
    assert(pmmlExporterForDT.isInstanceOf[DecisionTreePMMLModelExport])

    // get the pmmlwrapper object for DT and verify the inner model is of type TreeModel
    // and basic fields are populated as expected
    val pmmlWrapperForDT = pmmlExporterForDT.getPmml
    assert(pmmlWrapperForDT.getHeader.getDescription == "decision tree")
    assert(!pmmlWrapperForDT.getModels.isEmpty)
    assert(pmmlWrapperForDT.getModels.size() == 1)

    // validate the inner tree model fields are populated as expected
    val pmmlModelForDT = pmmlWrapperForDT.getModels.get(0)
    assert(pmmlModelForDT.isInstanceOf[TreeModel])
    val pmmlTreeModel = pmmlModelForDT.asInstanceOf[TreeModel]
    assert(pmmlTreeModel.getFunctionName == MiningFunctionType.CLASSIFICATION)

    // validate the pmml root node fields are populated as expected
    val pmmlRootNode = pmmlTreeModel.getNode
    assert(pmmlRootNode != null)
    assert(pmmlRootNode.getNodes != null && pmmlRootNode.getNodes.size() == 2)
    assert(pmmlRootNode.getId === "1")

    // validate the pmml root node predicate is a compound predicate since the mllib split had
    // multiple categories
    val predicate = pmmlRootNode.getPredicate()
    assert(predicate != null)
    assert(predicate.isInstanceOf[CompoundPredicate])
    val cPredicate1 = predicate.asInstanceOf[CompoundPredicate]
    assert(cPredicate1.getBooleanOperator == BooleanOperator.OR)
    assert(cPredicate1.getPredicates != null && cPredicate1.getPredicates.size() == 2)
    val predicatesList1 = cPredicate1.getPredicates
    assert(predicatesList1.get(0).isInstanceOf[SimplePredicate])
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getField.getValue === "field_200")
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getValue === "10.0")
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)

    assert(predicatesList1.get(1).isInstanceOf[SimplePredicate])
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getField.getValue === "field_200")
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getValue === "20.0")
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)

    // validate level 1 left node is populated properly
    val pmmlLeftNode_L1 = pmmlRootNode.getNodes.get(0)
    assert(pmmlLeftNode_L1 != null)
    assert(pmmlLeftNode_L1.hasNodes)
    assert(pmmlLeftNode_L1.getId === "2")
    assert(pmmlLeftNode_L1.getScore == "3.0")

    // validate level 1 right node is populated properly
    val pmmlRightNode_L1 = pmmlRootNode.getNodes.get(1)
    assert(pmmlRightNode_L1 != null)
    assert(!pmmlRightNode_L1.hasNodes)
    assert(pmmlRightNode_L1.getId === "3")
    assert(pmmlRightNode_L1.getScore == "4.0")

    // validate predicate for level 2 split is populated as expected
    val predicate2 = pmmlLeftNode_L1.getPredicate()
    assert(predicate2 != null)
    assert(predicate2.isInstanceOf[CompoundPredicate])
    val cPredicate2 = predicate2.asInstanceOf[CompoundPredicate]

    assert(cPredicate2.getBooleanOperator == BooleanOperator.OR)
    assert(cPredicate2.getPredicates != null && cPredicate2.getPredicates.size() == 2)
    val predicatesList2 = cPredicate2.getPredicates
    assert(predicatesList2.get(0).isInstanceOf[SimplePredicate])
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getValue === "1.0")
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)

    assert(predicatesList2.get(1).isInstanceOf[SimplePredicate])
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getValue === "4.0")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)


    // validate level 2 left node is populated as expected
    val pmmlLeftNode_L2 = pmmlLeftNode_L1.getNodes.get(0)
    assert(pmmlLeftNode_L2 != null)
    assert(!pmmlLeftNode_L2.hasNodes)
    assert(pmmlLeftNode_L2.getId === "4")
    assert(pmmlLeftNode_L2.getScore == "1.0")

    // validate level 2 right node is populated as expected
    val pmmlRightNode_L2 = pmmlLeftNode_L1.getNodes.get(1)
    assert(pmmlRightNode_L2 != null)
    assert(!pmmlRightNode_L2.hasNodes)
    assert(pmmlRightNode_L2.getId === "5")
    assert(pmmlRightNode_L2.getScore == "2.0")

    // validate mining schema is populated as expected
    assert(pmmlModelForDT.getMiningSchema != null)
    val miningSchema = pmmlModelForDT.getMiningSchema
    assert(miningSchema.getMiningFields != null && miningSchema.getMiningFields.size() == 2)

    val miningFields = miningSchema.getMiningFields
    assert(miningFields.get(0).getName.getValue === "field_100")
    assert(miningFields.get(1).getName.getValue === "field_200")


    // validate data dictionary is populated as expected
    val dataDictionary = pmmlWrapperForDT.getDataDictionary
    assert(dataDictionary != null)
    val dataFields = dataDictionary.getDataFields
    assert(dataFields != null && dataFields.size() == 2)
    val sortedDataFields = dataFields.asScala.toList.sortBy(dataField => dataField.getName.getValue)
    assert(sortedDataFields(0).getName.getValue == "field_100")
    assert(sortedDataFields(0).getOpType == OpType.CATEGORICAL)
    assert(sortedDataFields(0).getValues != null && sortedDataFields(0).getValues.size() == 2)
    val sortedValues1 = sortedDataFields(0).getValues.asScala.toList.sortBy(value => value.getValue)
    assert(sortedValues1(0).getValue == "1.0")
    assert(sortedValues1(1).getValue == "4.0")

    assert(sortedDataFields(1).getName.getValue == "field_200")
    assert(sortedDataFields(1).getOpType == OpType.CATEGORICAL)
    assert(sortedDataFields(1).getValues != null && sortedDataFields(1).getValues.size() == 2)
    val sortedValues2 = sortedDataFields(1).getValues.asScala.toList.sortBy(value => value.getValue)
    assert(sortedValues2(0).getValue == "10.0")
    assert(sortedValues2(1).getValue == "20.0")

  }

  test("TreeModelUtils should return distinct datafields and miningfields for continuous " +
    "features") {

    // instantiate MLLIb DecisionTreeModel with Classification algo ,5 nodes, 2 levels
    val mlLeftNode_L3 = new Node(6, new Predict(1.0, 0.5), 0.2, true, None, None, None, None)
    val mlRightNode_L3 = new Node(7, new Predict(2.0, 0.5), 0.2, true, None, None, None, None)
    val splitForL3 = new Split(100, 10.00, FeatureType.Continuous, Nil)
    val mlLeftNode_L2 = new Node(4, new Predict(3.0, 0.5), 0.2, false, Some(splitForL3),
      Some(mlLeftNode_L3), Some(mlRightNode_L3), None)
    val mlRightNode_L2 = new Node(5, new Predict(4.0, 0.5), 0.2, true, None, None, None, None)

    val splitForL2 = new Split(100, 4.00, FeatureType.Continuous, Nil)
    val mlLeftNode_L1 = new Node(2, new Predict(3.0, 0.5), 0.2, false,
      Some(splitForL2), Some(mlLeftNode_L2), Some(mlRightNode_L3), None)
    val mlRightNode_L1 = new Node(3, new Predict(4.0, 0.5), 0.2, true, None, None, None, None)

    val split1 = new Split(200, 10.00, FeatureType.Categorical, List(10))
    val mlTopNode = new Node(1, new Predict(5.0, 0.1), 0.2, false, Some(split1),
      Some(mlLeftNode_L1), Some(mlRightNode_L1), None)
    val decisionTreeModel = new DecisionTreeModel(mlTopNode, Algo.Regression)

    // get the pmml exporter for the DT and verify its the right exporter
    val pmmlExporterForDT = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel)
    assert(pmmlExporterForDT.isInstanceOf[DecisionTreePMMLModelExport])

    // get the pmmlwrapper object for DT and verify the inner model is of type TreeModel
    // and basic fields are populated as expected
    val pmmlWrapperForDT = pmmlExporterForDT.getPmml
    // validate the inner tree model fields are populated as expected
    val pmmlModelForDT = pmmlWrapperForDT.getModels.get(0)
    assert(pmmlModelForDT.isInstanceOf[TreeModel])
    // validate mining schema is populated as expected
    assert(pmmlModelForDT.getMiningSchema != null)
    val miningSchema = pmmlModelForDT.getMiningSchema
    assert(miningSchema.getMiningFields != null && miningSchema.getMiningFields.size() == 2)

    val miningFields = miningSchema.getMiningFields
    assert(miningFields.get(0).getName.getValue === "field_100")
    assert(miningFields.get(1).getName.getValue === "field_200")


    // validate data dictionary is populated as expected
    val dataDictionary = pmmlWrapperForDT.getDataDictionary
    assert(dataDictionary != null)
    val dataFields = dataDictionary.getDataFields
    assert(dataFields != null && dataFields.size() == 2)
    val sortedDataFields = dataFields.asScala.toList.sortBy(dataField => dataField.getName.getValue)
    assert(sortedDataFields(0).getName.getValue == "field_100")
    assert(sortedDataFields(0).getOpType == OpType.CONTINUOUS)
    assert(sortedDataFields(0).getValues.isEmpty)

    assert(sortedDataFields(1).getName.getValue == "field_200")
    assert(sortedDataFields(1).getOpType == OpType.CATEGORICAL)
    assert(sortedDataFields(1).getValues != null && sortedDataFields(1).getValues.size() == 1)
    val sortedValues2 = sortedDataFields(1).getValues.asScala.toList.sortBy(value => value.getValue)
    assert(sortedValues2(0).getValue == "10.0")
  }

  test("TreeModelUtils getPredicate should return simple predicate for node with split with " +
  "continuous feature type") {
    val split = new Split(100, 10.0, FeatureType.Continuous, Nil)
    val treeNode = new Node(1, new Predict(0.5, 0.5), 0.2, true, Some(split), None, None, None)
    val privateMethodProxy = PrivateMethod[Option[Predicate]]('getPredicate)
    val predicate = TreeModelUtils invokePrivate privateMethodProxy(treeNode)
    assert(predicate.isDefined)
    assert(predicate.get.isInstanceOf[SimplePredicate])
    assert(predicate.get.asInstanceOf[SimplePredicate].getField.getValue == "field_100")
    assert(predicate.get.asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator
    .LESS_OR_EQUAL)
    assert(predicate.get.asInstanceOf[SimplePredicate].getValue == "10.0")
  }

  test("TreeModelUtils getPredicate should work as expected for node with split with catogorical " +
    "feature") {
    val split1 = new Split(100, 10.0, FeatureType.Categorical, List(1))
    val treeNode1 = new Node(1, new Predict(0.5, 0.5), 0.2, true, Some(split1), None, None, None)
    val privateMethodProxy = PrivateMethod[Option[Predicate]]('getPredicate)
    val predicate1 = TreeModelUtils invokePrivate privateMethodProxy(treeNode1)
    assert(predicate1.isDefined)
    assert(predicate1.get.isInstanceOf[SimplePredicate])
    assert(predicate1.get.asInstanceOf[SimplePredicate].getField.getValue == "field_100")
    assert(predicate1.get.asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator
    .EQUAL)
    assert(predicate1.get.asInstanceOf[SimplePredicate].getValue == "1.0")

    val split2 = new Split(100, 10.0, FeatureType.Categorical, List(1, 2))
    val treeNode2 = new Node(1, new Predict(0.5, 0.5), 0.2, true, Some(split2), None, None, None)
    val predicate2 = TreeModelUtils invokePrivate privateMethodProxy(treeNode2)
    assert(predicate2.isDefined && predicate2.get.isInstanceOf[CompoundPredicate])
    val cPredicate2 = predicate2.get.asInstanceOf[CompoundPredicate]

    assert(cPredicate2.getBooleanOperator == BooleanOperator.OR)
    assert(cPredicate2.getPredicates != null && cPredicate2.getPredicates.size() == 2)
    val predicatesList2 = cPredicate2.getPredicates
    assert(predicatesList2.get(0).isInstanceOf[SimplePredicate])
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getValue === "1.0")
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)

    assert(predicatesList2.get(1).isInstanceOf[SimplePredicate])
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getValue === "2.0")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getOperator == SimplePredicate
    .Operator.EQUAL)
  }

  test("TreeModelUtils getPredicate returns None if split not defined for node") {
    val treeNode1 = new Node(1, new Predict(0.5, 0.5), 0.2, true, None, None, None, None)
    val privateMethodProxy = PrivateMethod[Option[Predicate]]('getPredicate)
    val predicate1 = TreeModelUtils invokePrivate privateMethodProxy(treeNode1)
    assert(!predicate1.isDefined)
  }
}

