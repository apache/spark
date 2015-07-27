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
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{FeatureType, Strategy, Algo}
import org.apache.spark.mllib.tree.impurity.{Gini, Impurities}
import org.apache.spark.mllib.tree.model.{Split, Predict, Node, DecisionTreeModel}
import org.apache.spark.mllib.tree.{DecisionTree, DecisionTreeSuite}
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.dmg.pmml.CompoundPredicate.BooleanOperator
import org.dmg.pmml._
import scala.collection.JavaConverters._

class DecisionTreePMMLModelExportSuite extends SparkFunSuite with MLlibTestSparkContext{

  test("PMML export should work as expected for DecisionTree model with regressor"){
    val mlLeftNode = new Node(2, new Predict(0.5, 0.5),0.2,true,None,None,None,None)
    val mlRightNode = new Node(3, new Predict(1.0, 0.5),0.2,true,None,None,None,None)
    val split = new Split(100,10.00,FeatureType.Continuous,Nil)
    val mlTopNode = new Node(1,new Predict(0.0,0.1),0.2, false, Some(split),Some(mlLeftNode),Some(mlRightNode),None)

    val decisionTreeModel = new DecisionTreeModel(mlTopNode, Algo.Regression)

    val pmmlExporterForDT = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel)

    assert(pmmlExporterForDT.isInstanceOf[DecisionTreePMMLModelExport])

    val pmmlWrapperForDT = pmmlExporterForDT.getPmml

    println(decisionTreeModel.toPMML())

    assert(pmmlWrapperForDT.getHeader.getDescription == "decision tree")

    assert(! pmmlWrapperForDT.getModels.isEmpty)
    assert(pmmlWrapperForDT.getModels.size() == 1)

    val pmmlModelForDT = pmmlWrapperForDT.getModels.get(0)
    assert(pmmlModelForDT.isInstanceOf[TreeModel])

    val pmmlTreeModel = pmmlModelForDT.asInstanceOf[TreeModel]

    assert(pmmlTreeModel.getFunctionName == MiningFunctionType.REGRESSION)

    val pmmlRootNode = pmmlTreeModel.getNode
    assert(pmmlRootNode != null)
    assert(pmmlRootNode.getNodes != null && pmmlRootNode.getNodes.size() == 2)
    assert(pmmlRootNode.getId === "1")

    val predicate = pmmlRootNode.getPredicate()
    assert(predicate != null)
    assert(predicate.isInstanceOf[SimplePredicate])
    assert(predicate.asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicate.asInstanceOf[SimplePredicate].getValue === "10.0")
    assert(predicate.asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.LESS_OR_EQUAL)


    val pmmlLeftNode = pmmlRootNode.getNodes.get(0)
    assert(pmmlLeftNode != null)
    assert(!pmmlLeftNode.hasNodes)
    assert(pmmlLeftNode.getId === "2")
    assert(pmmlLeftNode.getScore == "0.5")

    val pmmlRightNode = pmmlRootNode.getNodes.get(1)
    assert(pmmlRightNode != null)
    assert(!pmmlRightNode.hasNodes)
    assert(pmmlRightNode.getId === "3")
    assert(pmmlRightNode.getScore == "1.0")

    assert(pmmlModelForDT.getMiningSchema != null)
    val miningSchema = pmmlModelForDT.getMiningSchema
    assert(miningSchema.getMiningFields != null && miningSchema.getMiningFields.size() == 1)

    val miningFields = miningSchema.getMiningFields
    assert(miningFields.get(0).getName.getValue === "field_100")


    val dataDictionary = pmmlWrapperForDT.getDataDictionary
    assert(dataDictionary != null)
    val dataFields = dataDictionary.getDataFields
    assert(dataFields != null && dataFields.size() == 1)
    assert(dataFields.get(0).getName.getValue == "field_100")
    assert(dataFields.get(0).getOpType == OpType.CONTINUOUS)


  }

  test("PMML export should work as expected for DecisionTree model with classifier"){
    val mlLeftNode_L2 = new Node(4, new Predict(1.0, 0.5),0.2,true,None,None,None,None)
    val mlRightNode_L2 = new Node(5, new Predict(2.0, 0.5),0.2,true,None,None,None,None)
    val splitForL2 = new Split(100,10.00,FeatureType.Categorical,List(1,4))
    val mlLeftNode_L1 = new Node(2, new Predict(3.0, 0.5),0.2,false,Some(splitForL2),Some(mlLeftNode_L2),Some(mlRightNode_L2),None)

    val mlRightNode_L1 = new Node(3, new Predict(4.0, 0.5),0.2,true,None,None,None,None)

    val split = new Split(100,10.00,FeatureType.Categorical,List(1,3,4))

    val mlTopNode = new Node(1,new Predict(5.0,0.1),0.2, false, Some(split),Some(mlLeftNode_L1),Some(mlRightNode_L1),None)

    val decisionTreeModel = new DecisionTreeModel(mlTopNode, Algo.Classification)

    println(decisionTreeModel.toPMML())

    val pmmlExporterForDT = PMMLModelExportFactory.createPMMLModelExport(decisionTreeModel)

    assert(pmmlExporterForDT.isInstanceOf[DecisionTreePMMLModelExport])

    val pmmlWrapperForDT = pmmlExporterForDT.getPmml

    assert(pmmlWrapperForDT.getHeader.getDescription == "decision tree")

    assert(! pmmlWrapperForDT.getModels.isEmpty)
    assert(pmmlWrapperForDT.getModels.size() == 1)

    val pmmlModelForDT = pmmlWrapperForDT.getModels.get(0)
    assert(pmmlModelForDT.isInstanceOf[TreeModel])

    val pmmlTreeModel = pmmlModelForDT.asInstanceOf[TreeModel]

    assert(pmmlTreeModel.getFunctionName == MiningFunctionType.CLASSIFICATION)

    val pmmlRootNode = pmmlTreeModel.getNode
    assert(pmmlRootNode != null)
    assert(pmmlRootNode.getNodes != null && pmmlRootNode.getNodes.size() == 2)
    assert(pmmlRootNode.getId === "1")

    val predicate = pmmlRootNode.getPredicate()
    assert(predicate != null)
    assert(predicate.isInstanceOf[CompoundPredicate])
    val cPredicate1 = predicate.asInstanceOf[CompoundPredicate]

    assert(cPredicate1.getBooleanOperator == BooleanOperator.OR)
    assert(cPredicate1.getPredicates != null && cPredicate1.getPredicates.size() == 3)
    val predicatesList1 = cPredicate1.getPredicates
    assert(predicatesList1.get(0).isInstanceOf[SimplePredicate])
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getValue === "1.0")
    assert(predicatesList1.get(0).asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.EQUAL)

    assert(predicatesList1.get(1).isInstanceOf[SimplePredicate])
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getValue === "3.0")
    assert(predicatesList1.get(1).asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.EQUAL)

    assert(predicatesList1.get(2).isInstanceOf[SimplePredicate])
    assert(predicatesList1.get(2).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList1.get(2).asInstanceOf[SimplePredicate].getValue === "4.0")
    assert(predicatesList1.get(2).asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.EQUAL)



    val pmmlLeftNode_L1 = pmmlRootNode.getNodes.get(0)
    assert(pmmlLeftNode_L1 != null)
    assert(pmmlLeftNode_L1.hasNodes)
    assert(pmmlLeftNode_L1.getId === "2")
    assert(pmmlLeftNode_L1.getScore == "3.0")

    val pmmlRightNode_L1 = pmmlRootNode.getNodes.get(1)
    assert(pmmlRightNode_L1 != null)
    assert(!pmmlRightNode_L1.hasNodes)
    assert(pmmlRightNode_L1.getId === "3")
    assert(pmmlRightNode_L1.getScore == "4.0")


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
    assert(predicatesList2.get(0).asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.EQUAL)

    assert(predicatesList2.get(1).isInstanceOf[SimplePredicate])
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getField.getValue === "field_100")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getValue === "4.0")
    assert(predicatesList2.get(1).asInstanceOf[SimplePredicate].getOperator == SimplePredicate.Operator.EQUAL)


    val pmmlLeftNode_L2 = pmmlLeftNode_L1.getNodes.get(0)
    assert(pmmlLeftNode_L2 != null)
    assert(! pmmlLeftNode_L2.hasNodes)
    assert(pmmlLeftNode_L2.getId === "4")
    assert(pmmlLeftNode_L2.getScore == "1.0")

    val pmmlRightNode_L2 = pmmlLeftNode_L1.getNodes.get(1)
    assert(pmmlRightNode_L2 != null)
    assert(!pmmlRightNode_L2.hasNodes)
    assert(pmmlRightNode_L2.getId === "5")
    assert(pmmlRightNode_L2.getScore == "2.0")


    assert(pmmlModelForDT.getMiningSchema != null)
    val miningSchema = pmmlModelForDT.getMiningSchema
    assert(miningSchema.getMiningFields != null && miningSchema.getMiningFields.size() == 1)

    val miningFields = miningSchema.getMiningFields
    assert(miningFields.get(0).getName.getValue === "field_100")


    val dataDictionary = pmmlWrapperForDT.getDataDictionary
    assert(dataDictionary != null)
    val dataFields = dataDictionary.getDataFields
    assert(dataFields != null && dataFields.size() == 1)
    assert(dataFields.get(0).getName.getValue == "field_100")
    assert(dataFields.get(0).getOpType == OpType.CATEGORICAL)
    assert(dataFields.get(0).getValues != null && dataFields.get(0).getValues.size() == 3)
    val sortedValues = dataFields.get(0).getValues.asScala.toList.sortBy(value => value.getValue)
    assert(sortedValues(0).getValue == "1.0")
    assert(sortedValues(1).getValue == "3.0")
    assert(sortedValues(2).getValue == "4.0")

  }
}
