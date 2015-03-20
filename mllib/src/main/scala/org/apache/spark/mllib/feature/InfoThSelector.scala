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

package org.apache.spark.mllib.feature


import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.{InfoTheory => IT}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.feature.{InfoThCriterionFactory => FT}

/**
 * Train a info-theory feature selection model according to a criterion.
 * 
 * @param criterionFactory Factory to create info-theory measurements for each feature.
 * @param data RDD of LabeledPoint (discrete data).
 * 
 */
@Experimental
class InfoThSelector private[feature] (
    val criterionFactory: FT, 
    val data: RDD[LabeledPoint]) extends Serializable {

  // Pool of criterions
  private type Pool = RDD[(Int, InfoThCriterion)]
  // Case class for criterions by feature
  protected case class F(feat: Int, crit: Double)
    
  val (nFeatures, isDense) = data.first.features match {
    case v: SparseVector => (v.size, false)
    case v: DenseVector => (v.size, true)
  }
  
  val byteData: RDD[BV[Byte]] = data.map {
    case LabeledPoint(label, values: SparseVector) => 
      new BSV[Byte](0 +: values.indices.map(_ + 1), 
        label.toByte +: values.values.toArray.map(_.toByte), values.indices.size + 1)
    case LabeledPoint(label, values: DenseVector) => 
      new BDV[Byte](label.toByte +: values.toArray.map(_.toByte))
  }

  /**
   * Perform a info-theory selection process without pool optimization.
   * 
   * @param data Data points (first element is the class attribute).
   * @param nToSelect Number of features to select.
   * @return A list with the most relevant features and its scores.
   * 
   */
  private[feature] def selectFeaturesWithoutPool(data: RDD[BV[Byte]], nToSelect: Int) = {
    
    val nElements = data.count()
    val nFeatures = data.first.size - 1
    val label = 0
    
    // calculate relevance
    val MiAndCmi = IT.miAndCmi(data, 1 to nFeatures, Seq(label), None, nElements, nFeatures)
    var pool = MiAndCmi.map{case ((x, y), (mi, _)) => (x, criterionFactory.getCriterion.init(mi))}
      .collectAsMap()  
    // Print most relevant features
    val strRels = MiAndCmi.collect().sortBy(-_._2._1)
      .take(nToSelect)
      .map({case ((f, _), (mi, _)) => f + "\t" + "%.4f" format mi})
      .mkString("\n")
    // println("\n*** MaxRel features ***\nFeature\tScore\n" + strRels)  
    // get maximum and select it
    val firstMax = pool.maxBy(_._2.score)
    var selected = Seq(F(firstMax._1, firstMax._2.score))
    pool = pool - firstMax._1

    while (selected.size < nToSelect) {
      // update pool
      val newMiAndCmi = IT.miAndCmi(data, pool.keys.toSeq, Seq(selected.head.feat), Some(label), 
        nElements, nFeatures) 
        .map({ case ((x, _), crit) => (x, crit) })
        .collectAsMap()
      pool.foreach({ case (k, crit) =>
        newMiAndCmi.get(k) match {
          case Some((mi, cmi)) => crit.update(mi, cmi)
          case None => 
        }
      })

      // get maximum and save it
      val max = pool.maxBy(_._2.score)
      // select the best feature and remove from the whole set of features
      selected = F(max._1, max._2.score) +: selected
      pool = pool - max._1
    }    
    selected.reverse
  }
   
   
 /**
   * Perform a info-theory selection process with pool optimization.
   * 
   * @param data Data points (first element is the class attribute).
   * @param nToSelect Number of features to select.
   * @param poolSize Initial pool size (also used as pool increment).
   * @return A list with the most relevant features and its scores.
   * 
   */
  private[feature] def selectFeaturesWithPool(data: RDD[BV[Byte]],nToSelect: Int,poolSize: Int) = {
    
    val label = 0
    val nElements = data.count()
    
    // calculate relevance for all attributes
    var orderedRels = IT.miAndCmi(data,1 to nFeatures, Seq(label), None, nElements, nFeatures)
      .collect
      .map({ case ((k, _), (mi, _)) => (k, mi) })
      .sortBy(-_._2)
        
    // Print most relevant features
    val strRels = orderedRels.take(nToSelect)
      .map({case (f, c) => f + "\t" + "%.4f" format c}).mkString("\n")
    // println("\n*** MaxRel features ***\nFeature\tScore\n" + strRels)
  
    // extract initial pool
    val initialPoolSize = math.min(math.max(poolSize, nToSelect), orderedRels.length)
    var pool = orderedRels.take(initialPoolSize).map({ case (k, mi) =>
      (k, criterionFactory.getCriterion.init(mi))
    })
    .toMap
    orderedRels = orderedRels.drop(initialPoolSize)

    // select the best feature (those with the maximum relevance)
    var max = pool.maxBy(_._2.score)
    var selected = Seq(F(max._1, max._2.score))
    pool = pool - max._1

    while (selected.size < nToSelect) {
      // update pool
      val newMiAndCmi = IT.miAndCmi(data, pool.keys.toSeq, Seq(selected.head.feat), 
        Some(label), nElements, nFeatures)
        .collectAsMap()
        .map({ case ((x, _), crit) => (x, crit) })
            
      pool.foreach({ case (k, crit) => newMiAndCmi.get(k) match {
          case Some((mi, cmi)) => crit.update(mi, cmi)
          case None => 
        }
      })
            
      // look for maximum and bound
      max = pool.maxBy(_._2.score)
      var min = pool.minBy(_._2.relevance)._2.asInstanceOf[InfoThCriterion with Bound]
      
      // increase pool if necessary
      while (max._2.score < min.bound && orderedRels.size > 0) {                
        val realPoolSize = math.min(poolSize, orderedRels.length)        
        val newFeatures = orderedRels.take(realPoolSize)
          .map{ case (k, mi) => (k, criterionFactory.getCriterion.init(mi)) }
          .toMap
        
        // Do missed calculations (for each previously selected attribute)
        val missedMiAndCmi = IT.miAndCmi(data, newFeatures.keys.toSeq, 
          selected.map(_.feat), Some(label), nElements, nFeatures)
          .collect()          
              
        missedMiAndCmi.foreach{ case ((feat, _), (mi, cmi)) => 
          newFeatures.get(feat) match {
            case Some(crit) => crit.update(mi, cmi)
            case None =>
          }     
        }
        
        // Add new features to the pool and remove them from the whole set
        pool ++= newFeatures.toSeq
        orderedRels = orderedRels.drop(realPoolSize)        
        // Look for the maximum and the bound
        max = pool.maxBy(_._2.score)        
        min = pool.minBy(_._2.relevance)._2.asInstanceOf[InfoThCriterion with Bound]
      }
      
      // select the best feature
      selected = F(max._1, max._2.score) +: selected
      pool = pool - max._1
    }
    selected.reverse
  }  

  private[feature] def run(nToSelect: Int, poolSize: Int = 30) = {
    
    require(nToSelect < nFeatures)
    byteData.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    val selected = criterionFactory.getCriterion match {
      case _: InfoThCriterion with Bound if poolSize != 0 =>
        selectFeaturesWithPool(byteData, nToSelect, poolSize)
      case _: InfoThCriterion =>
        selectFeaturesWithoutPool(byteData, nToSelect)
    }
    
    byteData.unpersist()
    
    // Print best features according to the mRMR measure
    val out = selected.map{case F(feat, rel) => feat + "\t" + "%.4f".format(rel)}.mkString("\n")
    println("\n*** mRMR features ***\nFeature\tScore\n" + out)
    // Features must be sorted
    new SelectorModel(selected.map{case F(feat, rel) => feat - 1}.sorted.toArray)
  }
}

object InfoThSelector {

  /**
   * Train a feature selection model according to a given criterion
   * and return a subset of data.
   *
   * @param   criterionFactory Initialized criterion to use in this selector
   * @param   data RDD of LabeledPoint (discrete data in range of 256 values).
   * @param   nToSelect maximum number of features to select
   * @param   poolSize number of features to be used in pool optimization.
   * @return  A feature selection model which contains a subset of selected features
   * 
   * Note: LabeledPoint data must be integer values in double representation 
   * with a maximum of 256 distinct values. In this manner, data can be transformed
   * to byte class directly, making the selection process much more efficient. 
   * 
   */
  def train(
      criterionFactory: FT, 
      data: RDD[LabeledPoint],
      nToSelect: Int = 100,
      poolSize: Int = 100) = {
    new InfoThSelector(criterionFactory, data).run(nToSelect, poolSize)
  }
}
