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
import org.apache.spark.SparkContext._ 
import org.apache.spark.broadcast.Broadcast

/**
 * Information Theory function and distributed primitives.
 */
object InfoTheory {

  private val log2 = { x: Double => math.log(x) / math.log(2) } 
  
  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n Number of elements
   * 
   */
  private[feature] def entropy(freqs: Seq[Long], n: Long) = {
    freqs.aggregate(0.0)({ case (h, q) =>
      h + (if (q == 0) 0  else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  private[feature] def entropy(freqs: Seq[Long]): Double = {
    entropy(freqs, freqs.reduce(_ + _))
  }

  private val createCombiner: ((Byte, Long)) => (Long, Long, Long, Long, Long, Long, Long) = {
    case (1, q) => (q, 0, 0, 0, 0, 0, 0)
    case (2, q) => (0, q, 0, 0, 0, 0, 0)
    case (3, q) => (0, 0, q, 0, 0, 0, 0)
    case (4, q) => (0, 0, 0, q, 0, 0, 0)
    case (5, q) => (0, 0, 0, 0, q, 0, 0)
    case (6, q) => (0, 0, 0, 0, 0, q, 0)
    case (7, q) => (0, 0, 0, 0, 0, 0, q)
  }

  private val mergeValues: ((Long, Long, Long, Long, Long, Long, Long), (Byte, Long)) => 
      (Long, Long, Long, Long, Long, Long, Long) = {
    case ((qxz, qyz, qxyz, qz, qxy, qx, qy), (ref, q)) =>
      ref match {
        case 1 => (qxz + q, qyz, qxyz, qz, qxy, qx, qy)
        case 2 => (qxz, qyz + q, qxyz, qz, qxy, qx, qy)
        case 3 => (qxz, qyz, qxyz + q, qz, qxy, qx, qy)
        case 4 => (qxz, qyz, qxyz, qz + q, qxy, qx, qy)
        case 5 => (qxz, qyz, qxyz, qz, qxy + q, qx, qy)
        case 6 => (qxz, qyz, qxyz, qz, qxy, qx + q, qy)
        case 7 => (qxz, qyz, qxyz, qz, qxy, qx, qy + q)
      }
  }

  private val mergeCombiners: (
      (Long, Long, Long, Long, Long, Long, Long), 
      (Long, Long, Long, Long, Long, Long, Long)) => 
      (Long, Long, Long, Long, Long, Long, Long) = {
    case ((qxz1, qyz1, qxyz1, qz1, qxy1, qx1, qy1), (qxz2, qyz2, qxyz2, qz2, qxy2, qx2, qy2)) =>
      (qxz1 + qxz2, qyz1 + qyz2, qxyz1 + qxyz2, qz1 + qz2, qxy1 + qxy2, qx1 + qx2, qy1 + qy2)
  }
  
  /* Pair generator for dense data */
  private def DenseGenerator(
      v: BV[Byte], 
      varX: Broadcast[Seq[Int]],
      varY: Broadcast[Seq[Int]],
      varZ: Broadcast[Option[Int]]) = {
    
     val dv = v.asInstanceOf[BDV[Byte]]
     val zval = varZ.value match {case Some(z) => Some(v(z)) case None => None}     
     var pairs = Seq.empty[((Any, Byte, Byte, Option[Byte]), Long)]
     val multY = varY.value.length > 1
     
     for(xind <- varX.value){
       for(yind <- varY.value) {
         val indexes = if(multY) (xind, yind) else xind
         pairs = ((indexes, dv(xind), dv(yind), zval), 1L) +: pairs
       }
     }     
     pairs
  } 
    
  
  /**
   * Method that calculates mutual information (MI) and conditional mutual information (CMI) 
   * simultaneously for several variables. Indexes must be disjoint.
   *
   * @param data RDD of data (first element is the class attribute)
   * @param varX Indexes of primary variables (must be disjoint with Y and Z)
   * @param varY Indexes of secondary variable (must be disjoint with X and Z)
   * @param varZ Indexes of conditional variable (must be disjoint  with X and Y)
   * @param n    Number of instances
   * @return     RDD of (primary var, (MI, CMI))
   * 
   */
  def miAndCmi(
      data: RDD[BV[Byte]],
      varX: Seq[Int],
      varY: Seq[Int],
      varZ: Option[Int],
      n: Long,      
      nFeatures: Int, 
      inverseX: Boolean = false) = {
    
    // Pre-requisites
    require(varX.size > 0 && varY.size > 0)

    // Broadcast variables
    val sc = data.context
    val bvarX = sc.broadcast(varX)
    val bvarY = sc.broadcast(varY)
    val bvarZ = sc.broadcast(varZ)
    
    // Common function to generate pairs, it choose between sparse and dense processing 
    data.first match {
      case v: BDV[Byte] =>
        val generator = DenseGenerator(_: BV[Byte], bvarX, bvarY, bvarZ)
        calculateMIDenseData(data, generator, varY(0), n)
      case v: BSV[Byte] =>     
        // Not implemented yet!
        throw new NotImplementedError()
    }
  }
  
  /**
   * Submethod that calculates MI and CMI for dense data.
   *
   * @param data RDD of data (first element is the class attribute)
   * @param pairsGenerator Function that generates the combinations between variables
   * @param firstY First Y index
   * @param n    Number of instances
   * @return     RDD of (primary var, (MI, CMI))
   * 
   */
  private def calculateMIDenseData(
      data: RDD[BV[Byte]],
      pairsGenerator: BV[Byte] => Seq[((Any, Byte, Any, Option[Byte]), Long)],
      firstY: Int,
      n: Long) = {

    val combinations = data.flatMap(pairsGenerator).reduceByKey(_ + _)
    // Split each combination keeping instance keys
      .flatMap {
      case ((k, x, y, Some(z)), q) =>          
        Seq(((k, 1:Byte /* "xz" */ , (x, z)),    (Set(y), q)),
            ((k, 2:Byte /* "yz" */ , (y, z)),    (Set(x), q)),
            ((k, 3:Byte /* "xyz" */, (x, y, z)), (Set.empty, q)),
            ((k, 4:Byte /* "z" */  , z),         (Set((x, y)), q)),
            ((k, 5:Byte /* "xy" */ , (x, y)),    (Set.empty,  q)),
            ((k, 6:Byte /* "x" */  , x),         (Set(y), q)),
            ((k, 7:Byte /* "y" */  , y),         (Set(x), q)))
      case ((k, x, y, None), q) =>
        Seq(((k, 5:Byte /* "xy" */ , (x, y)),    (Set.empty,  q)),
            ((k, 6:Byte /* "x" */  , x),         (Set(y), q)),
            ((k, 7:Byte /* "y" */  , y),         (Set(x), q)))
    }

    // Count frequencies for each combination
    val grouped_frequencies = combinations.reduceByKey({
      case ((keys1, q1), (keys2, q2)) => (keys1 ++ keys2, q1 + q2)
    })      
    // Separate by origin of combinations
    .flatMap({
      case ((k, ref, value), (keys, q))  => 
          val none: Option[Byte] = None
          ref match {
            case 1 => 
              val (x, z) = value.asInstanceOf[(Byte, Byte)]
              for (y <- keys) yield ((k, x, y.asInstanceOf[Byte], Some(z)), (1:Byte, q))
            case 2 =>
              val (y, z) = value.asInstanceOf[(Byte, Byte)]
              for (x <- keys) yield ((k, x.asInstanceOf[Byte], y, Some(z)), (2:Byte, q))
            case 3 =>
              val (x, y, z) = value.asInstanceOf[(Byte, Byte, Byte)]
              Seq(((k, x, y, Some(z)), (3:Byte, q)))
            case 4 =>
              val z = value.asInstanceOf[Byte]
              for ((x, y) <- keys) yield 
                ((k, x.asInstanceOf[Byte], y.asInstanceOf[Byte], Some(z)), (4:Byte, q))
            case 5 =>
                val (x, y) = value.asInstanceOf[(Byte, Byte)]
                Seq(((k, x, y, none), (5:Byte, q)))
            case 6 =>
              val x = value.asInstanceOf[Byte]
              for (y <- keys) yield ((k, x, y.asInstanceOf[Byte], none), (6:Byte, q))
            case 7 =>
              val y = value.asInstanceOf[Byte]
              for (x <- keys) yield ((k, x.asInstanceOf[Byte], y, none), (7:Byte, q))
          }        
    })
    // Group by origin
    .combineByKey(createCombiner, mergeValues, mergeCombiners)

    // Calculate MI and CMI by instance
    grouped_frequencies.map({case ((k, _, _, z), (qxz, qyz, qxyz, qz, qxy, qx, qy)) =>
    // Select id
      val finalKey = k match {
        case (kx: Int, ky: Int) => (kx, ky)
        case kx: Int => (kx, firstY)
      }           
      // Choose between MI or CMI
      z match {
        case Some(_) =>
          val pz = qz.toDouble / n
          val pxyz = (qxyz.toDouble / n) / pz
          val pxz = (qxz.toDouble / n) / pz
          val pyz = (qyz.toDouble / n) / pz
          (finalKey, (0.0, pz * pxyz * log2(pxyz / (pxz * pyz))))
        case None => 
          val pxy = qxy.toDouble / n
          val px = qx.toDouble / n
          val py = qy.toDouble / n
          (finalKey, (pxy * log2(pxy / (px * py)), 0.0))
      }
    })
    // Compute the final result by attribute
    .reduceByKey({ case ((mi1, cmi1), (mi2, cmi2)) => (mi1 + mi2, cmi1 + cmi2) })
  }
}
