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

package org.apache.spark.mllib.ann

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import breeze.linalg.{axpy => brzAxpy, Vector => BV}
import breeze.linalg.{Vector => BV}
import breeze.linalg.{axpy => brzAxpy}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.RegressionModel
import org.apache.spark.util.random.XORShiftRandom

/*
 * Implements a Artificial Neural Network (ANN)
 *
 * format of data:
 * data[ 0..noInput-1 ]: Input
 * data[ noInput..noInput+noOutput-1 ]: Output
 *
 */

trait ANN {

  def noInput: Integer
  def noHidden: Integer
  def noOutput: Integer
  def beta: Double

  def g( x: Double ) = (1/(1 + math.exp(-beta*x)))
  def dg( x: Double ) = beta*g(x)*(1 - g(x))

  /* returns the hidden layer including the -1 robonode! */
  def computeHidden( data: Vector, weights: Vector ): Vector = {

    val brzData = data.toBreeze
    val brzInp = DenseVector.vertcat( 
      brzData( 0 to noInput - 1 ).toDenseVector, DenseVector[Double](-1.0) )
    val brzWeights = weights.toBreeze
    var hidden = DenseVector.zeros[Double]( noHidden + 1 )

    for( j <- 0 to noHidden-1 ) {

      val weightsSubset = brzWeights( 
        j*(noInput + 1) to j*(noInput + 1) + (noInput + 1) - 1 ).toVector
      hidden( j ) = g( weightsSubset.dot( brzInp ) )

    }

    hidden( noHidden ) = -1.0

    Vectors.fromBreeze( hidden )

  }

  /* returns the hidden layer including the -1 robonode, as well as the final estimation */
  def computeValues( data: Vector, weights: Vector ): (Vector, Vector) = {

    var hidden = computeHidden( data, weights )
    var output = new Array[Double](noOutput)

    for( k <- 0 to noOutput - 1 ) {
      val brzWeights = weights.toBreeze
      var weightsSubset = brzWeights( noHidden*(noInput + 1) + k*(noHidden + 1) to
            noHidden*(noInput + 1) + (k + 1)*(noHidden + 1) - 1).toVector
      output(k) = g( weightsSubset.dot( hidden.toBreeze ) )
    }

    ( hidden, Vectors.dense( output ) )

  }

}

class ParallelANNModel private[mllib]
(
    override val weights: Vector,
    val noInp: Integer,
    val noHid: Integer,
    val noOut: Integer,
    val b: Double )
  extends GeneralizedSteepestDescendModel(weights) with RegressionModel with Serializable with ANN {

  val noInput = noInp
  val noHidden = noHid
  val noOutput = noOut
  val beta = b

  override def predictPoint( data: Vector, weights: Vector ): Double = {
    val outp = computeValues( data, weights )._2
    outp.toArray(0)
  }

  def predictPointV( data: Vector, weights: Vector): Vector = {
    computeValues( data, weights )._2
  }

}

class ParallelANNWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double,
    private var noInput: Int,
    private var noHidden: Int,
    private var noOutput: Int,
    private val beta: Double )
  extends GeneralizedSteepestDescendAlgorithm[ParallelANNModel] with Serializable {

  private val rand = new XORShiftRandom

  private val gradient = new LeastSquaresGradientANN( noInput, noHidden, noOutput, beta )
  private val updater = new ANNUpdater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  def this() = {
    this( 1.0, 100, 1.0, 1, 5, 1, 1.0 )
  }

  def this( noHidden: Int ) = {
    this( 1.0, 100, 1.0, 1, noHidden, 1, 1.0 )
  }

  def this( noInput: Int, noHidden: Int ) = {
    this( 1.0, 100, 1.0, noInput, noHidden, 1, 1.0 )
  }

  def this( noInput: Int, noHidden: Int, noOutput: Int ) = {
    this( 1.0, 100, 1.0, noInput, noHidden, noOutput, 1.0 )
  }

  override protected def createModel(weights: Vector) = {
    new ParallelANNModel( weights, noInput, noHidden, noOutput, beta )
  }

  def checkOutput( rdd: RDD[(Vector,Vector)] ) {
    val oVals = rdd.flatMap( T => T._2.toArray )
    var omax = oVals.max
    assert( omax <= 1 )
    var omin = oVals.min
    assert( omin >= 0 )
  }

  def randomDouble( i: Int ): Double = {
    rand.nextDouble()
  }

  def train( rdd: RDD[(Vector,Vector)] ): ParallelANNModel = {

    val ft = rdd.first()

    assert( noInput == ft._1.size )
    assert( noOutput == ft._2.size )

    checkOutput( rdd )

    val noWeights = (noInput + 1)*noHidden + (noHidden + 1)*noOutput

    val initialWeightsArr = new Array[Double](noWeights)

    for( i <- 0 to (noInput + 1)*noHidden - 1 )
      initialWeightsArr( i ) = (randomDouble(i)*4.8 - 2.4)/(noInput + 1)
    for( i <- 0 to (noHidden + 1)*noOutput - 1 )
      initialWeightsArr( (noInput + 1)*noHidden + i ) = (randomDouble(i)*4.8 - 2.4)/(noHidden + 1)

    val initialWeights = Vectors.dense( initialWeightsArr )

    run( rdd, initialWeights )

  }

  def train( rdd: RDD[(Vector,Vector)], model: ParallelANNModel ): ParallelANNModel = {
    run( rdd, model.weights )
  }

}

/**
 * data consists of input vector and output vector, and has the following form:
 *
 * [ ---input--- ---output--- ]
 *
 * where input = data( 0 to noInput-1 ) and output = data( noInput to noInput+noOutput-1 )
 *
 * V_ij is the weight from input node i to hidden node j
 * W_jk is the weight from hidden node j to output node k
 *
 * The weights have the following mapping:
 *
 * V_ij goes to position i + j*(noInput+1)
 * W_jk goes to position (noInput+1)*noHidden + j + k*(noHidden+1)
 *
 * Gradient has same mapping, i.e.
 * dE/dVij goes to i + j*(noInput+1)
 * dE/dWjk goes to (noInput+1)*noHidden + j +k*(noHidden+1)
 *
 * Where E = ((estOutput-output),(estOutput-output)),
 * the inner product of the difference between estimation and target output with itself.
 */

class LeastSquaresGradientANN(
    noInp: Integer, 
    noHid: Integer, 
    noOut: Integer, 
    b: Double ) 
  extends Gradient with ANN {

  val noInput = noInp
  val noHidden = noHid
  val noOutput = noOut
  val beta = b

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {

    val brzData = data.toBreeze
    val brzInp = DenseVector.vertcat( brzData( 0 to noInput - 1 ).toDenseVector,
      DenseVector[Double](-1.0) )

    val brzOut = brzData( noInput.toInt to noInput + noOutput - 1 ).toVector
    val brzWeights = weights.toBreeze
    val gradient = DenseVector.zeros[Double]( (noInput + 1)*noHidden + (noHidden + 1)*noOutput )


    val (hidden, output) = computeValues( data, weights )
    var brzHidden = hidden.toBreeze /* already includes the robonode */
    val brzEst = output.toBreeze
    val diff = brzEst :- brzOut
    val E = diff.dot(diff)

    /*
     * The following three fields are for verification only
    val eps = .000001
    val noInpCheck = 0
    val noOutCheck = 0
    */

    var brzWeights_tmp = weights.toBreeze

    /* Wjk */
    for( j <- 0 to noHidden ) {

      for( k <- 0 to noOutput - 1 ) {

        val brzW = brzWeights( noHidden*(noInput + 1) + k*(noHidden + 1) to
          noHidden*(noInput + 1) + (k + 1)*(noHidden + 1) - 1 ).toVector
        var sum_l = brzHidden.dot( brzW )
        gradient( noHidden*(noInput + 1) + k*(noHidden + 1) + j )
          = 2*(diff(k))*dg(sum_l)*brzHidden(j)

        /*
         * The following is for verification only
        if( noInput==noInpCheck && noOutput==noOutCheck )
        {
        brzWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j )
          = brzWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j ) + eps
        val est2 = computeValues( data, Vectors.fromBreeze( brzWeights_tmp ) )._2.toBreeze
        val diff2 = est2 - brzOut
        val d = ( diff2.dot(diff2) - E ) / eps
        println( "Calc/Est Wjk: "+ ( gradient( noHidden*(noInput+1)+k*(noHidden+1)+j), d ) )
        brzWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j )
          = brzWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j ) - eps
        }
        */

      }

    }

    /* Vij */
    for( i <- 0 to noInput ) {

      for( j <- 0 to noHidden - 1 ) { /* the hidden robonode has no associated Vij */

        for( k<- 0 to noOutput - 1 ) {

          val brzW = brzWeights( noHidden*(noInput + 1) to
            noHidden*(noInput + 1) + (noHidden + 1) - 1 ).toVector
          val sum_n1 = brzHidden.dot( brzW )
          val brzV = brzWeights( j*(noInput + 1) to j*(noInput + 1) + (noInput + 1) - 1 ).toVector
          val sum_n2 = brzV.dot( brzInp )
          gradient( i + j*(noInput + 1) ) =
          gradient( i + j*(noInput + 1) ) 
            + 2*(diff(k))*dg( sum_n1 )*brzWeights( noHidden*(noInput + 1)
            + k*(noHidden + 1) + j )*dg( sum_n2 )*brzInp( i )
        }

        /*
         * The following is for verification only
        if( noInput==noInpCheck && noOutput==noOutCheck )
        {
          brzWeights_tmp( i+j*(noInput+1) ) = brzWeights_tmp( i+j*(noInput+1) ) + eps
          val est2 = computeValues( data, Vectors.fromBreeze( brzWeights_tmp ) )._2.toBreeze
          val diff2 = est2 - brzOut
          val d = ( diff2.dot( diff2 ) - E ) / eps
          println( "Calc/Est Vij: "+ ( gradient( i+j*(noInput+1) ), d ) )
          brzWeights_tmp( i+j*(noInput+1) ) = brzWeights_tmp( i+j*(noInput+1) ) - eps
        }
        */
      }
    }
    (Vectors.fromBreeze(gradient), E)

  }

  override def compute(
      data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector): Double = {

    val (grad, err) = compute( data, label, weights )

    cumGradient.toBreeze += grad.toBreeze

    return err

  }
}

class ANNUpdater extends Updater {

  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {

    val thisIterStepSize = stepSize

    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector

    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)

    (Vectors.fromBreeze(brzWeights), 0)
  }

}

class ParallelANN (

    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double,
    private var noInput: Int,
    private var noHidden: Int,
    private var noOutput: Int,
    private val beta: Double

  ) extends GeneralizedSteepestDescendAlgorithm[ParallelANNModel] with Serializable {

  private val gradient = new LeastSquaresGradientANN( noInput, noHidden, noOutput, beta )
  private val updater = new SimpleUpdater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  def this() = {
    this( 0.001, 100, 1.0, 1, 5, 1, 1.0 )
  }

  override protected def createModel(weights: Vector) = {
    new ParallelANNModel(weights, noInput, noHidden, noOutput, beta)
  }

}
