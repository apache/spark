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
  def computeHidden( data: Array[Double], weights: Array[Double] ): Array[Double] = {

    var arrHidden = new Array[Double]( noHidden + 1 )

    for( j <- 0 to noHidden-1 ) {

      val start = j*(noInput + 1)
      var v: Double = 0;
      for( w <- 0 to noInput-1 )
        v = v + data(w)*weights( start + w )
      v = v - 1.0 * weights( start + noInput ) // robonode
      arrHidden( j ) = g( v )

    }

    arrHidden( noHidden ) = -1.0

    arrHidden

  }

  /* returns the hidden layer including the -1 robonode, as well as the final estimation */
  def computeValues( 
      data: Array[Double], 
      weights: Array[Double] ): 
      (Array[Double], Array[Double]) = {

    var hidden = computeHidden( data, weights )
    var output = new Array[Double](noOutput)

    for( k <- 0 to noOutput - 1 ) {
      var tmp: Double = 0.0;
      for( i <- 0 to noHidden )
        tmp = tmp + hidden(i)*weights( noHidden * ( noInput + 1 ) + k * ( noHidden + 1 ) + i )
      output(k) = g( tmp )

    }

    ( hidden, output )

  }

}

class ParallelANNModel private[mllib] (
    override val weights: Vector,
    val noInp: Integer,
    val noHid: Integer,
    val noOut: Integer,
    val b: Double )
  extends GeneralizedSteepestDescentModel(weights) with RegressionModel with Serializable with ANN {

  val noInput = noInp
  val noHidden = noHid
  val noOutput = noOut
  val beta = b

  override def predictPoint( data: Vector, weights: Vector ): Double = {
    val outp = computeValues( data.toArray, weights.toArray )._2
    outp(0)
  }

  def predictPointV( data: Vector, weights: Vector): Vector = {
    Vectors.dense( computeValues( data.toArray, weights.toArray )._2 )
  }

}

class ParallelANN private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double,
    private var noInput: Int,
    private var noHidden: Int,
    private var noOutput: Int,
    private val beta: Double )
  extends GeneralizedSteepestDescentAlgorithm[ParallelANNModel] with Serializable {

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

  def train( rdd: RDD[(Vector,Vector)], weights: Vector ): ParallelANNModel = {

    val ft = rdd.first()
    assert( noInput == ft._1.size )
    assert( noOutput == ft._2.size )
    assert( weights.size == (noInput + 1) * noHidden + (noHidden + 1) * noOutput )
    run( rdd, weights );

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

  /* For verification only
  private val rand = new XORShiftRandom
  */

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {

    val arrData = data.toArray
    val arrWeights = weights.toArray

    var gradient = new Array[Double]( (noInput + 1) * noHidden + (noHidden + 1) * noOutput )

    val (arrHidden, output) = computeValues( arrData, arrWeights )
    val arrEst = output

    var diff = new Array[Double]( noOutput )
    var E: Double = 0.0
    for( i <-0 to noOutput-1 ) {
      diff( i ) = arrEst( i ) - arrData( noInput.toInt + i );
      E = E + diff(i) * diff(i)
    }

    /*
     * The following fields are for verification only
    val eps = .000001
    val testOneVOutOf = 5000;
    val testOneWOutOf = 2500;
    var arrWeights_tmp = weights.toArray
    val warnErr = 5e-7
    */

    /* Wjk */
    for( k <- 0 to noOutput - 1 ) {

      var start = noHidden*(noInput + 1) + k*(noHidden + 1)
      var sum_l: Double = 0
      for( w <- 0 to noHidden )
         sum_l = sum_l +  arrHidden( w ) * arrWeights( w + start )
      val dg_sum_l = dg( sum_l )


      for( j <- 0 to noHidden ) {

        gradient( noHidden*(noInput + 1) + k*(noHidden + 1) + j )
          = 2*(diff(k))*dg_sum_l*arrHidden(j)

        /*
         * The following is for verification only
        if( rand.nextInt % (testOneWOutOf>>1) == 0 ) {
          arrWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j )
            = arrWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j ) + eps
          val est2 = computeValues( arrData, arrWeights_tmp )._2
          var E2: Double = 0.0;
          for( w <- 0 to noOutput-1 ) {
            val diff2 = est2(w)-data( noInput+w )
            E2 = E2 + diff2*diff2
          }
          val d = ( E2 - E ) / eps
          val compErr = math.abs( gradient( noHidden*(noInput+1)+k*(noHidden+1)+j) - d )
          if( compErr > warnErr ) {
            println( "!!! Calc/Est Wjk: " + 
              ( ( gradient( noHidden*(noInput+1)+k*(noHidden+1)+j), d ), compErr ) )
          }
          arrWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j )
            = arrWeights_tmp( noHidden*(noInput+1)+k*(noHidden+1)+j ) - eps
        }
        */

      }

    }

    var start = noHidden * (noInput + 1)
    var sum_n1: Double = 0
    for( w <- 0 to noHidden )
       sum_n1 = sum_n1 + arrHidden( w )*arrWeights( w + start )
    val dg_sum_n1 = dg( sum_n1 )


    /* Vij */
    for( j <- 0 to noHidden - 1 ) { /* the hidden robonode has no associated Vij */

      start = j * ( noInput + 1 )
      var sum_n2: Double = 0
      for( w <- 0 to noInput-1 ) // non-robonodes
         sum_n2 = sum_n2 + arrData( w )*arrWeights( w + start)
      sum_n2 = sum_n2 - arrWeights( noInput + start) // robonode
      val dg_sum_n2 = dg( sum_n2 )

      for( i <- 0 to noInput ) {


        for( k<- 0 to noOutput - 1 ) {

          if( i<noInput ) { // non-robonode
            gradient( i + j * (noInput + 1) ) =
            gradient( i + j * (noInput + 1) ) +
              2 * ( diff(k)  )* dg_sum_n1 *
              arrWeights( noHidden * (noInput + 1) + k * (noHidden + 1) + j ) *
              dg_sum_n2*arrData( i )

          }
          else { // robonode
            gradient( i + j * (noInput + 1) ) =
            gradient( i + j * (noInput + 1) ) -
              2 * ( diff(k) ) * dg_sum_n1 *
              arrWeights( noHidden * (noInput + 1) + k * (noHidden + 1) + j ) *
              dg_sum_n2
          }

        }

        /*
         * The following is for verification only
        if( rand.nextInt % (testOneVOutOf>>1) == 0 ) {
          arrWeights_tmp( i+j*(noInput+1) ) = arrWeights_tmp( i+j*(noInput+1) ) + eps
          val est2 = computeValues( arrData, arrWeights_tmp )._2

          var E2: Double = 0.0;
          for( w <- 0 to noOutput-1 ) {
            val diff2 = est2(w)-data( noInput+w )
            E2 = E2 + diff2*diff2
          }

          val d = ( E2 - E ) / eps
          val compErr = math.abs( gradient( i+j*(noInput+1) )-d )
          if( compErr>warnErr )
            println( "!!! Calc/Est Vij: "+ ( ( gradient( i+j*(noInput+1) ), d ), compErr ) )
          arrWeights_tmp( i+j*(noInput+1) ) = arrWeights_tmp( i+j*(noInput+1) ) - eps
        }
        */
      }
    }

    (Vectors.dense(gradient), E)

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
