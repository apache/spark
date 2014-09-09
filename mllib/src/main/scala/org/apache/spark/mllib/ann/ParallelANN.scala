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
import org.apache.spark.mllib.regression.{LabeledPoint, RegressionModel}
import org.apache.spark.util.random.XORShiftRandom

/*
 * Implements a Artificial Neural Network (ANN)
 *
 * The data consists of an input vector and an output vector, combined into a single vector
 * as follows:
 *
 * [ ---input--- ---output--- ]
 *
 * NOTE: output values should be in the range [0,1]
 *
 * For a network of L layers:
 *
 * topology( l ) indicates the number of nodes in layer l, excluding the bias node.
 *
 * noInput = topology(0), the number of input nodes
 * noOutput = topology(L-1), the number of output nodes
 *
 * input = data( 0 to noInput-1 )
 * output = data( noInput to noInput+noOutput-1 )
 *
 * W_ijl is the weight from node i in layer l-1 to node j in layer l
 * W_ijl goes to position ofsWeight(l) + j*(topology(l-1)+1) + i in the weights vector
 *
 * B_jl is the bias input of node j in layer l
 * B_jl goes to position ofsWeight(l) + j*(topology(l-1)+1) + topology(l-1) in the weights vector
 *
 * error function: E( O, Y ) = sum( O_j - Y_j )
 * (with O = (O_0, ..., O_(noOutput-1)) the output of the ANN,
 * and (Y_0, ..., Y_(noOutput-1)) the input)
 *
 * node_jl is node j in layer l
 * node_jl goes to position ofsNode(l) + j
 *
 * The weights gradient is defined as dE/dW_ijl and dE/dB_jl
 * It has same mapping as W_ijl and B_jl
 *
 * For back propagation:
 * delta_jl = dE/dS_jl, where S_jl the output of node_jl, but before applying the sigmoid
 * delta_jl has the same mapping as node_jl
 *
 * Where E = ((estOutput-output),(estOutput-output)),
 * the inner product of the difference between estimation and target output with itself.
 *
 */

class ParallelANNModel private[mllib] (
    val weights: Vector,
    val topology: Array[Int] )
  extends Serializable {

  private val L = topology.length - 1

  private val ofsWeight: Array[Int] = {
    val tmp = new Array[Int](L + 1)
    var curPos = 0;
    tmp( 0 ) = 0;
    for( l <- 1 to L ) {
      tmp( l ) = curPos
      curPos = curPos + ( topology( l - 1 ) + 1 ) * ( topology( l ) )
    }
    tmp
  }

  private def g( x: Double ) = 1.0 / (1.0 + math.exp( -x ) )

  def computeValues( arrData: Array[Double], arrWeights: Array[Double] ): Array[Double] = {

    var arrPrev = new Array[Double]( topology( 0 ) )

    for( i <- 0 until topology( 0 ) )
      arrPrev( i ) = arrData( i )

    for( l <- 1 to L ) {
      val arrCur = new Array[Double]( topology( l ) )
      for( j <- 0 until topology( l ) ) {
        var cum: Double = 0.0
        for( i <-0 until topology( l-1 ) )
          cum = cum +
            arrPrev( i ) * arrWeights( ofsWeight( l ) + ( topology( l-1 ) + 1 ) * j + i )
        cum = cum +
          arrWeights( ofsWeight( l ) + ( topology( l-1 ) + 1 )*j + topology( l-1 ) ) // bias
        arrCur( j ) = g( cum )
      }
      arrPrev = arrCur;
    }

    arrPrev

  }

  def predictPoint( data: Vector, weights: Vector ): Double = {
    val outp = computeValues( data.toArray, weights.toArray )
    outp(0)
  }

  def predictPointV( data: Vector, weights: Vector): Vector = {
    Vectors.dense( computeValues( data.toArray, weights.toArray ) )
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Vector prediction from the trained model
   *
   * Returns the complete vector.
   */
  def predictV( testData: Vector ): Vector = {

    predictPointV( testData, weights )

  }


}

class ParallelANN(
    private var topology: Array[Int],
    private var numIterations: Int,
    private var stepSize: Double,
    private var miniBatchFraction: Double )
  extends Serializable {

  private val rand = new XORShiftRandom

  private val gradient = new LeastSquaresGradientANN( topology )
  private val updater = new ANNUpdater()
  val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  val noWeights = {

    var tmp = 0

    for( i<-1 until topology.size ) {
      tmp = tmp + topology(i) * (topology(i-1) + 1)
    }

    tmp

  }

  def this( topology: Array[Int] ) = {
    this( topology, 100, 1.0, 1.0 )
  }

  def this( noInput: Int, noHidden: Int, noOutput: Int ) = {
    this( Array( noInput, noHidden, noOutput ) )
  }

  protected def createModel( weights: Vector ) = {
    new ParallelANNModel( weights, topology )
  }

  def train( rdd: RDD[(Vector,Vector)] ): ParallelANNModel = {

    val ft = rdd.first()

    assert( topology( 0 ) == ft._1.size )
    assert( topology( topology.length-1 ) == ft._2.size )

    val initialWeightsArr = new Array[Double](noWeights)

    var pos = 0;

    for( l <- 1 until topology.length ) {
      for( i <- 0 until ( topology( l ) * ( topology( l - 1 ) + 1 ) ) ) {
        initialWeightsArr( pos ) = ( rand.nextDouble * 4.8 - 2.4 ) / ( topology( l - 1 ) + 1)
      pos = pos + 1;
      }
    }

    assert( pos == noWeights )

    val initialWeights = Vectors.dense( initialWeightsArr )

    run( rdd, initialWeights )

  }

  def train( rdd: RDD[(Vector,Vector)], model: ParallelANNModel ): ParallelANNModel = {

    run( rdd, model.weights )

  }

  def train( rdd: RDD[(Vector,Vector)], weights: Vector ): ParallelANNModel = {

    val ft = rdd.first()
    assert( weights.size == noWeights )
    run( rdd, weights );

  }

  private def run(input: RDD[(Vector,Vector)], initialWeights: Vector): ParallelANNModel = {

    val data = input.map( v => (
      (0.0).toDouble,
      Vectors.fromBreeze( DenseVector.vertcat(
        v._1.toBreeze.toDenseVector,
        v._2.toBreeze.toDenseVector ) )
      ) )
    val weights = optimizer.optimize(data, initialWeights)
    createModel( weights )
  }

}

object ParallelANN {

  def train(
             input: RDD[(Vector,Vector)],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double,
             initialWeights: Vector): ParallelANNModel = {
    null
  }

}


class LeastSquaresGradientANN(
    topology: Array[Int] )
  extends Gradient {

  private def g( x: Double ) = 1.0 / (1.0 + math.exp( -x ) )

  private val L = topology.length - 1

  private val noWeights = {
    var tmp = 0
    for( i<-1 to L ) {
      tmp = tmp + topology(i) * ( topology( i - 1 ) + 1 )
    }
    tmp
  }

  val ofsWeight: Array[Int] = {

    var tmp = new Array[Int]( L + 1 )
    var curPos = 0;

    tmp( 0 ) = 0;
    for( l <- 1 to L ) {
      tmp( l ) = curPos
      curPos = curPos + ( topology( l - 1 ) + 1 ) * ( topology( l ) )
    }

    tmp

  }

  val noNodes: Int = {

    var tmp: Integer = 0

    for( l <-0 until topology.size ) {
      tmp = tmp + topology( l )
    }

    tmp

  }

  val ofsNode: Array[Int] = {

    var tmp = new Array[Int]( L + 1 )
    tmp( 0 ) = 0

    for( l <-1 to L ) {
      tmp( l ) = tmp( l - 1 ) + topology( l - 1 )
    }

    tmp

  }

  /* For verification only
  def calcErr( arrData: Array[Double], arrWeights: Array[Double] ): Double = {

    var arrPrev = new Array[Double]( topology( 0 ) )

    for( i <- 0 until topology( 0 ) )
      arrPrev( i ) = arrData( i )

    for( l <- 1 to L ) {
      val arrCur = new Array[Double]( topology( l ) )
      for( j <- 0 until topology( l ) ) {
        var cum: Double = 0.0
        for( i <-0 until topology( l-1 ) ) {
          cum = cum +
            arrPrev( i ) * arrWeights( ofsWeight( l ) + ( topology( l-1 ) + 1 ) * j + i )
        }
        cum = cum +
          arrWeights( ofsWeight( l ) + ( topology( l-1 ) + 1 )*j + topology( l-1 ) ) // bias
        arrCur( j ) = g( cum )
      }
      arrPrev = arrCur;
    }

    val arrDiff = new Array[Double]( topology( L ) )
    for( j <- 0 until topology( L ) ) {
      arrDiff( j ) = ( arrPrev( j ) - arrData( topology(0) + j ) )
    }

    var err: Double = 0;
    for( j <-0 until topology( L ) ) {
      err = err + arrDiff( j )*arrDiff( j )
    }

    err*.5
  }
  */

  override def compute( data: Vector, label: Double, weights: Vector ): ( Vector, Double ) = {

    val arrData = data.toArray
    val arrWeights = weights.toArray
    val arrNodes = new Array[Double]( noNodes )

    /*
     * nodes
     */

    for( i <- 0 until topology( 0 ) ) {
      arrNodes( i ) = arrData( i )
    }

    for( l <- 1 to L ) {
      for( j <- 0 until topology( l ) ) {
        var cum: Double = 0.0;
        for( i <- 0 until topology( l-1 ) ) {
          cum = cum +
            arrWeights( ofsWeight( l ) + ( topology( l-1 ) + 1 ) * j + i ) *
            arrNodes( ofsNode( l-1 ) + i )
        }
        cum = cum + arrWeights( ofsWeight( l ) + ( topology( l-1 ) +  1 )*j + topology( l-1 ) )
        arrNodes( ofsNode( l ) + j ) = g( cum )
      }
    }

    val arrDiff = new Array[Double]( topology( L ) )
    for( j <- 0 until topology( L ) ) {
      arrDiff( j ) = ( arrNodes( ofsNode( L ) + j ) - arrData( topology(0) + j ) )
    }

    var err: Double = 0;
    for( j <-0 until topology( L ) ) {
      err = err + arrDiff( j )*arrDiff( j )
    }
    err = err*.5

    /*
     * back propagation
     */

    val arrDelta = new Array[Double]( noNodes )

    for( j <- 0 until topology( L ) ) {
      arrDelta( ofsNode( L ) + j ) =
        arrDiff( j ) *
        arrNodes( ofsNode( L ) + j ) * ( 1 - arrNodes( ofsNode( L ) + j ) )
    }

    for( l <- L-1 until 0 by -1 ) {
      for( j <- 0 until topology( l ) ) {
        var cum: Double = 0.0
        for( i <- 0 until topology( l + 1 ) ) {
          cum = cum +
            arrWeights( ofsWeight( l + 1 ) + ( topology( l ) + 1 ) * i + j ) *
            arrDelta( ofsNode( l + 1 ) + i )  *
            arrNodes( ofsNode( l ) + j ) * ( 1 - arrNodes( ofsNode( l ) + j ) )
        }
        arrDelta( ofsNode( l ) + j ) = cum
      }
    }

    /*
     * gradient
     */

    /* for verification only
    val arrWcopy = new Array[Double]( noWeights )
    Array.copy(arrWeights, 0, arrWcopy, 0, noWeights )
    val eps = 0.000001
    val errGradAccept = 5e-6
    */

    val arrGrad = new Array[Double]( noWeights )

    for( l <- 1 to L ) {
      for( j <-0 until topology( l ) ) {
        for( i <- 0 until topology( l-1 ) ) {
          arrGrad( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + i ) =
            arrNodes( ofsNode( l - 1 ) + i ) *
            arrDelta( ofsNode( l ) + j )

          /* for verification only
          val tmpErr0 = calcErr( arrData, arrWcopy )
          arrWcopy( ofsWeight( l ) + ( topology( l -1 ) + 1 ) * j + i ) =
            arrWcopy( ofsWeight( l ) + ( topology( l -1 ) + 1 ) * j + i ) + eps
          val tmpErr1 = calcErr( arrData, arrWcopy )
          arrWcopy( ofsWeight( l ) + ( topology( l -1 ) + 1 ) * j + i ) =
            arrWcopy( ofsWeight( l ) + ( topology( l -1 ) + 1 ) * j + i ) - eps
          val dE = ( tmpErr1 - tmpErr0 ) / eps

          val errGrad =
            math.abs( dE - arrGrad( ofsWeight( l ) +
            ( topology( l - 1 ) + 1 ) * j + i ) )

          try {
            assert( errGrad < errGradAccept )
          }
          catch {
            case e: AssertionError =>
              println( (dE, arrGrad( ofsWeight( l ) +
                ( topology( l - 1 ) + 1 ) * j + i ), errGrad ) )
          }
          */
        }

        arrGrad( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 )  ) =
          arrDelta( ofsNode( l ) + j )

        /* for verification only
        val tmpErr0 = calcErr( arrData, arrWcopy )
        arrWcopy( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ) =
          arrWcopy( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ) + eps
        val tmpErr1 = calcErr( arrData, arrWcopy )
        arrWcopy( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ) =
          arrWcopy( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ) - eps
        val dE = ( tmpErr1 - tmpErr0 ) / eps

        val errGrad = math.abs( dE -
          arrGrad( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ) )

        try {
          assert( errGrad < errGradAccept )
        }
        catch {
          case e: AssertionError =>
            println( (dE,
              arrGrad( ofsWeight( l ) + ( topology( l - 1 ) + 1 ) * j + topology( l-1 ) ),
              errGrad ) )
        }
        */

      }
    }

    ( Vectors.dense( arrGrad ), err )

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
