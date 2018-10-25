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
package org.apache.spark.ml.optim.loss

import org.apache.spark.SparkFunSuite
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.optim.aggregator.DifferentiableLossAggregatorSuite.TestAggregator
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD

class RDDLossFunctionSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var instances: RDD[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    instances = sc.parallelize(Seq(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
      Instance(2.0, 0.3, Vectors.dense(4.0, 0.5))
    ))
  }

  test("regularization") {
    val coefficients = Vectors.dense(0.5, -0.1)
    val regLossFun = new L2Regularization(0.1, (_: Int) => true, None)
    val getAgg = (bvec: Broadcast[Vector]) => new TestAggregator(2)(bvec.value)
    val lossNoReg = new RDDLossFunction(instances, getAgg, None)
    val lossWithReg = new RDDLossFunction(instances, getAgg, Some(regLossFun))

    val (loss1, grad1) = lossNoReg.calculate(coefficients.asBreeze.toDenseVector)
    val (regLoss, regGrad) = regLossFun.calculate(coefficients)
    val (loss2, grad2) = lossWithReg.calculate(coefficients.asBreeze.toDenseVector)

    BLAS.axpy(1.0, Vectors.fromBreeze(grad1), regGrad)
    assert(regGrad ~== Vectors.fromBreeze(grad2) relTol 1e-5)
    assert(loss1 + regLoss === loss2)
  }

  test("empty RDD") {
    val rdd = sc.parallelize(Seq.empty[Instance])
    val coefficients = Vectors.dense(0.5, -0.1)
    val getAgg = (bv: Broadcast[Vector]) => new TestAggregator(2)(bv.value)
    val lossFun = new RDDLossFunction(rdd, getAgg, None)
    withClue("cannot calculate cost for empty dataset") {
      intercept[IllegalArgumentException]{
        lossFun.calculate(coefficients.asBreeze.toDenseVector)
      }
    }
  }

  test("versus aggregating on an iterable") {
    val coefficients = Vectors.dense(0.5, -0.1)
    val getAgg = (bv: Broadcast[Vector]) => new TestAggregator(2)(bv.value)
    val lossFun = new RDDLossFunction(instances, getAgg, None)
    val (loss, grad) = lossFun.calculate(coefficients.asBreeze.toDenseVector)

    // just map the aggregator over the instances array
    val agg = new TestAggregator(2)(coefficients)
    instances.collect().foreach(agg.add)

    assert(loss === agg.loss)
    assert(Vectors.fromBreeze(grad) === agg.gradient)
  }

}
