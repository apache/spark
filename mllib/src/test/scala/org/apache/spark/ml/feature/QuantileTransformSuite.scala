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

package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row

class QuantileTransformSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Array[Vector] = _
  @transient var res: Array[Vector] = _
  @transient var gaussianRes: Array[Vector] = _
  @transient var zeros: Array[Vector] = _
  @transient var dataWithNaN: Array[Vector] = _
  @transient var resWithNaN: Array[Vector] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    data = Array(
      Vectors.dense(0.5, 0.5),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(2.0, -2.0),
      Vectors.dense(3.0, -3.0),
      Vectors.dense(4.0, -4.0)
    )

    gaussianRes = Array(
      Vectors.dense(-5.19933758, 5.19933758),
      Vectors.dense(-0.96742157, 0.52440051),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.67448975, -0.67448975),
      Vectors.dense(5.19933758, -5.19933758)
    )

    res = Array(
      Vectors.dense(0.0, 1.0),
      Vectors.dense(0.16666667, 0.7),
      Vectors.dense(0.5, 0.5),
      Vectors.dense(0.75, 0.25),
      Vectors.dense(1.0, 0.0)
    )

    zeros = Array.fill(10)(Vectors.zeros(2).toSparse)

    dataWithNaN = Array(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(Double.NaN, -0.5),
      Vectors.dense(1.0, -1.0),
      Vectors.dense(1.5, Double.NaN),
      Vectors.dense(2.0, -2.0)
    )

    resWithNaN = Array(
      Vectors.dense(0.0, 1.0),
      Vectors.dense(Double.NaN, 0.66666667),
      Vectors.dense(0.33333333, 0.33333333),
      Vectors.dense(0.66666667, Double.NaN),
      Vectors.dense(1.0, 0.0)
    )
  }

  private def assertResult: Row => Unit = {
    case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1 ~== vector2 absTol 1E-5,
        "The vector value is not correct after transformation.")
  }

  test("params") {
    ParamsSuite.checkParams(new QuantileTransform)
    ParamsSuite.checkParams(new QuantileTransformModel("empty",
      Array(Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(4.0, 5.0, 6.0))))
  }

  test("uniform: fit & transform") {
  /*
    Python code:

    import numpy as np
    from sklearn.preprocessing import QuantileTransformer

    X = np.array([[0.5, 0.5], [1.0, -1.0], [2.0, -2.0], [3.0, -3.0], [4.0, -4.0]])
    qt = QuantileTransformer(n_quantiles=3)
    qtm = qt.fit(X)

    >>> qtm.references_
    array([0. , 0.5, 1. ])
    >>> qtm.quantiles_
    array([[ 0.5, -4. ],
           [ 2. , -2. ],
           [ 4. ,  0.5]])
    >>> qtm.transform(X)
    array([[0.        , 1.        ],
           [0.16666667, 0.7       ],
           [0.5       , 0.5       ],
           [0.75      , 0.25      ],
           [1.        , 0.        ]])
   */
    val df = data.zip(res).toSeq.toDF("features", "expected")
    val qt = new QuantileTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(3)
    val qtm = qt.fit(df)
    assert(qtm.references ~== Vectors.dense(0.0, 0.5, 1.0) absTol 1E-5)
    assert(qtm.quantiles(0) ~== Vectors.dense(0.5, 2.0, 4.0) absTol 1E-5)
    assert(qtm.quantiles(1) ~== Vectors.dense(-4.0, -2.0, 0.5) absTol 1E-5)
    testTransformer[(Vector, Vector)](df, qtm, "transformed", "expected")(
      assertResult)

    /*
      Python code:

      qt3 = QuantileTransformer(n_quantiles=3, ignore_implicit_zeros=False)
      qtm3 = qt3.fit(X2.toarray())

      >>> qtm3.references_
      array([0. , 0.5, 1. ])
      >>> qtm3.quantiles_
      array([[ 0. , -4. ],
             [ 0. ,  0. ],
             [ 4. ,  0.5]])
     */
    val df2 = (data ++ zeros).zip(res ++ zeros).toSeq.toDF("features", "expected")
    val qt2 = new QuantileTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(3)
    val qtm2 = qt2.fit(df2)
    assert(qtm2.references ~== Vectors.dense(0.0, 0.5, 1.0) absTol 1E-5)
    assert(qtm2.quantiles(0) ~== Vectors.dense(0.0, 0.0, 4.0) absTol 1E-5)
    assert(qtm2.quantiles(1) ~== Vectors.dense(-4.0, 0.0, 0.5) absTol 1E-5)
  }

  test("gaussian: fit & transform") {
    /*
      Python code:

      import numpy as np
      from sklearn.preprocessing import QuantileTransformer

      X = np.array([[0.5, 0.5], [1.0, -1.0], [2.0, -2.0], [3.0, -3.0], [4.0, -4.0]])
      qt = QuantileTransformer(n_quantiles=3, output_distribution="normal")
      qtm = qt.fit(X)

      >>> qtm.references_
      array([0. , 0.5, 1. ])
      >>> qtm.quantiles_
      array([[ 0.5, -4. ],
             [ 2. , -2. ],
             [ 4. ,  0.5]])
      >>> qtm.transform(X)
      array([[-5.19933758,  5.19933758],
             [-0.96742157,  0.52440051],
             [ 0.        ,  0.        ],
             [ 0.67448975, -0.67448975],
             [ 5.19933758, -5.19933758]])
     */
    val df = data.zip(gaussianRes).toSeq.toDF("features", "expected")
    val qt = new QuantileTransform()
      .setDistribution("gaussian")
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(3)
    val qtm = qt.fit(df)
    assert(qtm.references ~== Vectors.dense(0.0, 0.5, 1.0) absTol 1E-5)
    assert(qtm.quantiles(0) ~== Vectors.dense(0.5, 2.0, 4.0) absTol 1E-5)
    assert(qtm.quantiles(1) ~== Vectors.dense(-4.0, -2.0, 0.5) absTol 1E-5)
    testTransformer[(Vector, Vector)](df, qtm, "transformed", "expected")(
      assertResult)
  }

  test("deal with NaN") {
    /*
      Python code:

      import numpy as np
      from sklearn.preprocessing import QuantileTransformer

      X = np.array([[0.0, 0.0], [np.nan, -0.5], [1.0, -1.0], [1.5, np.nan], [2.0, -2.0]])
      qt = QuantileTransformer(n_quantiles=4)

      >>> qt.fit_transform(X)
      array([[0.        , 1.        ],
             [       nan, 0.66666667],
             [0.33333333, 0.33333333],
             [0.66666667,        nan],
             [1.        , 0.        ]])
     */
    val df = dataWithNaN.zip(resWithNaN).toSeq.toDF("features", "expected")
    val qt = new QuantileTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(4)
    val qtm = qt.fit(df)
    assert(qtm.references ~== Vectors.dense(0.0, 0.3333333, 0.6666667, 1.0) absTol 1E-5)
    assert(qtm.quantiles(0) ~== Vectors.dense(0.0, 1.0, 1.5, 2.0) absTol 1E-5)
    assert(qtm.quantiles(1) ~== Vectors.dense(-2.0, -1.0, -0.5, 0.0) absTol 1E-5)
    testTransformer[(Vector, Vector)](df, qtm, "transformed", "expected")(
      assertResult)
  }

  test("QuantileTransform read/write") {
    val qt = new QuantileTransform()
      .setInputCol("features")
      .setOutputCol("transformed")
      .setNumQuantiles(4)
    testDefaultReadWrite(qt)
  }

  test("QuantileTransformModel read/write") {
    val instance = new QuantileTransformModel("myQuantileTransformModel",
      Array(Vectors.dense(1.0, 2.0),
        Vectors.dense(3.0, 4.0)))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.quantiles === instance.quantiles)
  }
}
