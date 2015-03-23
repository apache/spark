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

package org.apache.spark.ml.recommendation

import java.util.Random
import breeze.linalg.{DenseVector => BrzVector, DenseMatrix => BrzMatrix, sum, norm, upperTriangular}
import breeze.numerics._
import breeze.optimize.proximal.QuadraticMinimizer
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.scalatest.FunSuite

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.ml.recommendation.ALS._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import breeze.optimize.proximal.Constraint._

class ALSSuite extends FunSuite with MLlibTestSparkContext with Logging {

  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  test("LocalIndexEncoder") {
    val random = new Random
    for (numBlocks <- Seq(1, 2, 5, 10, 20, 50, 100)) {
      val encoder = new LocalIndexEncoder(numBlocks)
      val maxLocalIndex = Int.MaxValue / numBlocks
      val tests = Seq.fill(5)((random.nextInt(numBlocks), random.nextInt(maxLocalIndex))) ++
        Seq((0, 0), (numBlocks - 1, maxLocalIndex))
      tests.foreach { case (blockId, localIndex) =>
        val err = s"Failed with numBlocks=$numBlocks, blockId=$blockId, and localIndex=$localIndex."
        val encoded = encoder.encode(blockId, localIndex)
        assert(encoder.blockId(encoded) === blockId, err)
        assert(encoder.localIndex(encoded) === localIndex, err)
      }
    }
  }

  test("normal equation construction with explict feedback") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 3.0f)
      .add(Array(4.0f, 5.0f), 6.0f)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 2)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5")
    // b = np.matrix("3; 6")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(17.0, 22.0, 29.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(27.0, 36.0) relTol 1e-8)

    val ne1 = new NormalEquation(2)
      .add(Array(7.0f, 8.0f), 9.0f)
    ne0.merge(ne1)
    assert(ne0.n === 3)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5; 7 8")
    // b = np.matrix("3; 6; 9")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(66.0, 78.0, 93.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(90.0, 108.0) relTol 1e-8)

    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f), 2.0f)
    }
    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f, 2.0f, 3.0f), 4.0f)
    }
    intercept[IllegalArgumentException] {
      val ne2 = new NormalEquation(3)
      ne0.merge(ne2)
    }

    ne0.reset()
    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))
  }

  test("normal equation construction with implicit feedback") {
    val k = 2
    val alpha = 0.5
    val ne0 = new NormalEquation(k)
      .addImplicit(Array(-5.0f, -4.0f), -3.0f, alpha)
      .addImplicit(Array(-2.0f, -1.0f), 0.0f, alpha)
      .addImplicit(Array(1.0f, 2.0f), 3.0f, alpha)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 0) // addImplicit doesn't increase the count.
    // NumPy code that computes the expected values:
    // alpha = 0.5
    // A = np.matrix("-5 -4; -2 -1; 1 2")
    // b = np.matrix("-3; 0; 3")
    // b1 = b > 0
    // c = 1.0 + alpha * np.abs(b)
    // C = np.diag(c.A1)
    // I = np.eye(3)
    // ata = A.transpose() * (C - I) * A
    // atb = A.transpose() * C * b1
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(39.0, 33.0, 30.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(2.5, 5.0) relTol 1e-8)
  }

  test("CholeskySolver") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 4.0f)
      .add(Array(1.0f, 3.0f), 9.0f)
      .add(Array(1.0f, 4.0f), 16.0f)
    val ne1 = new NormalEquation(k)
      .merge(ne0)

    val chol = new CholeskySolver
    val x0 = chol.solve(ne0, 0.0).map(_.toDouble)
    // NumPy code that computes the expected solution:
    // A = np.matrix("1 2; 1 3; 1 4")
    // b = b = np.matrix("3; 6")
    // x0 = np.linalg.lstsq(A, b)[0]
    assert(Vectors.dense(x0) ~== Vectors.dense(-8.333333, 6.0) relTol 1e-6)

    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))

    val x1 = chol.solve(ne1, 0.5).map(_.toDouble)
    // NumPy code that computes the expected solution, where lambda is scaled by n:
    // x0 = np.linalg.solve(A.transpose() * A + 0.5 * 3 * np.eye(2), A.transpose() * b)
    assert(Vectors.dense(x1) ~== Vectors.dense(-0.1155556, 3.28) relTol 1e-6)
  }

  test("QuadraticSolver without proximal operator") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 4.0f)
      .add(Array(1.0f, 3.0f), 9.0f)
      .add(Array(1.0f, 4.0f), 16.0f)
    val ne1 = new NormalEquation(k)
      .merge(ne0)

    val qm = new QuadraticSolver(2, SMOOTH)
    val x0 = qm.solve(ne0, 0.0).map(_.toDouble)
    // NumPy code that computes the expected solution:
    // A = np.matrix("1 2; 1 3; 1 4")
    // b = b = np.matrix("3; 6")
    // x0 = np.linalg.lstsq(A, b)[0]
    assert(Vectors.dense(x0) ~== Vectors.dense(-8.333333, 6.0) relTol 1e-6)

    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))

    val x1 = qm.solve(ne1, 0.5).map(_.toDouble)
    // NumPy code that computes the expected solution, where lambda is scaled by n:
    // x0 = np.linalg.solve(A.transpose() * A + 0.5 * 3 * np.eye(2), A.transpose() * b)
    assert(Vectors.dense(x1) ~== Vectors.dense(-0.1155556, 3.28) relTol 1e-6)
  }

  test("QuadraticSolver with POSITIVE constraint") {
    val n = 5
    val ata = Array(
      4.377,
      -3.531, 4.344,
      -1.306, 0.934, 2.644,
      -0.139, 0.305, -0.203, 5.883,
      3.418, -2.140, -0.170, 1.428, 4.684)
    val atb: Array[Double] = Array(-1.632, 2.115, 1.094, -1.025, -0.636)

    val ne = new NormalEquation(5)

    System.arraycopy(ata, 0, ne.ata, 0, ne.triK)
    System.arraycopy(atb, 0, ne.atb, 0, ne.k)

    val goodx = Vectors.dense(0.13025, 0.54506, 0.2874, 0.0, 0.028628)
    val qm = new QuadraticSolver(n, POSITIVE)
    val xpos = qm.solve(ne, 0.0).map(_.toDouble)

    assert(Vectors.dense(xpos) ~= goodx absTol 1e-3)
  }

  test("QuadraticSolver with BOX constraint") {
    val n = 5
    val ata = Array(
      4.3142,
      -3.3292,14.2353,
      -2.4655,-3.1405,8.4796,
      -3.3249,-2.3052,2.5011,18.1953,
      -4.3772,0.7867,3.2092,12.9338,10.5297)

    val atb = Array(-1.0347, -0.7269, 0.3034, -0.2939, 0.7873)

    val ne = new NormalEquation(5)

    System.arraycopy(ata, 0, ne.ata, 0, ne.triK)
    System.arraycopy(atb, 0, ne.atb, 0, ne.k)

    val goodx = Vectors.dense(0.0000, 0.0000, 0.0085, 0.0000, 0.0722)

    val qm = new QuadraticSolver(n, BOX)
    val xbounds = qm.solve(ne, 0.0).map(_.toDouble)
    assert(Vectors.dense(xbounds) ~= goodx absTol 1e-3)
  }

  // Generated from MovieLens dataset debug on convergence
  test("QuadraticSolver with EQUALITY constraint") {
    val Hml = new BrzMatrix(25, 25, Array(112.647378, 44.962984, 49.127829, 43.708389, 43.333008, 46.345827, 49.581542, 42.991226, 43.999341, 41.336724, 42.879992, 46.896465, 41.778920, 46.333559, 51.168782, 44.800998, 43.735417, 42.672057, 40.024492, 48.793499, 48.696170, 45.870016, 46.398093, 44.305791, 41.863013, 44.962984, 107.202825, 44.218178, 38.585858, 36.606830, 41.783275, 44.631314, 40.883821, 37.948817, 34.908843, 38.356328, 43.642467, 36.213124, 38.760314, 43.317775, 36.803445, 41.905953, 40.238334, 42.325769, 45.853665, 46.601722, 40.668861, 49.084078, 39.292553, 35.781804, 49.127829, 44.218178, 118.264304, 40.139032, 43.741591, 49.387932, 45.558785, 40.793703, 46.136010, 41.839393, 39.544248, 43.161644, 43.361811, 43.832852, 50.572459, 42.036961, 47.251940, 45.273068, 42.842437, 49.323737, 52.125739, 45.831747, 49.466716, 44.762183, 41.930313, 43.708389, 38.585858, 40.139032, 94.937989, 36.562570, 41.628404, 38.604965, 39.080500, 37.267530, 34.291272, 34.891704, 39.216238, 35.970491, 40.733288, 41.872521, 35.825264, 38.144457, 41.368293, 40.751910, 41.123673, 41.930358, 41.002915, 43.099168, 36.018699, 33.646602, 43.333008, 36.606830, 43.741591, 36.562570, 105.764912, 42.799031, 38.215171, 42.193565, 38.708056, 39.448031, 37.882184, 40.172339, 40.625192, 39.015338, 36.433413, 40.848178, 36.480813, 41.439981, 40.797598, 40.325652, 38.599119, 42.727171, 39.382845, 41.535989, 41.518779, 46.345827, 41.783275, 49.387932, 41.628404, 42.799031, 114.691992, 43.015599, 42.688570, 42.722905, 38.675192, 38.377970, 44.656183, 39.087805, 45.443516, 50.585268, 40.949970, 41.920556, 43.711898, 41.463472, 51.248836, 46.869144, 45.178199, 45.709593, 42.402465, 44.097412, 49.581542, 44.631314, 45.558785, 38.604965, 38.215171, 43.015599, 114.667896, 40.966284, 37.748084, 39.496813, 40.534741, 42.770125, 40.628678, 41.194251, 47.837969, 44.596875, 43.448257, 43.291878, 39.005953, 50.493111, 46.296591, 43.449036, 48.798961, 42.877859, 37.055014, 42.991226, 40.883821, 40.793703, 39.080500, 42.193565, 42.688570, 40.966284, 106.632656, 37.640927, 37.181799, 40.065085, 38.978761, 36.014753, 38.071494, 41.081064, 37.981693, 41.821252, 42.773603, 39.293957, 38.600491, 43.761301, 42.294750, 42.410289, 40.266469, 39.909538, 43.999341, 37.948817, 46.136010, 37.267530, 38.708056, 42.722905, 37.748084, 37.640927, 102.747189, 34.574727, 36.525241, 39.839891, 36.297838, 42.756496, 44.673874, 38.350523, 40.330611, 42.288511, 39.472844, 45.617102, 44.692618, 41.194977, 41.284030, 39.580938, 42.382268, 41.336724, 34.908843, 41.839393, 34.291272, 39.448031, 38.675192, 39.496813, 37.181799, 34.574727, 94.205550, 37.583319, 38.504211, 36.376976, 34.239351, 39.060978, 37.515228, 37.079566, 37.317791, 38.046576, 36.112222, 39.406838, 39.258432, 36.347136, 38.927619, 41.604838, 42.879992, 38.356328, 39.544248, 34.891704, 37.882184, 38.377970, 40.534741, 40.065085, 36.525241, 37.583319, 98.109622, 39.428284, 37.518381, 39.659011, 38.477483, 40.547021, 42.678061, 42.279104, 41.515782, 43.478416, 45.003800, 42.433639, 42.757336, 35.814356, 39.017848, 46.896465, 43.642467, 43.161644, 39.216238, 40.172339, 44.656183, 42.770125, 38.978761, 39.839891, 38.504211, 39.428284, 103.478446, 39.984358, 40.587958, 44.490750, 40.600474, 40.698368, 42.296794, 41.567854, 47.002908, 43.922434, 43.479144, 44.291425, 43.352951, 42.613649, 41.778920, 36.213124, 43.361811, 35.970491, 40.625192, 39.087805, 40.628678, 36.014753, 36.297838, 36.376976, 37.518381, 39.984358, 99.799628, 38.027891, 44.268308, 36.202204, 39.921811, 38.668774, 36.832286, 45.833218, 43.228963, 36.833273, 44.787401, 38.176476, 39.062471, 46.333559, 38.760314, 43.832852, 40.733288, 39.015338, 45.443516, 41.194251, 38.071494, 42.756496, 34.239351, 39.659011, 40.587958, 38.027891, 114.304283, 46.958354, 39.636801, 40.927870, 49.118094, 43.093642, 50.196436, 45.535041, 43.087415, 48.540036, 35.942528, 37.962886, 51.168782, 43.317775, 50.572459, 41.872521, 36.433413, 50.585268, 47.837969, 41.081064, 44.673874, 39.060978, 38.477483, 44.490750, 44.268308, 46.958354, 122.935323, 39.948695, 46.801841, 44.455283, 40.160668, 54.193098, 49.678271, 41.834745, 47.227606, 42.214571, 42.598524, 44.800998, 36.803445, 42.036961, 35.825264, 40.848178, 40.949970, 44.596875, 37.981693, 38.350523, 37.515228, 40.547021, 40.600474, 36.202204, 39.636801, 39.948695, 97.365126, 40.163209, 39.177628, 38.935283, 41.465246, 40.962743, 40.533287, 43.367907, 38.723316, 36.312733, 43.735417, 41.905953, 47.251940, 38.144457, 36.480813, 41.920556, 43.448257, 41.821252, 40.330611, 37.079566, 42.678061, 40.698368, 39.921811, 40.927870, 46.801841, 40.163209, 110.416786, 46.843429, 41.834126, 46.788801, 46.983780, 43.511429, 47.291825, 40.023523, 40.581819, 42.672057, 40.238334, 45.273068, 41.368293, 41.439981, 43.711898, 43.291878, 42.773603, 42.288511, 37.317791, 42.279104, 42.296794, 38.668774, 49.118094, 44.455283, 39.177628, 46.843429, 107.474576, 44.590023, 48.333476, 44.059916, 42.653703, 44.171623, 39.363181, 41.716539, 40.024492, 42.325769, 42.842437, 40.751910, 40.797598, 41.463472, 39.005953, 39.293957, 39.472844, 38.046576, 41.515782, 41.567854, 36.832286, 43.093642, 40.160668, 38.935283, 41.834126, 44.590023, 105.140579, 43.149105, 41.516560, 43.494333, 45.664210, 36.466241, 37.477898, 48.793499, 45.853665, 49.323737, 41.123673, 40.325652, 51.248836, 50.493111, 38.600491, 45.617102, 36.112222, 43.478416, 47.002908, 45.833218, 50.196436, 54.193098, 41.465246, 46.788801, 48.333476, 43.149105, 123.746816, 53.234332, 44.633908, 53.537592, 43.196327, 42.747181, 48.696170, 46.601722, 52.125739, 41.930358, 38.599119, 46.869144, 46.296591, 43.761301, 44.692618, 39.406838, 45.003800, 43.922434, 43.228963, 45.535041, 49.678271, 40.962743, 46.983780, 44.059916, 41.516560, 53.234332, 125.202062, 43.967875, 52.416619, 39.937196, 39.775405, 45.870016, 40.668861, 45.831747, 41.002915, 42.727171, 45.178199, 43.449036, 42.294750, 41.194977, 39.258432, 42.433639, 43.479144, 36.833273, 43.087415, 41.834745, 40.533287, 43.511429, 42.653703, 43.494333, 44.633908, 43.967875, 107.336922, 44.396001, 39.819884, 38.676633, 46.398093, 49.084078, 49.466716, 43.099168, 39.382845, 45.709593, 48.798961, 42.410289, 41.284030, 36.347136, 42.757336, 44.291425, 44.787401, 48.540036, 47.227606, 43.367907, 47.291825, 44.171623, 45.664210, 53.537592, 52.416619, 44.396001, 114.651847, 40.826050, 37.634130, 44.305791, 39.292553, 44.762183, 36.018699, 41.535989, 42.402465, 42.877859, 40.266469, 39.580938, 38.927619, 35.814356, 43.352951, 38.176476, 35.942528, 42.214571, 38.723316, 40.023523, 39.363181, 36.466241, 43.196327, 39.937196, 39.819884, 40.826050, 96.128345, 40.788606, 41.863013, 35.781804, 41.930313, 33.646602, 41.518779, 44.097412, 37.055014, 39.909538, 42.382268, 41.604838, 39.017848, 42.613649, 39.062471, 37.962886, 42.598524, 36.312733, 40.581819, 41.716539, 37.477898, 42.747181, 39.775405, 38.676633, 37.634130, 40.788606, 97.605849))
    val fml = BrzVector(-1219.296604, -1126.029219, -1202.257728, -1022.064083, -1047.414836, -1155.507387, -1169.502847, -1091.655366, -1063.832607, -1015.829142, -1075.864072, -1101.427162, -1058.907539, -1115.171116, -1205.015211, -1090.627084, -1143.206126, -1140.107801, -1100.285642, -1198.992795, -1246.276120, -1159.678276, -1194.177391, -1056.458015, -1058.791892)
    fml *= -1.0

    val ml = 25
    val upper = upperTriangular(Hml).data.filter{x=> abs(x) >= 1e-8}

    val ne = new NormalEquation(ml)

    System.arraycopy(upper, 0, ne.ata, 0, ne.triK)
    System.arraycopy(fml.data, 0, ne.atb, 0, ne.k)

    val goodx = Vectors.dense(0.3131862265452959, 0.0, 0.01129486116330884, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.060642310566736704, 0.0, 0.0, 0.0, 0.0, 0.0, 0.6151756449091074, 0.0, 0.0, 0.0, 0.0)

    val qm = new QuadraticSolver(ml, EQUALITY)
    val xequality = qm.solve(ne, 0.0).map(_.toDouble)
    assert(Vectors.dense(xequality) ~= goodx absTol 1e-3)
    assert(sum(BrzVector(xequality)) ~= 1.0 absTol 1e-4)
  }

  // Generated from MovieLens dataset debug on convergence
  test("QuadraticSolver with SPARSE constraint") {
    val Hl1 = new BrzMatrix(25, 25, Array(253.535098, 236.477785, 234.421906, 223.374867, 241.007512, 204.695511, 226.465507, 223.351032, 249.179386, 221.411909, 238.679352, 203.579010, 217.564498, 243.852681, 266.607649, 213.994496, 241.620759, 223.602907, 220.038678, 264.704959, 240.341716, 223.672378, 244.323303, 223.216217, 226.074990, 236.477785, 278.862035, 245.756639, 237.489890, 252.783139, 214.077652, 241.816953, 238.790633, 260.536460, 228.121417, 255.103936, 216.608405, 237.150426, 258.933231, 281.958112, 228.971242, 252.508513, 234.242638, 240.308477, 285.416390, 254.792243, 240.176223, 259.048267, 235.566855, 236.277617, 234.421906, 245.756639, 269.162882, 231.416867, 251.321527, 208.134322, 236.567647, 236.558029, 255.805108, 226.535825, 251.514713, 212.770208, 228.565362, 261.748652, 273.946966, 227.411615, 252.767900, 232.823977, 233.084574, 278.315614, 250.872786, 235.227909, 255.104263, 238.931093, 235.402356, 223.374867, 237.489890, 231.416867, 254.771963, 241.703229, 209.028084, 231.517998, 228.768510, 250.805315, 216.548935, 245.473869, 207.687875, 222.036114, 250.906955, 263.018181, 216.128966, 244.445283, 227.436840, 231.369510, 270.721492, 242.475130, 226.471530, 248.130112, 225.826557, 228.266719, 241.007512, 252.783139, 251.321527, 241.703229, 285.702320, 219.051868, 249.442308, 240.400187, 264.970407, 232.503138, 258.819837, 220.160683, 235.621356, 267.743972, 285.795029, 229.667231, 260.870105, 240.751687, 247.183922, 289.044453, 260.715749, 244.210258, 267.159502, 242.992822, 244.070245, 204.695511, 214.077652, 208.134322, 209.028084, 219.051868, 210.164224, 208.151908, 201.539036, 226.373834, 192.056565, 219.950686, 191.459568, 195.982460, 226.739575, 240.677519, 196.116652, 217.352348, 203.533069, 204.581690, 243.603643, 217.785986, 204.205559, 223.747953, 203.586842, 200.165867, 226.465507, 241.816953, 236.567647, 231.517998, 249.442308, 208.151908, 264.007925, 227.080718, 253.174653, 220.322823, 248.619983, 210.100242, 223.279198, 254.807401, 269.896959, 222.927882, 247.017507, 230.484479, 233.358639, 274.935489, 249.237737, 235.229584, 253.029955, 228.601700, 230.512885, 223.351032, 238.790633, 236.558029, 228.768510, 240.400187, 201.539036, 227.080718, 258.773479, 249.471480, 215.664539, 243.078577, 202.337063, 221.020998, 249.979759, 263.356244, 213.470569, 246.182278, 225.727773, 229.873732, 266.295057, 242.954024, 225.510760, 249.370268, 227.813265, 232.141964, 249.179386, 260.536460, 255.805108, 250.805315, 264.970407, 226.373834, 253.174653, 249.471480, 302.360150, 237.902729, 265.769812, 224.947876, 243.088105, 273.690377, 291.076027, 241.089661, 267.772651, 248.459822, 249.662698, 295.935799, 267.460908, 255.668926, 275.902272, 248.495606, 246.827505, 221.411909, 228.121417, 226.535825, 216.548935, 232.503138, 192.056565, 220.322823, 215.664539, 237.902729, 245.154567, 234.956316, 199.557862, 214.774631, 240.339217, 255.161923, 209.328714, 232.277540, 216.298768, 220.296241, 253.817633, 237.638235, 220.785141, 239.098500, 220.583355, 218.962732, 238.679352, 255.103936, 251.514713, 245.473869, 258.819837, 219.950686, 248.619983, 243.078577, 265.769812, 234.956316, 288.133073, 225.087852, 239.810430, 268.406605, 283.289840, 233.858455, 258.306589, 240.263617, 246.844456, 290.492875, 267.212598, 243.218596, 265.681905, 244.615890, 242.543363, 203.579010, 216.608405, 212.770208, 207.687875, 220.160683, 191.459568, 210.100242, 202.337063, 224.947876, 199.557862, 225.087852, 217.501685, 197.897572, 229.825316, 242.175607, 201.123644, 219.820165, 202.894307, 211.468055, 246.048907, 225.135194, 210.076305, 226.806762, 212.014431, 205.123267, 217.564498, 237.150426, 228.565362, 222.036114, 235.621356, 195.982460, 223.279198, 221.020998, 243.088105, 214.774631, 239.810430, 197.897572, 244.439113, 241.621129, 260.400953, 216.482178, 236.805076, 216.680343, 223.816297, 263.188711, 236.311810, 222.950152, 244.636356, 219.121372, 219.911078, 243.852681, 258.933231, 261.748652, 250.906955, 267.743972, 226.739575, 254.807401, 249.979759, 273.690377, 240.339217, 268.406605, 229.825316, 241.621129, 302.928261, 288.344398, 238.549018, 267.239982, 248.073140, 254.230916, 296.789984, 267.158551, 252.226496, 271.170860, 248.325354, 253.694013, 266.607649, 281.958112, 273.946966, 263.018181, 285.795029, 240.677519, 269.896959, 263.356244, 291.076027, 255.161923, 283.289840, 242.175607, 260.400953, 288.344398, 343.457361, 257.368309, 284.795470, 263.122266, 271.239770, 320.209823, 283.933299, 264.416752, 292.035194, 268.764031, 265.345807, 213.994496, 228.971242, 227.411615, 216.128966, 229.667231, 196.116652, 222.927882, 213.470569, 241.089661, 209.328714, 233.858455, 201.123644, 216.482178, 238.549018, 257.368309, 239.295031, 234.913508, 218.066855, 219.648997, 257.969951, 231.243624, 224.657569, 238.158714, 217.174368, 215.933866, 241.620759, 252.508513, 252.767900, 244.445283, 260.870105, 217.352348, 247.017507, 246.182278, 267.772651, 232.277540, 258.306589, 219.820165, 236.805076, 267.239982, 284.795470, 234.913508, 289.709239, 241.312315, 247.249491, 286.702147, 264.252852, 245.151647, 264.582984, 240.842689, 245.837476, 223.602907, 234.242638, 232.823977, 227.436840, 240.751687, 203.533069, 230.484479, 225.727773, 248.459822, 216.298768, 240.263617, 202.894307, 216.680343, 248.073140, 263.122266, 218.066855, 241.312315, 255.363057, 230.209787, 271.091482, 239.220241, 225.387834, 247.486715, 226.052431, 224.119935, 220.038678, 240.308477, 233.084574, 231.369510, 247.183922, 204.581690, 233.358639, 229.873732, 249.662698, 220.296241, 246.844456, 211.468055, 223.816297, 254.230916, 271.239770, 219.648997, 247.249491, 230.209787, 264.014907, 271.938970, 246.664305, 227.889045, 249.908085, 232.035369, 229.010298, 264.704959, 285.416390, 278.315614, 270.721492, 289.044453, 243.603643, 274.935489, 266.295057, 295.935799, 253.817633, 290.492875, 246.048907, 263.188711, 296.789984, 320.209823, 257.969951, 286.702147, 271.091482, 271.938970, 352.825726, 286.200221, 267.716897, 297.182554, 269.776351, 266.721561, 240.341716, 254.792243, 250.872786, 242.475130, 260.715749, 217.785986, 249.237737, 242.954024, 267.460908, 237.638235, 267.212598, 225.135194, 236.311810, 267.158551, 283.933299, 231.243624, 264.252852, 239.220241, 246.664305, 286.200221, 294.042749, 246.504021, 269.570596, 243.980697, 242.690997, 223.672378, 240.176223, 235.227909, 226.471530, 244.210258, 204.205559, 235.229584, 225.510760, 255.668926, 220.785141, 243.218596, 210.076305, 222.950152, 252.226496, 264.416752, 224.657569, 245.151647, 225.387834, 227.889045, 267.716897, 246.504021, 259.897656, 251.730847, 229.335712, 229.759185, 244.323303, 259.048267, 255.104263, 248.130112, 267.159502, 223.747953, 253.029955, 249.370268, 275.902272, 239.098500, 265.681905, 226.806762, 244.636356, 271.170860, 292.035194, 238.158714, 264.582984, 247.486715, 249.908085, 297.182554, 269.570596, 251.730847, 303.872223, 251.585636, 247.878402, 223.216217, 235.566855, 238.931093, 225.826557, 242.992822, 203.586842, 228.601700, 227.813265, 248.495606, 220.583355, 244.615890, 212.014431, 219.121372, 248.325354, 268.764031, 217.174368, 240.842689, 226.052431, 232.035369, 269.776351, 243.980697, 229.335712, 251.585636, 257.544914, 228.810942, 226.074990, 236.277617, 235.402356, 228.266719, 244.070245, 200.165867, 230.512885, 232.141964, 246.827505, 218.962732, 242.543363, 205.123267, 219.911078, 253.694013, 265.345807, 215.933866, 245.837476, 224.119935, 229.010298, 266.721561, 242.690997, 229.759185, 247.878402, 228.810942, 253.353769))
    val fl1 = BrzVector(-892.842851, -934.071560, -932.936015, -888.124343, -961.050207, -791.191087, -923.711397, -904.289301, -988.384984, -883.909133, -959.465030, -798.551172, -871.622303, -997.463289, -1043.912620, -863.013719, -976.975712, -897.033693, -898.694786, -1069.245497, -963.491924, -901.263474, -983.768031, -899.865392, -902.283567)
    fl1 *= -1.0
    val ml = 25

    val upper = upperTriangular(Hl1).data.filter { x => abs(x) >= 1e-8 }

    val ne = new NormalEquation(ml)
    ne.n = 1

    System.arraycopy(upper, 0, ne.ata, 0, ne.triK)
    System.arraycopy(fl1.data, 0, ne.atb, 0, ne.k)

    val goodx = Vectors.dense(0.18611, 0.00000, 0.06317, -0.10417, 0.11262,
      -0.20495, 0.52668, 0.32790, 0.19421, 0.72180,
      0.06309, -0.41326, -0.00000, 0.52078, -0.00000,
      0.18040, 0.62915, 0.16329, -0.06424, 0.37539,
      0.01659, 0.00000, 0.11215, 0.24778, 0.04082)

    val qm = new QuadraticSolver(ml, SPARSE)

    val xl1 = qm.solve(ne, 2.0).map(_.toDouble)
    assert(Vectors.dense(xl1) ~= goodx absTol 1e-3)
  }

  test("RatingBlockBuilder") {
    val emptyBuilder = new RatingBlockBuilder[Int]()
    assert(emptyBuilder.size === 0)
    val emptyBlock = emptyBuilder.build()
    assert(emptyBlock.srcIds.isEmpty)
    assert(emptyBlock.dstIds.isEmpty)
    assert(emptyBlock.ratings.isEmpty)

    val builder0 = new RatingBlockBuilder()
      .add(Rating(0, 1, 2.0f))
      .add(Rating(3, 4, 5.0f))
    assert(builder0.size === 2)
    val builder1 = new RatingBlockBuilder()
      .add(Rating(6, 7, 8.0f))
      .merge(builder0.build())
    assert(builder1.size === 3)
    val block = builder1.build()
    val ratings = Seq.tabulate(block.size) { i =>
      (block.srcIds(i), block.dstIds(i), block.ratings(i))
    }.toSet
    assert(ratings === Set((0, 1, 2.0f), (3, 4, 5.0f), (6, 7, 8.0f)))
  }

  test("UncompressedInBlock") {
    val encoder = new LocalIndexEncoder(10)
    val uncompressed = new UncompressedInBlockBuilder[Int](encoder)
      .add(0, Array(1, 0, 2), Array(0, 1, 4), Array(1.0f, 2.0f, 3.0f))
      .add(1, Array(3, 0), Array(2, 5), Array(4.0f, 5.0f))
      .build()
    assert(uncompressed.length === 5)
    val records = Seq.tabulate(uncompressed.length) { i =>
      val dstEncodedIndex = uncompressed.dstEncodedIndices(i)
      val dstBlockId = encoder.blockId(dstEncodedIndex)
      val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
      (uncompressed.srcIds(i), dstBlockId, dstLocalIndex, uncompressed.ratings(i))
    }.toSet
    val expected =
      Set((1, 0, 0, 1.0f), (0, 0, 1, 2.0f), (2, 0, 4, 3.0f), (3, 1, 2, 4.0f), (0, 1, 5, 5.0f))
    assert(records === expected)

    val compressed = uncompressed.compress()
    assert(compressed.size === 5)
    assert(compressed.srcIds.toSeq === Seq(0, 1, 2, 3))
    assert(compressed.dstPtrs.toSeq === Seq(0, 2, 3, 4, 5))
    var decompressed = ArrayBuffer.empty[(Int, Int, Int, Float)]
    var i = 0
    while (i < compressed.srcIds.size) {
      var j = compressed.dstPtrs(i)
      while (j < compressed.dstPtrs(i + 1)) {
        val dstEncodedIndex = compressed.dstEncodedIndices(j)
        val dstBlockId = encoder.blockId(dstEncodedIndex)
        val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
        decompressed += ((compressed.srcIds(i), dstBlockId, dstLocalIndex, compressed.ratings(j)))
        j += 1
      }
      i += 1
    }
    assert(decompressed.toSet === expected)
  }

  /**
   * Generates an explicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genExplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val x = random.nextDouble()
      if (x < totalFraction) {
        val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
        if (x < trainingFraction) {
          val noise = noiseStd * random.nextGaussian()
          training += Rating(userId, itemId, rating + noise.toFloat)
        } else {
          test += Rating(userId, itemId, rating)
        }
      }
    }
    logInfo(s"Generated an explicit feedback dataset with ${training.size} ratings for training " +
      s"and ${test.size} for test.")
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  /**
   * Generates an implicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genImplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    // The assumption of the implicit feedback model is that unobserved ratings are more likely to
    // be negatives.
    val positiveFraction = 0.8
    val negativeFraction = 1.0 - positiveFraction
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
      val threshold = if (rating > 0) positiveFraction else negativeFraction
      val observed = random.nextDouble() < threshold
      if (observed) {
        val x = random.nextDouble()
        if (x < totalFraction) {
          if (x < trainingFraction) {
            val noise = noiseStd * random.nextGaussian()
            training += Rating(userId, itemId, rating + noise.toFloat)
          } else {
            test += Rating(userId, itemId, rating)
          }
        }
      }
    }
    logInfo(s"Generated an implicit feedback dataset with ${training.size} ratings for training " +
      s"and ${test.size} for test.")
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  /**
   * Generates random user/item factors, with i.i.d. values drawn from U(a, b).
   * @param size number of users/items
   * @param rank number of features
   * @param random random number generator
   * @param a min value of the support (default: -1)
   * @param b max value of the support (default: 1)
   * @return a sequence of (ID, factors) pairs
   */
  private def genFactors(
      size: Int,
      rank: Int,
      random: Random,
      a: Float = -1.0f,
      b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    require(b > a)
    val ids = mutable.Set.empty[Int]
    while (ids.size < size) {
      ids += random.nextInt()
    }
    val width = b - a
    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
  }

  /**
   * Test ALS using the given training/test splits and parameters.
   * @param training training dataset
   * @param test test dataset
   * @param rank rank of the matrix factorization
   * @param maxIter max number of iterations
   * @param regParam regularization constant
   * @param implicitPrefs whether to use implicit preference
   * @param numUserBlocks number of user blocks
   * @param numItemBlocks number of item blocks
   * @param targetRMSE target test RMSE
   */
  def testALS(
      training: RDD[Rating[Int]],
      test: RDD[Rating[Int]],
      rank: Int,
      maxIter: Int,
      regParam: Double,
      implicitPrefs: Boolean = false,
      numUserBlocks: Int = 2,
      numItemBlocks: Int = 3,
      targetRMSE: Double = 0.05): Unit = {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._
    val als = new ALS()
      .setRank(rank)
      .setRegParam(regParam)
      .setImplicitPrefs(implicitPrefs)
      .setNumUserBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
    val alpha = als.getAlpha
    val model = als.fit(training.toDF())
    val predictions = model.transform(test.toDF())
      .select("rating", "prediction")
      .map { case Row(rating: Float, prediction: Float) =>
        (rating.toDouble, prediction.toDouble)
      }
    val rmse =
      if (implicitPrefs) {
        // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
        // We limit the ratings and the predictions to interval [0, 1] and compute the weighted RMSE
        // with the confidence scores as weights.
        val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
          val confidence = 1.0 + alpha * math.abs(rating)
          val rating01 = math.max(math.min(rating, 1.0), 0.0)
          val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
          val err = prediction01 - rating01
          (confidence, confidence * err * err)
        }.reduce { case ((c0, e0), (c1, e1)) =>
          (c0 + c1, e0 + e1)
        }
        math.sqrt(weightedSumSq / totalWeight)
      } else {
        val mse = predictions.map { case (rating, prediction) =>
          val err = rating - prediction
          err * err
        }.mean()
        math.sqrt(mse)
      }
    logInfo(s"Test RMSE is $rmse.")
    assert(rmse < targetRMSE)
  }

  test("exact rank-1 matrix") {
    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)
    testALS(training, test, maxIter = 1, rank = 1, regParam = 1e-5, targetRMSE = 0.001)
    testALS(training, test, maxIter = 1, rank = 2, regParam = 1e-5, targetRMSE = 0.001)
  }

  test("approximate rank-1 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 0.01, targetRMSE = 0.02)
    testALS(training, test, maxIter = 2, rank = 2, regParam = 0.01, targetRMSE = 0.02)
  }

  test("approximate rank-2 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03)
    testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03)
  }

  test("different block settings") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
      testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03,
        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks)
    }
  }

  test("more blocks than ratings") {
    val (training, test) =
      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 1e-4, targetRMSE = 0.002,
     numItemBlocks = 5, numUserBlocks = 5)
  }

  test("implicit feedback") {
    val (training, test) =
      genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, implicitPrefs = true,
      targetRMSE = 0.3)
  }

  test("using generic ID types") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)

    val longRatings = ratings.map(r => Rating(r.user.toLong, r.item.toLong, r.rating))
    val (longUserFactors, _) = ALS.train(longRatings, rank = 2, maxIter = 4)
    assert(longUserFactors.first()._1.getClass === classOf[Long])

    val strRatings = ratings.map(r => Rating(r.user.toString, r.item.toString, r.rating))
    val (strUserFactors, _) = ALS.train(strRatings, rank = 2, maxIter = 4)
    assert(strUserFactors.first()._1.getClass === classOf[String])
  }

  test("nonnegative constraint") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)

    val (userFactors, itemFactors) = ALS.train(ratings, rank = 2, maxIter = 4, userConstraint=POSITIVE, itemConstraint=POSITIVE)
    def isNonnegative(factors: RDD[(Int, Array[Float])]): Boolean = {
      factors.values.map { _.forall(_ >= 0.0) }.reduce(_ && _)
    }
    assert(isNonnegative(userFactors))
    assert(isNonnegative(itemFactors))
    // TODO: Validate the solution.
  }

  test("als partitioner is a projection") {
    for (p <- Seq(1, 10, 100, 1000)) {
      val part = new ALSPartitioner(p)
      var k = 0
      while (k < p) {
        assert(k === part.getPartition(k))
        assert(k === part.getPartition(k.toLong))
        k += 1
      }
    }
  }

  test("partitioner in returned factors") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    val (userFactors, itemFactors) = ALS.train(
      ratings, rank = 2, maxIter = 4, numUserBlocks = 3, numItemBlocks = 4)
    for ((tpe, factors) <- Seq(("User", userFactors), ("Item", itemFactors))) {
      assert(userFactors.partitioner.isDefined, s"$tpe factors should have partitioner.")
      val part = userFactors.partitioner.get
      userFactors.mapPartitionsWithIndex { (idx, items) =>
        items.foreach { case (id, _) =>
          if (part.getPartition(id) != idx) {
            throw new SparkException(s"$tpe with ID $id should not be in partition $idx.")
          }
        }
        Iterator.empty
      }.count()
    }
  }
}
