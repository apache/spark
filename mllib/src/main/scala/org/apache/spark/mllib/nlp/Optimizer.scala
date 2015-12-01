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
package org.apache.spark.mllib.nlp

import scala.collection.mutable.ArrayBuffer

private[mllib] class Optimizer {

  private var w: ArrayBuffer[Double] = new ArrayBuffer[Double]()
  private var v: ArrayBuffer[Double] = new ArrayBuffer[Double]()
  private var xi: ArrayBuffer[Double] = new ArrayBuffer[Double]()
  private var diag: ArrayBuffer[Double] = new ArrayBuffer[Double]()
  private var iflag: Int = 0
  private var point: Int = 0
  private var ispt: Int = 0
  private var iypt: Int = 0
  private var iycn: Int = 0
  private var iter: Int = 0
  private var info: Int = 0
  private var stp1: Double = 0
  private var npt: Int = 0
  private var iscn: Int = 0
  private var nfev: Int = 0
  private var stp: Double = 1.0
  private var continue: Boolean = true
  private val eps: Double = 1e-7
  private var dginit: Double = 0.0
  private var infoc: Int = 0
  private var brackt: Boolean = false
  private var stage1: Boolean = false
  private var finit: Double = 0.0
  private var dgtest: Double = 0.0
  private val ftol: Double = 1e-4
  private val p5: Double = 0.5
  private val p66: Double = 0.66
  private val xtrapf: Double = 4.0
  private val maxfev: Int = 20
  private var width: Double = 0.0
  private var width1: Double = 0.0
  private var stx: Double = 0.0
  private var fx: Double = 0.0
  private var dgx: Double = 0.0
  private var sty: Double = 0.0
  private var fy: Double = 0.0
  private var dgy: Double = 0.0
  private var contadj: Boolean = true
  private var stmin: Double = 0.0
  private var stmax: Double = 0.0


  def optimizer(size: Int, x: ArrayBuffer[Double], f: Double,
            g: ArrayBuffer[Double], C: Float): Unit = {
    val msize: Int = 5
    var bound: Int = 0
    var ys: Double = 0.0
    var yy: Double = 0.0
    var cp: Int = 0
    var j: Int = 0
    val p5: Double = 0.5
    val p66: Double = 0.66
    val xtrapf: Double = 4.0
    val maxfev: Int = 20
    var i: Int = 0
    var persist: Boolean = true

    if (w.isEmpty) {
      iflag = 0
      for (i <- 0 until size) {
        diag.append(1.0)
      }
      ispt = size + (msize << 1)
      iypt = ispt + size * msize
      for (i <- 0 until size * (2 * msize + 1) + 2 * msize) {
        w.append(0.0)
      }
    }
    if (iflag == 0) {
      point = 0
      while (j < size) {
        w.update(ispt + j, -g(j) * diag(j))
        j += 1
      }
      stp1 = 1.0 / math.sqrt(ddot(size, g, 0, g, 0))
    }
    while (true) {
      if (iflag == 0) {
        iter += 1
        info = 0
        if (iter == 1) {
          nfev = 0
          stp = 1.0
          stp = stp1
          for (i <- 1 until size) {
            w(i) = g(i)
          }
        } else {
          if (iter > size) {
            bound = size
          }
          ys = ddot(size, w, iypt + npt, w, ispt + npt)
          yy = ddot(size, w, iypt + npt, w, iypt + npt)
          i = 0
          while (i < size) {
            diag(i) = ys / yy
            i += 1
          }
        }
      }
      else if (iflag != 1 && iter != 1) {
        cp = point
        if (point == 0) {
          cp = msize
        }
        w(size + cp) = 1.0 / ys

        for (i <- 0 until size) {
          w(i) = -g(i)
        }

        bound = math.min(iter - 1, msize)
        cp = point
        i = 0
        while (i < bound) {
          cp -= 1
          if (cp == -1) cp = msize
          val sq: Double = ddot(size, w, ispt + cp * size, w, 0)
          val inmc: Int = size + msize + cp
          iycn = iypt + cp * size
          w(inmc) = w(size + cp) * sq
          val d: Double = -w(inmc)
          daxpy(size, d, w, iycn, w, 0)
          i += 1
        }
        i = 0
        while (i < size) {
          w(i) = diag(i) * w(i)
          i += 1
        }
        i = 0
        while (i < bound) {
          val yr: Double = ddot(size, w, iypt + cp * size + 1, w, 1)
          var beta: Double = w(size + cp + 1) * yr
          val inmc: Int = size + msize + cp
          beta = w(inmc) - beta
          iscn = ispt + cp * size
          daxpy(size, beta, w, iscn, w, 0)
          cp += 1
          if (cp == msize) cp = 0
          i += 1
        }
        i = 0
        while (i < size) {
          w(ispt + point * size + i) = w(i)
          i += 1
        }
      }
      // mcsrch line search
      if (info != -1) {
        infoc = 1
        if (size <= 0 || stp <= 0.0) {
          return
        }
        dginit = ddot(size, g, 0, w, ispt)
        if (dginit >= 0.0) return
        brackt = false
        stage1 = true
        nfev = 0
        finit = f
        dgtest = ftol * dginit
        width = 1e20 - 1e-20
        width1 = width / p5
        j = 0
        while (j < size) {
          diag(j) = x(j)
          j += 1
        }
        stx = 0.0
        fx = finit
        dgx = dginit
        sty = 0.0
        fy = finit
        dgy = dginit
      }
      while (persist) {
        if (info != -1) {
          stmin = stx
          stmax = stp + xtrapf * (stp - stx)
          stp = math.max(stp, 1e-20)
          stp = math.min(stp, 1e20)
          j = 0
          while (j < size) {
            x(j) = diag(j) + stp * w(j + ispt)
            j += 1
          }
          info = -1
          persist = false
        } else {
          info = 0
          nfev += 1
          val dg: Double = ddot(size, g, 0, w, ispt)
          val ftest1: Double = finit + stp * dgtest

          if (stp == 1e20 && f <= ftest1 && dg <= dgtest) {
            info = 5
          }
          if (stp == 1e-20 && (f > ftest1 || dg >= dgtest)) {
            info = 4
          }
          if (nfev >= maxfev) {
            info = 3
          }
          if (f <= ftest1 && math.abs(dg) <= 0.9 * (-dginit)) {
            info = 1
          }
          if (info != 0) {
            persist = false
          }
        }
      }
      // mcsrch
      if (info == -1) {
        iflag = 1
        return
      }
      if (info != 1) {
        iflag = -1
        return
      }
      npt = point * size
      i = 0
      while (i < size) {
        w(ispt + npt + i) = stp * w(ispt + npt + i)
        w(iypt + npt + i) = g(i) - w(i)
        i += 1
      }
      point += 1
      if (point == msize) point = 0
      val gnorm: Double = math.sqrt(ddot(size, v, 0, v, 0))
      val xnorm: Double = math.max(1.0, math.sqrt(ddot(size, x, 0, x, 0)))
      if (gnorm / xnorm <= eps) {
        iflag = 0
      }
    }
  }

  def ddot(size: Int, v1: ArrayBuffer[Double], v1start: Int,
           v2: ArrayBuffer[Double], v2start: Int): Double = {
    var result: Double = 0
    var i: Int = v1start
    var j: Int = v2start
    while (i < v1.size && j < v2.size) {
      result = result + v1(i) * v2(j)
      i += 1
      j += 1
    }
    result
  }

  def daxpy(n: Int, da: Double, dx: ArrayBuffer[Double], dxStart: Int,
            dy: ArrayBuffer[Double], dyStart: Int): Unit = {
    var i: Int = 0
    while (i < n) {
      dy(i + dyStart) += da * dx(i + dxStart)
      i += 1
    }
  }
}
