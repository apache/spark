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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer


private[mllib] class FeatureIndex extends Serializable {
  var maxid: Int = 0
  var alpha: ArrayBuffer[Double] = ArrayBuffer[Double]()
  var alpha_float: ArrayBuffer[Float] = ArrayBuffer[Float]()
  var cost_factor: Double = 0.0
  var xsize: Int = 0
  var check_max_xsize: Boolean = false
  var max_xsize: Int = 0
  var unigram_templs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var bigram_templs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var y: ArrayBuffer[String] = ArrayBuffer[String]()
  var templs: String = new String
  var dic: scala.collection.mutable.Map[String, (Int, Int)] =
    scala.collection.mutable.Map[String, (Int, Int)]()
  val kMaxContextSize: Int = 8
  val BOS = Vector[String]("_B-1", "_B-2", "_B-3", "_B-4",
    "_B-5", "_B-6", "_B-7", "_B-8")
  val EOS = Vector[String]("_B+1", "_B+2", "_B+3", "_B+4",
    "_B+5", "_B+6", "_B+7", "_B+8")
  val featureCache: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  val featureCacheH: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  @transient var sc: SparkContext = _

  def getFeatureCacheIdx(fVal: Int): Int = {
    var i: Int = 0
    while (i < featureCache.size) {
      if (featureCache(i) == fVal) {
        return i
      }
      i += 1
    }
    0
  }

  def getFeatureCache(): ArrayBuffer[Int] = {
    featureCache
  }

  def getFeatureCacheH(): ArrayBuffer[Int] = {
    featureCacheH
  }

  /**
   * Read one template file
   * @param lines the unit template file
   */
  def openTemplate(lines: Array[String]): Unit = {
    var i: Int = 0
    while (i < lines.length) {
      if (lines(i).charAt(0) == 'U') {
        unigram_templs += lines(i)
      } else if (lines(i).charAt(0) == 'B') {
        bigram_templs += lines(i)
      }
      i += 1
    }
    make_templs()
  }

  /**
   * Parse the feature file. If Sentences or paragraphs are defined as a unit
   * for processing, they should be saved in a string. Multiple units are saved
   * in the RDD.
   * @param lines the unit source file
   * @return
   */
  def openTagSet(lines: Array[String]): FeatureIndex = {
    var lineHead = lines(0).charAt(0)
    var tag: Array[String] = null
    var i: Int = 0
    var max: Int = 0
    var j: Int = 1
    while (i < lines.length) {
      lineHead = lines(i).charAt(0)
      if (lineHead != '\0' && lineHead != ' ' && lineHead != '\t') {
        tag = lines(i).split('|')
        if (tag.length > max) {
          max = tag.length
        }
        y.append(tag(tag.length - 1))
      }
      i += 1
    }
    i = 0
    while (i < y.size) {
      while (j < y.size) {
        if (y(i) == y(j)) {
          y.remove(j)
        }
        while (j < y.size && y(i) == y(j)) {
          y.remove(j)
        }
        j += 1
      }
      i += 1
      j = i + 1
    }
    xsize = max - 1
    this
  }

  def make_templs(): Unit = {
    var i: Int = 0
    while (i < unigram_templs.length) {
      templs += unigram_templs(i)
      i += 1
    }
    while (i < bigram_templs.length) {
      templs += bigram_templs(i)
      i += 1
    }
  }

  def shrink(freq: Int): Unit = {
    var newMaxId: Int = 0
    val key: String = null
    val count: Int = 0
    val currId: Int = 0

    if (freq > 1) {
      while (dic.iterator.next() != null) {
        dic.getOrElse(key, (currId, count))
        if (count > freq) {
          dic.getOrElseUpdate(key, (newMaxId, count))
        }

        if (key.toString.charAt(0) == 'U') {
          newMaxId += y.size
        } else {
          newMaxId += y.size * y.size
        }
      }
      maxid = newMaxId
    }
  }

  /**
   * Set node relationship and its feature index.
   * Node represents a word.
   */
  def rebuildFeatures(tagger: Tagger): Unit = {
    var cur: Int = 0
    var i: Int = 0
    var j: Int = 0
    var fid = tagger.feature_id
    var nd = new Node

    while (cur < tagger.x.size) {
      val nodeList: ArrayBuffer[Node] = new ArrayBuffer[Node]()
      tagger.node.append(nodeList)
      while (i < tagger.ysize) {
        nd = new Node
        nd.x = cur
        nd.y = i
        nd.fvector = featureCacheH(fid)
        nodeList.append(nd)
        i += 1
      }
      i = 0
      fid += 1
      tagger.node.update(cur, nodeList)
      cur += 1
    }
    cur = 1
    i = 0

    while (cur < tagger.x.size) {
      while (j < tagger.ysize) {
        while (i < tagger.ysize) {
          val path: Path = new Path
          path.add(tagger.node(cur - 1)(j), tagger.node(cur)(i))
          path.fvector = featureCacheH(fid)
          i += 1
        }
        i = 0
        j += 1
      }
      j = 0
      cur += 1
    }
  }

  /**
   * Build feature index
   */
  def buildFeatures(tagger: Tagger): Unit = {
    var os: String = null
    var id: Int = 0
    var cur: Int = 0
    var it: Int = 0
    featureCacheH.append(0)
    while (cur < tagger.x.size) {
      while (it < unigram_templs.length) {
        os = applyRule(unigram_templs(it), cur, tagger)
        id = getId(os)
        featureCache.append(id)
        it += 1
      }
      featureCache.append(-1)
      featureCacheH.append(maxid)
      cur += 1
      it = 0
    }
    it = 0
    cur = 1
    while (cur < tagger.x.size) {
      while (it < bigram_templs.length) {
        os = applyRule(bigram_templs(it), cur, tagger)
        id = getId(os)
        featureCache.append(id)
        it += 1
      }
      featureCache.append(-1)
      featureCacheH.append(maxid)
      cur += 1
      it = 0
    }
  }

  def getId(src: String): Int = {
    var n: Int = maxid
    var idx: Int = 0
    var fid: Int = 0
    if(src == null) {
      return 0
    }
    if (dic.get(src).isEmpty) {
      dic.update(src, (maxid, 1))
      n = maxid
      if (src.charAt(0) == 'U') {
        // Unigram
        maxid += y.size
      }
      else {
        // Bigram
        maxid += y.size * y.size
      }
      return n
    }
    else {
      idx = dic.get(src).get._2
      idx += 1
      fid = dic.get(src).get._1
      dic.update(src, (fid, idx))
      return fid
    }
  }

  def applyRule(src: String, idx: Int, tagger: Tagger): String = {
    var dest: String = ""
    var r: String = ""
    var i: Int = 0
    while (i < src.length) {
      if (src.charAt(i) == '%') {
        if (src.charAt(i + 1) == 'x') {
          r = getIndex(src.substring(i + 2), idx, tagger)
          if (r == null) {
            return null
          }
          dest += r
        }
      } else {
        dest += src.charAt(i)
      }
      i += 1
    }
    dest
  }

  def getIndex(src: String, pos: Int, tagger: Tagger): String = {
    var neg: Int = 1
    var col: Int = 0
    var row: Int = 0
    var idx: Int = 0
    var rtn: String = null
    var encol: Boolean = false
    var i: Int = 0
    if (src.charAt(0) != '[') {
      return null
    }
    i += 1
    if (src.charAt(1) == '-') {
      neg = -1
      i += 1
    }
    while (i < src.length) {
      if (src.charAt(i) - '0' <= 9 && src.charAt(i) - '0' >= 0) {
        if (!encol) {
          row = 10 * row + (src.charAt(i) - '0')
        } else {
          col = 10 * col + (src.charAt(i) - '0')
        }
      } else if (src.charAt(i) == ',') {
        encol = true
      } else if (src.charAt(i) == ']') {
        i = src.length // break
      }
      i += 1
    }
    row *= neg
    if (row < -kMaxContextSize || row > kMaxContextSize ||
      col < 0 || col >= xsize) {
      return null
    }

    max_xsize = math.max(max_xsize, col + 1)

    idx = pos + row
    if (idx < 0) {
      return BOS(-idx - 1)
    }
    if (idx >= tagger.x.size) {
      return EOS(idx - tagger.x.size)
    }
    tagger.x(idx)(col)
  }

  def setAlpha(_alpha: ArrayBuffer[Double]): Unit = {
    alpha = _alpha
  }

  def initAlpha(size: Int): Unit = {
    var i: Int = 0
    while (i <= size + 20) {
      alpha.append(0.0)
      i += 1
    }
  }

  def calcCost(n: Node): Node = {
    var c: Float = 0
    var cd: Double = 0.0
    var idx: Int = getFeatureCacheIdx(n.fvector)

    n.cost = 0.0
    if (alpha_float.nonEmpty) {
      while (featureCache(idx) != -1) {
        c += alpha_float(featureCache(idx) + n.y)
        n.cost = c
        idx += 1
      }
    } else if (alpha.nonEmpty) {
      while (featureCache(idx) != -1) {
        cd += alpha(featureCache(idx) + n.y)
        n.cost = cd
        idx += 1
      }
    }
    n
  }

  def calcCost(p: Path): Path = {
    var c: Float = 0
    var cd: Double = 0.0
    var idx: Int = getFeatureCacheIdx(p.fvector)
    p.cost = 0.0
    if (alpha_float.nonEmpty) {
      while (featureCache(idx) != -1) {
        c += alpha_float(featureCache(idx) +
          p.lnode.y * y.size + p.rnode.y)
        p.cost = c
        idx += 1
      }
    } else if (alpha.nonEmpty) {
      while (featureCache(idx) != -1) {
        cd += alpha(featureCache(idx) +
          p.lnode.y * y.size + p.rnode.y)
        p.cost = cd
        idx += 1
      }
    }
    p
  }

  def saveModelTxt: Array[String] = {
    var y_str: String = ""
    var i: Int = 0
    var template: String = ""
    val keys: ArrayBuffer[String] = new ArrayBuffer[String]()
    val values: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val contents: ArrayBuffer[String] = new ArrayBuffer[String]
    while (i < y.size) {
      y_str += y(i)
      y_str += '\0'
      i += 1
    }
    i = 0
    while (i < unigram_templs.size) {
      template += unigram_templs(i)
      template += "\0"
      i += 1
    }
    i = 0
    while (i < bigram_templs.size) {
      template += bigram_templs(i)
      template += "\0"
      i += 1
    }
    while ((y_str.length + template.length) % 4 != 0) {
      template += "\0"
    }

    dic.foreach { (pair) => keys.append(pair._1) }
    dic.foreach { (pair) => values.append(pair._2._1) }

    contents.append("maxid=" + maxid)
    contents.append("xsize=" + xsize)
    contents.append(y_str)
    contents.append(template)
    i = 0
    while (i < keys.size) {
      contents.append(keys(i) + " " + values(i))
      i += 1
    }
    i = 0
    while (i < maxid) {
      contents.append(alpha(i).toString)
      i += 1
    }
    contents.toArray
  }

  def saveModel: Array[String] = {
    val contents: ArrayBuffer[String] = new ArrayBuffer[String]
    var i: Int = 0

    while (i < featureCache.size) {
      contents.append(featureCache(i).toString)
      i += 1
    }
    contents.append("FeatureCacheHeader")
    i = 0
    while (i < featureCacheH.size) {
      contents.append(featureCacheH(i).toString)
      i += 1
    }
    i = 0
    contents.append("Alpha")
    while (i < maxid) {
      contents.append(alpha(i).toString)
      i += 1
    }
    contents.append("Trace")
    contents.appendAll(saveModelTxt)
    contents.toArray
  }

  def openFromArray(contents: Array[String]): Unit = {
    var i: Int = 0
    var readFCache: Boolean = true
    var readFCacheH: Boolean = false
    var readAlpha: Boolean = false
    while (i < contents.length) {
      if (contents(i) == "FeatureCacheHeader") {
        readFCache = false
        readFCacheH = true
        i += 1
      } else if (contents(i) == "Alpha") {
        readAlpha = true
        readFCacheH = false
        i += 1
      } else if (contents(i) == "Trace") {
        i = contents.length + 1 // break
        readFCache = false
        readFCacheH = false
        readAlpha = false
      }
      if (readFCache) {
        featureCache.append(contents(i).toInt)
      } else if (readFCacheH) {
        featureCacheH.append(contents(i).toInt)
      } else if (readAlpha) {
        alpha.append(contents(i).toDouble)
      }
      i += 1
    }
  }
}
