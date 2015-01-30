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

package org.apache.spark.mllib.fpm

import scala.collection.mutable.{ListBuffer, ArrayBuffer, Map}

class FPTree extends Serializable {

  val root: FPTreeNode = new FPTreeNode(null, 0)

  def add(transaction: Array[String]): this.type = {
    var index = 0
    val size = transaction.size
    var curr = root
    while (index < size) {
      if (curr.children.contains(transaction(index))) {
        val node = curr.children(transaction(index))
        node.count = node.count + 1
        curr = node
      } else {
        val newNode = new FPTreeNode(transaction(index), 1)
        newNode.parent = curr
        curr.children(transaction(index)) = newNode
        curr = newNode
      }
      index = index + 1
    }

    // TODO: in oder to further reduce the amount of data for shuffle,
    // remove the same pattern which has the same hash number
    this
  }

  /**
   * merge with the input tree
   * @param tree the tree to merge
   * @return tree after merge
   */
  def merge(tree: FPTree): this.type = {
    // merge two trees recursively to remove all duplicated nodes
    mergeTree(this.root, tree.root)
    this
  }

  /**
   * merge two trees from their root node
   * @param tree1 root node of the tree one
   * @param tree2 root node of the tree two
   * @return root node after merge
   */
  private def mergeTree(tree1: FPTreeNode, tree2: FPTreeNode): FPTreeNode = {
    // firstly merge two roots, then iterate on the second tree, merge all children of it to the first tree
    require(tree1 != null)
    require(tree2 != null)
    if (!tree2.isRoot) {
      require(tree1.item.equals(tree2.item))
      tree1.count = tree1.count + tree2.count
    }
    if (!tree2.isLeaf) {
      val it = tree2.children.iterator
      while (it.hasNext) {
        val node = mergeSubTree(tree1, it.next()._2)
        tree1.children(node.item) = node
        node.parent = tree1
      }
    }
    tree1
  }

  /**
   * merge the second tree into the children of the first tree, if there is a match
   * @param tree1Root root node of the tree one
   * @param subTree2 the child of the tree two
   * @return root node after merge
   */
  private def mergeSubTree(tree1Root: FPTreeNode, subTree2: FPTreeNode): FPTreeNode = {
    if (tree1Root.children.contains(subTree2.item)) {
      mergeTree(tree1Root.children(subTree2.item), subTree2)
    } else {
      subTree2
    }
  }

  /**
   * Generate all frequent patterns by mining the FPTree recursively
   * @param minCount minimal count
   * @param suffix key of this tree
   * @return
   */
  def mine(minCount: Double, suffix: String): Array[(Array[String], Long)] = {
    val condPattBase = expandFPTree(this)
    mineFPTree(condPattBase, minCount, suffix)
  }

  /**
   * This function will walk through the tree and build all conditional pattern base out of it
   * @param tree the tree to expand
   * @return conditional pattern base
   */
  private def expandFPTree(tree: FPTree): ArrayBuffer[ArrayBuffer[String]] = {
    var output: ArrayBuffer[ArrayBuffer[String]] = null
    if (!tree.root.isLeaf) {
      val it = tree.root.children.iterator
      while (it.hasNext) {
        val childOuput = expandFPTreeNode(it.next()._2)
        if (output == null) output = childOuput else output ++= childOuput
      }
    }
    output
  }

  /**
   * Expand from the input node
   * @param node tree node
   * @return conditional pattern base
   */
  private def expandFPTreeNode(node: FPTreeNode): ArrayBuffer[ArrayBuffer[String]] = {
    // Iterate on all children and build the output recursively
    val output = new ArrayBuffer[ArrayBuffer[String]]()
    for (i <- 0 to node.count - 1) {
      output.append(ArrayBuffer[String](node.item))
    }
    val it = node.children.iterator
    var i = 0
    while (it.hasNext) {
      val child = it.next()
      val childOutput = expandFPTreeNode(child._2)
      require(childOutput.size <= output.size)
      for (buffer <- childOutput) {
        output(i) ++= buffer
        i = i + 1
      }
    }
    output
  }
  
  /**
   * Generate all frequent patterns by combinations of condition pattern base.
   * This implementation is different from classical fp-growth algorithm which generate
   * FPTree recursively.
   *
   * @param condPattBase condition pattern base
   * @param minCount the minimum count
   * @param suffix key of the condition pattern base
   * @return frequent item set
   */
  private def mineFPTree(
      condPattBase: ArrayBuffer[ArrayBuffer[String]],
      minCount: Double,
      suffix: String): Array[(Array[String], Long)] = {
    // frequently item
    val key = suffix
    // the set of construction CPFTree
    val value = condPattBase

    // tree step.start 2th
    var k = 1
    // save all frequently item set
    val fimSetBuffer = ArrayBuffer[(String, Long)]()
    // save step k's lineComList temp value to next step k+1 compute combinations
    var lineComListTempBuffer = ArrayBuffer[String]()
    // loop the data set from 1 to k while k>0
    while (k > 0) {
      // save step k's lineComList temp value
      var lineComListBuffer = ListBuffer[List[String]]()
      // loop every value to combinations while each value length >= k
      for (v <- value) {
        val vLen = v.length
        if (vLen >= k) {
          // calculate each value combinations while each value k == 2
          if (k == 1) {
            val lineCom = v.toList.combinations(k)
            lineComListBuffer ++= lineCom.toList
          } else {
            /* if each value length > k,it need calculate the intersect of each value & before combinations */
            val union_lineComListTemp2v = v intersect lineComListTempBuffer.toArray.array
            // calculate each value combinations after intersect
            if (union_lineComListTemp2v.length >= k) {
              val lineCom = union_lineComListTemp2v.toList.combinations(k)
              lineComListBuffer ++= lineCom.toList
            }
          }
        }
      }

      var lineComList: Array[(String, Long)] = null
      // reset
      lineComListTempBuffer = ArrayBuffer[String]()
      // calculate frequent item set
      if (lineComListBuffer != null || lineComListBuffer.size != 0) {
        val lineComListTemp = lineComListBuffer
            .map( v => ( (v :+ key).sortWith(_ > _),1) )
            .groupBy(_._1)
            .map(v => (v._1,v._2.length))
            .filter(_._2 >= minCount)
        if ( lineComListTemp != null || lineComListTemp.size != 0) {
          lineComList = lineComListTemp
              .map(v => (v._1.mkString(" "), v._2.toLong))
              .toArray
          fimSetBuffer ++= lineComList
          for (lcl <- lineComList) {
            lineComListTempBuffer ++= lcl._1.split(" ")
          }
        }
      }
      // reset k value
      if (lineComList == null || lineComList.length == 0) {
        k = 0
      } else {
        k = k + 1
      }
    }
    val fimSetArray = fimSetBuffer
        .map(v => (v._1.split(" "), v._2))
        .toArray
    fimSetArray
  }
}

class FPTreeNode(val item: String, var count: Int) extends Serializable {
  var parent: FPTreeNode = null
  val children: Map[String, FPTreeNode] = Map[String, FPTreeNode]()
  def isLeaf: Boolean = children.size == 0
  def isRoot: Boolean = parent == null
}
