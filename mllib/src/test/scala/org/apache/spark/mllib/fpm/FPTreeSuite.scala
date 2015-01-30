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

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class FPTreeSuite extends FunSuite with MLlibTestSparkContext {

  test("add transaction to tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("b"))
    FPGrowthSuite.printTree(tree)

    assert(tree.root.children.size == 2)
    assert(tree.root.children.contains("a"))
    assert(tree.root.children("a").item.equals("a"))
    assert(tree.root.children("a").count == 2)
    assert(tree.root.children.contains("b"))
    assert(tree.root.children("b").item.equals("b"))
    assert(tree.root.children("b").count == 1)
    var child = tree.root.children("a")
    assert(child.children.size == 1)
    assert(child.children.contains("b"))
    assert(child.children("b").item.equals("b"))
    assert(child.children("b").count == 2)
    child = child.children("b")
    assert(child.children.size == 2)
    assert(child.children.contains("c"))
    assert(child.children.contains("y"))
    assert(child.children("c").item.equals("c"))
    assert(child.children("y").item.equals("y"))
    assert(child.children("c").count == 1)
    assert(child.children("y").count == 1)
  }

  test("merge tree") {
    val tree1 = new FPTree
    tree1.add(Array[String]("a", "b", "c"))
    tree1.add(Array[String]("a", "b", "y"))
    tree1.add(Array[String]("b"))
    FPGrowthSuite.printTree(tree1)

    val tree2 = new FPTree
    tree2.add(Array[String]("a", "b"))
    tree2.add(Array[String]("a", "b", "c"))
    tree2.add(Array[String]("a", "b", "c", "d"))
    tree2.add(Array[String]("a", "x"))
    tree2.add(Array[String]("a", "x", "y"))
    tree2.add(Array[String]("c", "n"))
    tree2.add(Array[String]("c", "m"))
    FPGrowthSuite.printTree(tree2)

    val tree3 = tree1.merge(tree2)
    FPGrowthSuite.printTree(tree3)

    assert(tree3.root.children.size == 3)
    assert(tree3.root.children("a").count == 7)
    assert(tree3.root.children("b").count == 1)
    assert(tree3.root.children("c").count == 2)
    val child1 = tree3.root.children("a")
    assert(child1.children.size == 2)
    assert(child1.children("b").count == 5)
    assert(child1.children("x").count == 2)
    val child2 = child1.children("b")
    assert(child2.children.size == 2)
    assert(child2.children("y").count == 1)
    assert(child2.children("c").count == 3)
    val child3 = child2.children("c")
    assert(child3.children.size == 1)
    assert(child3.children("d").count == 1)
    val child4 = child1.children("x")
    assert(child4.children.size == 1)
    assert(child4.children("y").count == 1)
    val child5 = tree3.root.children("c")
    assert(child5.children.size == 2)
    assert(child5.children("n").count == 1)
    assert(child5.children("m").count == 1)
  }

  /*
  test("expand tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("a", "b"))
    tree.add(Array[String]("a"))
    tree.add(Array[String]("b"))
    tree.add(Array[String]("b", "n"))

    FPGrowthSuite.printTree(tree)
    val buffer = tree.expandFPTree(tree)
    for (a <- buffer) {
      a.foreach(x => print(x + " "))
      println
    }
  }
  */

  test("mine tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("a", "b"))
    tree.add(Array[String]("a"))
    tree.add(Array[String]("b"))
    tree.add(Array[String]("b", "n"))

    FPGrowthSuite.printTree(tree)
    val buffer = tree.mine(3.0, "t")

    for (a <- buffer) {
      a._1.foreach(x => print(x + " "))
      print(a._2)
      println
    }
    val s1 = buffer(0)._1
    val s2 = buffer(1)._1
    val s3 = buffer(2)._1
    assert(s1(1).equals("a"))
    assert(s2(1).equals("b"))
    assert(s3(1).equals("b"))
    assert(s3(2).equals("a"))
    assert(buffer(0)._2 == 4)
    assert(buffer(1)._2 == 5)
    assert(buffer(2)._2 == 3)
  }
}
