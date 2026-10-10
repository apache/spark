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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.Utils

/**
 * Benchmark for TreeNode.nodeName with synthetic leaf nodes, without a SparkSession.
 * To run this benchmark:
 * {{{
 *   build/sbt "catalyst/Test/runMain org.apache.spark.sql.catalyst.trees.TreeNodeBenchmark"
 * }}}
 */
object TreeNodeBenchmark extends BenchmarkBase {
  private abstract class SyntheticLeaf
    extends TreeNode[SyntheticLeaf] with LeafLike[SyntheticLeaf] {
    override def simpleStringWithNodeId(): String = nodeName
    override def verboseString(maxFields: Int): String = simpleString(maxFields)
  }

  private case class SyntheticNode() extends SyntheticLeaf
  private case class SyntheticExec() extends SyntheticLeaf

  @volatile private var result: String = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val iterations = 1_000_000
    val nodes: Seq[TreeNode[_]] = Seq(SyntheticNode(), SyntheticExec())
    nodes.foreach { node =>
      val name = s"nodeName: ${Utils.getSimpleName(node.getClass)}"
      runBenchmark(name) {
        val benchmark = new Benchmark(name, iterations, output = output)
        benchmark.addCase("String.replaceAll") { _ =>
          var i = 0
          while (i < iterations) {
            result = Utils.getSimpleName(node.getClass).replaceAll("Exec$", "")
            i += 1
          }
        }
        benchmark.addCase("TreeNode.nodeName") { _ =>
          var i = 0
          while (i < iterations) {
            result = node.nodeName
            i += 1
          }
        }
        benchmark.run()
      }
    }
  }
}
