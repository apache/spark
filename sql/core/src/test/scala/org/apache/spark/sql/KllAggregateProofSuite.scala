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

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DataType

private object KllAggregateProofCopies {
  val copies = new AtomicInteger
}

private case class KllProofInput(child: Expression) extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override protected def withNewChildInternal(newChild: Expression): Expression = {
    // Count proof reconstruction separately from lowering the scalar to a binding attribute.
    if (newChild.exists { case Literal(null, _) => true; case _ => false }) {
      KllAggregateProofCopies.copies.incrementAndGet()
    }
    copy(child = newChild)
  }
}

class KllAggregateProofExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction((FunctionIdentifier("kll_repeated_count"),
      new ExpressionInfo(classOf[Count].getName, "kll_repeated_count"),
      arguments => {
        val shared = (1 to 64).foldLeft(arguments.head) { (child, _) => KllProofInput(child) }
        Count(Seq.fill(32)(shared) ++ arguments.tail)
      }))
  }
}

class KllAggregateProofSuite extends QueryTest with SharedSparkSession {
  override protected def sparkConf = super.sparkConf.set("spark.sql.extensions",
    classOf[KllAggregateProofExtension].getName)

  test("single-pass injected aggregate arguments share their resolved scalar proof") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
      val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
          |FROM VALUES (0L), (1L), (2L) AS t(id))""".stripMargin
      KllAggregateProofCopies.copies.set(0)
      sql(s"SELECT builtin.kll_repeated_count($scalar) FROM VALUES (1) AS t(x)")
        .queryExecution.analyzed
      val copies = KllAggregateProofCopies.copies.get
      assert(copies <= 64 * 8, s"repeated shared aggregate-input proof copies=$copies")
      val error = intercept[AnalysisException] {
        sql(s"SELECT builtin.kll_repeated_count($scalar, rand(0)) FROM VALUES (1) AS t(x)")
          .queryExecution.analyzed
      }
      assert(error.getCondition == "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION")
    }
  }
}
