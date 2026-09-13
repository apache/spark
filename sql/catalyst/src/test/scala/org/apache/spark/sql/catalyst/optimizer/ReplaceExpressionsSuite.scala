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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Add, Coalesce, Literal, Nvl, RuntimeReplaceable}

class ReplaceExpressionsSuite extends SparkFunSuite {

  test("replace expands RuntimeReplaceable expressions") {
    val replaced = ReplaceExpressions.replace(new Nvl(Literal(1), Literal(2)))
    assert(!replaced.exists(_.isInstanceOf[RuntimeReplaceable]))
    assert(replaced == Coalesce(Seq(Literal(1), Literal(2))))
  }

  test("replace expands nested RuntimeReplaceable expressions") {
    val nested = new Nvl(new Nvl(Literal(1), Literal(2)), Literal(3))
    val replaced = ReplaceExpressions.replace(nested)
    assert(!replaced.exists(_.isInstanceOf[RuntimeReplaceable]))
    assert(replaced == Coalesce(Seq(Coalesce(Seq(Literal(1), Literal(2))), Literal(3))))
  }

  test("replace recurses into children of non-RuntimeReplaceable expressions") {
    val e = Add(Literal(1), new Nvl(Literal(2), Literal(3)))
    assert(ReplaceExpressions.replace(e) == Add(Literal(1), Coalesce(Seq(Literal(2), Literal(3)))))
  }

  test("replace leaves expressions without RuntimeReplaceable unchanged") {
    assert(ReplaceExpressions.replace(Literal(1)) == Literal(1))
  }
}
