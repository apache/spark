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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.streaming.{FlowAssigned, Unassigned, UserProvided}
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.types.IntegerType

/**
 * Unit tests for the NamedStreamingRelation wrapper node.
 */
class NamedStreamingRelationSuite extends SparkFunSuite {

  private def createMockPlan(): LocalRelation = {
    LocalRelation(AttributeReference("id", IntegerType)())
  }

  test("Unassigned sourceIdentifyingName") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)

    assert(wrapper.child eq plan)
    assert(wrapper.sourceIdentifyingName == Unassigned)
  }

  test("withUserProvidedName sets UserProvided name") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)
    val named = wrapper.withUserProvidedName(Some("my_source"))

    assert(named.sourceIdentifyingName == UserProvided("my_source"))
  }

  test("withUserProvidedName(None) returns same instance") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)
    val result = wrapper.withUserProvidedName(None)

    assert(result eq wrapper)
  }

  test("isStreaming returns true") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)

    assert(wrapper.isStreaming)
  }

  test("has NAMED_STREAMING_RELATION tree pattern") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)

    assert(wrapper.nodePatterns.contains(TreePattern.NAMED_STREAMING_RELATION))
  }

  test("resolved is false until unwrapped") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, UserProvided("test"))

    // Even with a named child, wrapper stays unresolved
    assert(!wrapper.resolved)
  }

  test("output delegates to child") {
    val plan = createMockPlan()
    val wrapper = NamedStreamingRelation(plan, Unassigned)

    assert(wrapper.output == plan.output)
  }

  test("pattern matching on sourceIdentifyingName variants") {
    val plan = createMockPlan()

    val userProvided = NamedStreamingRelation(plan, UserProvided("test"))
    val flowAssigned = NamedStreamingRelation(plan, FlowAssigned("0"))
    val unassigned = NamedStreamingRelation(plan, Unassigned)

    def extractName(wrapper: NamedStreamingRelation): Option[String] = {
      wrapper.sourceIdentifyingName match {
        case UserProvided(n) => Some(n)
        case FlowAssigned(n) => Some(n)
        case Unassigned => None
      }
    }

    assert(extractName(userProvided).contains("test"))
    assert(extractName(flowAssigned).contains("0"))
    assert(extractName(unassigned).isEmpty)
  }

  test("toString includes sourceIdentifyingName") {
    val plan = createMockPlan()

    val userNamed = NamedStreamingRelation(plan, UserProvided("my_source"))
    val flowNamed = NamedStreamingRelation(plan, FlowAssigned("0"))
    val unnamed = NamedStreamingRelation(plan, Unassigned)

    assert(userNamed.toString.contains("name=\"my_source\""))
    assert(flowNamed.toString.contains("name=\"0\""))
    assert(unnamed.toString.contains("name=<Unassigned>"))
  }
}
