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
package org.apache.spark.sql.catalyst

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/**
 * A collection of implicit conversions that create a DSL for constructing connect protos.
 *
 * {{{
 *   scala> import org.apache.spark.sql.connect.plans.DslLogicalPlan
 *
 *   // Standard way to construct connect proto
 *   scala> import org.apache.spark.connect.proto
 *   scala> :paste
 *   // Entering paste mode (ctrl-D to finish)
 *   val connectTestRelation =
 *    proto.Relation.newBuilder()
 *     .setRead(
 *       proto.Read.newBuilder()
 *       .setNamedTable(proto.Read.NamedTable.newBuilder().addParts("student").build())
 *       .build())
 *     .build()
 *  // Exiting paste mode, now interpreting.
 *    connectTestRelation: org.apache.spark.connect.proto.Relation =
 *     read {
 *      named_table {
 *        parts: "student"
 *      }
 *    }
 *
 *   // Now we can apply select on the proto relation above
 *   scala> import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
 *   scala> connectTestRelation.select(UnresolvedAttribute(Seq("id")))
 *   res14: org.apache.spark.connect.proto.Relation =
 *    project {
 *      input {
 *        read {
 *          named_table {
 *            parts: "student"
 *          }
 *        }
 *      }
 *      expressions {
 *        unresolved_attribute {
 *          parts: "id"
 *        }
 *      }
 *    }
 *
 * }}}
 *
 */
package object connect {

  object plans { // scalastyle:ignore
    implicit class DslLogicalPlan(val logicalPlan: proto.Relation) {
      def select(exprs: Expression*): proto.Relation = {
        val namedExpressions = exprs.map {
          case e: NamedExpression => e
          case e => UnresolvedAlias(e)
        }

        proto.Relation.newBuilder().setProject(
          proto.Project.newBuilder()
            .setInput(logicalPlan)
            .addExpressions(
              proto.Expression.newBuilder()
                .setUnresolvedAttribute(
                  proto.Expression.UnresolvedAttribute.newBuilder()
                    .addAllParts(namedExpressions.map(e => e.name).asJava)
                    .build()
                ).build()
            ).build()
        ).build()
      }
    }
  }
}
