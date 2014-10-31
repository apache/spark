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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute}

/**
 * A set of APIs for adding data sources to Spark SQL.
 */
package object sources {

  /**
   * Implemented by objects that produce relations for a specific kind of data source.  When
   * Spark SQL is given a DDL operation with a USING clause specified, this interface is used to
   * pass in the parameters specified by a user.
   *
   * Users may specify the fully qualified class name of a given data source.  When that class is
   * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
   * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
   * data source 'org.apache.spark.sql.json.DefaultSource'
   *
   * A new instance of this class with be instantiated each time a DDL call is made.
   */
  @DeveloperApi
  trait RelationProvider {
    /** Returns a new base relation with the given parameters. */
    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
  }

  /**
   * Represents a collection of tuples with a known schema.  Classes that extend BaseRelation must
   * be able to produce the schema of their data in the form of a [[StructType]]  In order to be
   * executed, a BaseRelation must also mix in at least one of the Scan traits.
   *
   * BaseRelations must also define a equality function that only returns true when the two
   * instances will return the same data.  This equality function is used when determining when
   * it is safe to substitute cached results for a given relation.
   */
  @DeveloperApi
  abstract class BaseRelation {
    def sqlContext: SQLContext
    def schema: StructType
  }

  /**
   * Mixed into a BaseRelation that can produce all of its tuples as an RDD of Row objects.
   */
  @DeveloperApi
  trait TableScan {
    self: BaseRelation =>

    def buildScan(): RDD[Row]
  }

  /**
   * Mixed into a BaseRelation that can eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects.
   */
  @DeveloperApi
  trait PrunedScan {
    self: BaseRelation =>

    def buildScan(requiredColumns: Seq[Attribute]): RDD[Row]
  }

  /**
   * Mixed into a BaseRelation that can eliminate unneeded columns and filter using selected
   * predicates before producing an RDD containing all matching tuples as Row objects.
   *
   * The pushed down filters are currently purely an optimization as they will all be evaluated
   * again.  This means it is safe to use them with methods that produce false positives such
   * as filtering partitions based on a bloom filter.
   */
  @DeveloperApi
  trait FilteredScan {
    self: BaseRelation =>

    def buildScan(
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row]
  }
}
