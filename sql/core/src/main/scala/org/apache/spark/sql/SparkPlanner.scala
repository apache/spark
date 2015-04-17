package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.DataSourceStrategy


protected[sql] class SparkPlanner(val sqlContext: SQLContext) extends SparkStrategies {

  def codegenEnabled: Boolean = sqlContext.conf.codegenEnabled

  def numPartitions: Int = sqlContext.conf.numShufflePartitions

  def strategies: Seq[Strategy] =
    sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
        DDLStrategy ::
        TakeOrdered ::
        HashAggregation ::
        LeftSemiJoin ::
        HashJoin ::
        InMemoryScans ::
        ParquetOperations ::
        BasicOperators ::
        CartesianProduct ::
        BroadcastNestedLoopJoin :: Nil)


}
