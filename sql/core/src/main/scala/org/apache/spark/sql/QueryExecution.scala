package org.apache.spark.sql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
 * :: DeveloperApi ::
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 */
@DeveloperApi
protected[sql] class QueryExecution(val sqlContext: SQLContext, val logical: LogicalPlan) {
  def assertAnalyzed(): Unit = sqlContext.analyzer.checkAnalysis(analyzed)

  lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)
  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    sqlContext.cacheManager.useCachedData(analyzed)
  }
  lazy val optimizedPlan: LogicalPlan = sqlContext.optimizer.execute(withCachedData)

  // TODO: Don't just pick the first one...
  lazy val sparkPlan: SparkPlan = {
    SparkPlan.currentContext.set(sqlContext)
    sqlContext.planner(optimizedPlan).next()
  }
  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = sqlContext.prepareForExecution.execute(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[Row] = executedPlan.execute()

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: Throwable => e.toString }

  def simpleString: String =
    s"""== Physical Plan ==
       |${stringOrError(executedPlan)}
      """.stripMargin.trim

  override def toString: String = {
    def output =
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}").mkString(", ")

    // TODO previously will output RDD details by run (${stringOrError(toRdd.toDebugString)})
    // however, the `toRdd` will cause the real execution, which is not what we want.
    // We need to think about how to avoid the side effect.
    s"""== Parsed Logical Plan ==
       |${stringOrError(logical)}
        |== Analyzed Logical Plan ==
        |${stringOrError(output)}
        |${stringOrError(analyzed)}
        |== Optimized Logical Plan ==
        |${stringOrError(optimizedPlan)}
        |== Physical Plan ==
        |${stringOrError(executedPlan)}
        |Code Generation: ${stringOrError(executedPlan.codegenEnabled)}
        |== RDD ==
      """.stripMargin.trim
  }
}

