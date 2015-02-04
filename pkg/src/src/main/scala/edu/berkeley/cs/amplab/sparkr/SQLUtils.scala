package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext}

object SQLUtils {
  def createSQLContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }
}
