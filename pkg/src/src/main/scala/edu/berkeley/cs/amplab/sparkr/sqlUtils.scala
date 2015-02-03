package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types.{StructType}

object sqlUtils {
  def createSQLContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }
}
