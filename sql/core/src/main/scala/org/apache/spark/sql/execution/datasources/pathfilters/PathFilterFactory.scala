package org.apache.spark.sql.execution.datasources.pathfilters

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object PathFilterFactory {
	PathFilterStrategies.register(ModifiedAfterFilter)
	PathFilterStrategies.register(ModifiedBeforeFilter)
	PathFilterStrategies.register(PathGlobFilter)
	def create(sparkSession: SparkSession, conf: Configuration, parameters: Map[String, String]): Iterable[FileIndexFilter]= {
		PathFilterStrategies.get(sparkSession, conf, parameters)
	}
}

