package org.apache.spark.sql.execution.datasources.pathfilters

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

trait PathFilterObject {
  def get(sparkSession: SparkSession,
          configuration: Configuration,
          options: Map[String, String]): FileIndexFilter
  def strategy(): String
}

case object PathFilterStrategies {
  var cache = Iterable[PathFilterObject]()

  def get(sparkSession: SparkSession,
          conf: Configuration,
          options: Map[String, String]): Iterable[FileIndexFilter] =
    (options.keys)
      .map(option => {
        cache
          .filter(pathFilter => pathFilter.strategy() == option)
          .map(filter => filter.get(sparkSession, conf, options))
          .headOption
          .getOrElse(null)
      })

  def register(filter: PathFilterObject): Unit = {
    cache = cache.++(Iterable[PathFilterObject](filter))
  }
}
