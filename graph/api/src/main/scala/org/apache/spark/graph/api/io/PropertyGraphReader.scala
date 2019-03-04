package org.apache.spark.graph.api.io

import org.apache.spark.graph.api.{CypherSession, PropertyGraph}

private[spark] case class ReaderConfig(
  path: String,
  source: String
)

case class PropertyGraphReader(
  session: CypherSession,
  config: ReaderConfig
) {

  /**
    * Saves the content of the `PropertyGraph` at the specified path.
    */
  def read(path: String): PropertyGraph = {
    session.readGraph(config.copy(path = path))
  }

  /**
    * Saves the content of the `DataFrame` in Parquet format at the specified path.
    */
  def parquet(path: String): PropertyGraph = {
    format("parquet").read(path)
  }

  /**
    * Saves the content of the `DataFrame` in ORC format at the specified path.
    *
    * @note Currently, this method can only be used after enabling Hive support
    */
  def orc(path: String): PropertyGraph = {
    format("orc").read(path)
  }

  /**
    * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
    */
  private[spark] def format(source: String): PropertyGraphReader = {
    copy(config = config.copy(source = source))
  }

}
