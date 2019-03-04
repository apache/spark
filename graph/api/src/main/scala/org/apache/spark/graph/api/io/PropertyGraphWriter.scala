package org.apache.spark.graph.api.io

import org.apache.spark.graph.api.PropertyGraph
import org.apache.spark.sql.SaveMode

private[spark] case class WriterConfig(
  path: String,
  mode: SaveMode,
  source: String
)

case class PropertyGraphWriter(
  graph: PropertyGraph,
  config: WriterConfig
) {

  /**
    * Saves the content of the `PropertyGraph` at the specified path.
    */
  def save(path: String): Unit = {
    copy(config = config.copy(path = path)).save()
  }

  /**
    * Specifies the behavior when data or table already exists. Options include:
    * <ul>
    * <li>`SaveMode.Overwrite`: overwrite the existing data.</li>
    * <li>`SaveMode.Append`: append the data.</li>
    * <li>`SaveMode.Ignore`: ignore the operation (i.e. no-op).</li>
    * <li>`SaveMode.ErrorIfExists`: default option, throw an exception at runtime.</li>
    * </ul>
    */
  def mode(saveMode: SaveMode): PropertyGraphWriter = {
    copy(config = config.copy(mode = saveMode))
  }

  private[spark] def format(source: String): PropertyGraphWriter = {
    copy(config = config.copy(source = source))
  }

  /**
    * Saves the content of the `PropertyGraph` in Parquet format at the specified path.
    */
  def parquet(path: String): Unit = {
    format("parquet").save(path)
  }

  /**
    * Saves the content of the `DataFrame` in ORC format at the specified path.
    *
    * @note Currently, this method can only be used after enabling Hive support
    */
  def orc(path: String): Unit = {
    format("orc").save(path)
  }

  private[spark] def save(): Unit = {
    graph.cypherSession.writeGraph(graph, config)
  }

}
