package org.apache.spark.graph.api

import org.apache.spark.graph.api.io.{PropertyGraphReader, PropertyGraphWriter, ReaderConfig, WriterConfig}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Allows for creating [[PropertyGraph]] instances and running Cypher-queries on them.
  */
trait CypherSession {

  def sparkSession: SparkSession

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph [[PropertyGraph]] on which the query is executed
    * @param query Cypher query to execute
    */
  def cypher(graph: PropertyGraph, query: String): CypherResult

  /**
    * Executes a Cypher query on the given input graph.
    *
    * @param graph      [[PropertyGraph]] on which the query is executed
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    */
  def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult

  /**
    * Creates a [[PropertyGraph]] from a sequence of [[NodeFrame]]s and [[RelationshipFrame]]s.
    * At least one [[NodeFrame]] has to be provided.
    *
    * @param nodes         [[NodeFrame]]s that define the nodes in the graph
    * @param relationships [[RelationshipFrame]]s that define the relationships in the graph
    */
  def createGraph(nodes: Seq[NodeFrame], relationships: Seq[RelationshipFrame] = Seq.empty): PropertyGraph

  /**
    * Creates a [[PropertyGraph]] from nodes and relationships.
    *
    * The given DataFrames need to adhere to column naming conventions:
    *
    * {{{
    * Id column:        `$ID`            (nodes and relationships)
    * SourceId column:  `$SOURCE_ID`     (relationships)
    * TargetId column:  `$TARGET_ID`     (relationships)
    *
    * Label columns:    `:{LABEL_NAME}`  (nodes)
    * RelType columns:  `:{REL_TYPE}`    (relationships)
    *
    * Property columns: `{Property_Key}` (nodes and relationships)
    * }}}
    *
    * @param nodes node [[DataFrame]]
    * @param relationships relationship [[DataFrame]]
    */
  def createGraph(nodes: DataFrame, relationships: DataFrame): PropertyGraph

  /**
    * Returns a [[PropertyGraphWriter]] for `graph`.
    */
  def write(graph: PropertyGraph): PropertyGraphWriter = PropertyGraphWriter(graph, WriterConfig(
    path = ".",
    SaveMode.ErrorIfExists,
    sparkSession.sessionState.conf.defaultDataSourceName)
  )

  /**
    * Returns a [[PropertyGraphReader]].
    */
  def read: PropertyGraphReader = PropertyGraphReader(this, ReaderConfig(
    path = ".",
    sparkSession.sessionState.conf.defaultDataSourceName))

  private[spark] def readGraph(config: ReaderConfig): PropertyGraph

  private[spark] def writeGraph(graph: PropertyGraph, config: WriterConfig): Unit

}
