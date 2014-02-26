package org.apache.spark.sql.execution

import parquet.io.InvalidRecordException
import parquet.schema.MessageType
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.hadoop.util.ContextUtil

import org.apache.spark.sql.catalyst.expressions.{Row, Attribute, Expression}

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.SparkSqlContext

/**
 * Parquet table scan operator. Imports the file that backs the given [[org.apache.spark.sql.execution.ParquetRelation]]
 * as a RDD[Row]. Only a stub currently.
 */
case class ParquetTableScan(
    attributes: Seq[Attribute],
    relation: ParquetRelation,
    columnPruningPred: Option[Expression])(
    @transient val sc: SparkSqlContext)
  extends LeafNode {

  /**
   * Runs this query returning the result as an RDD.
  */
  override def execute(): RDD[Row] = {
    val job = new Job()
    ParquetInputFormat.setReadSupportClass(job, classOf[org.apache.spark.sql.execution.RowReadSupport])
    val conf: Configuration = ContextUtil.getConfiguration(job)
    conf.set(
        RowReadSupport.PARQUET_ROW_REQUESTED_SCHEMA,
        ParquetTypesConverter.convertFromAttributes(attributes).toString)
    // TODO: add record filters, etc.
    sc.sparkContext.newAPIHadoopFile(
      relation.path,
      classOf[ParquetInputFormat[Row]],
      classOf[Void], classOf[Row],
      conf)
      .map(_._2)
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): ParquetTableScan = {
    val success = validateProjection(prunedAttributes)
    if(success)
      ParquetTableScan(prunedAttributes, relation, columnPruningPred)(sc)
    else
      this // TODO: add warning to log that column projection was unsuccessful?
  }

  /**
   * Evaluates a candidate projection by checking whether the candidate is a subtype of the
   * original type.
   *
   * @param projection The candidate projection.
   * @return True if the projection is valid, false otherwise.
   */
  private def validateProjection(projection: Seq[Attribute]): Boolean = {
    val original: MessageType = relation.parquetSchema
    val candidate: MessageType = ParquetTypesConverter.convertFromAttributes(projection)
    var retval = true
    try {
      original.checkContains(candidate)
    } catch {
      case e: InvalidRecordException => {
        retval = false
      }
    }
    retval
  }

  override def output: Seq[Attribute] = attributes
}

case class InsertIntoParquetTable(
    relation: ParquetRelation,
    child: SparkPlan)(
    @transient val sc: SparkSqlContext)
  extends UnaryNode {

  /**
   * Inserts all the rows in the Parquet file.
   */
  override def execute() = {

    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execition will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job()
    ParquetOutputFormat.setWriteSupportClass(job, classOf[org.apache.spark.sql.execution.RowWriteSupport])
    val conf = ContextUtil.getConfiguration(job)
    // TODO: move that to function in object
    conf.set(RowWriteSupport.PARQUET_ROW_SCHEMA, relation.parquetSchema.toString)

    // TODO: add checks: file exists, etc.
    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)
    fs.delete(fspath, true)

    JavaPairRDD.fromRDD(childRdd.map(Tuple2(null, _))).saveAsNewAPIHadoopFile(
      relation.path.toString,
      classOf[Void],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericRow],
      classOf[parquet.hadoop.ParquetOutputFormat[org.apache.spark.sql.catalyst.expressions.GenericRow]],
      conf)

    // From [[InsertIntoHiveTable]]:
    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.sparkContext.makeRDD(Nil, 1)
  }

  override def output = child.output
}
