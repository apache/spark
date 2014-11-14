package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{SQLConf, Row, SQLContext}
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources._
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil

class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ParquetRelation2(parameters("path"))(sqlContext)
  }
}

case class Partition(partitioningKeys: Map[String, Any], files: Seq[String])

case class ParquetRelation2(
    path: String)(
    @transient val sqlContext: SQLContext) extends PrunedFilteredScan with Logging {

  def sparkContext = sqlContext.sparkContext

  val (partitionKeys: Seq[String], partitions: Seq[Partition]) = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val partValue = "([^=]+)=([^=]+)".r

    val folders = fs.listStatus(new Path(path)).filter(s => s.isDir).map(_.getPath.getName).map {
      case partValue(key, value) => (key, value)
    }

    if (folders.size > 0) {
      val partitionKeys = folders.map(_._1).distinct
      if (partitionKeys.size > 1) {
        sys.error(s"Too many distinct partition keys: $partitionKeys")
      }

      (partitionKeys.toSeq, folders.map { case (key, value) =>
        val files = s"$path/$key=$value" :: Nil
        Partition(Map(key -> value.toInt), files)
      }.toSeq)

    } else {
      (Nil, Partition(Map.empty, path :: Nil) :: Nil)
    }
  }

  val schema =
    StructType.fromAttributes(
      ParquetTypesConverter.readSchemaFromFile(
        new Path(partitions.head.files.head),
        None,  // TODO
        sqlContext.isParquetBinaryAsString))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val job = new Job(sparkContext.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])
    val conf: Configuration = ContextUtil.getConfiguration(job)

    val requestedSchema = StructType(requiredColumns.map(schema(_)))

    val partitionFilters = filters.collect {
      case e @ EqualTo(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr=$value")
        (p: Partition) => p.partitioningKeys(attr) == value

      case e @ In(attr, values) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr IN ${values.mkString("{", ",", "}")}")
        val set = values.toSet
        (p: Partition) => set.contains(p.partitioningKeys(attr))

      case e @ GreaterThan(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr > $value")
        (p: Partition) => p.partitioningKeys(attr).asInstanceOf[Int] > value.asInstanceOf[Int]

      case e @ GreaterThanOrEqual(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr >= $value")
        (p: Partition) => p.partitioningKeys(attr).asInstanceOf[Int] >= value.asInstanceOf[Int]

      case e @ LessThan(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr < $value")
        (p: Partition) => p.partitioningKeys(attr).asInstanceOf[Int] < value.asInstanceOf[Int]

      case e @ LessThanOrEqual(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr <= $value")
        (p: Partition) => p.partitioningKeys(attr).asInstanceOf[Int] <= value.asInstanceOf[Int]
    }

    val selectedPartitions = partitions.filter(p => partitionFilters.forall(_(p)))

    selectedPartitions.flatMap(_.files).foreach { curPath =>
      val qualifiedPath = {
        val path = new Path(curPath)
        path.getFileSystem(conf).makeQualified(path)
      }
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, qualifiedPath)
    }

    def fractionRead = selectedPartitions.size.toDouble / partitions.size.toDouble * 100
    logInfo(s"Reading $fractionRead% of $path partitions")

    // Store both requested and original schema in `Configuration`
    conf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(requestedSchema.toAttributes))
    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(schema.toAttributes))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    conf.set(
      SQLConf.PARQUET_CACHE_METADATA,
      sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true"))

    val baseRDD =
      new org.apache.spark.rdd.NewHadoopRDD(
        sparkContext,
        classOf[FilteringParquetRowInputFormat],
        classOf[Void],
        classOf[Row],
        conf)

    baseRDD.map(_._2)
  }

  override val sizeInBytes = {
    def calculateTableSize(fs: FileSystem, path: Path): Long = {
      val fileStatus = fs.getFileStatus(path)
      val size = if (fileStatus.isDir) {
        fs.listStatus(path).map(status => calculateTableSize(fs, status.getPath)).sum
      } else {
        fileStatus.getLen
      }

      size
    }
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    calculateTableSize(fs, new Path(path))
  }
}