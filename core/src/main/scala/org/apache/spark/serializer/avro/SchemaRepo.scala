package org.apache.spark.serializer.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.{Logging, SparkConf}

/**
 * Created by rotems on 12/6/15.
 */
/**
 * A schema repository for avro records.
 * This repo assumes that it is possible to extract the schemaId of a record from the record itself.
 * @param config sparkConf for configuration purposes
 */
abstract class SchemaRepo(config: SparkConf) {

  /**
   * Receive from repo an avro schema as string by its ID
   * @param schemaId - the schemaId
   * @return schema if found, none otherwise
   */
  def getRawSchema(schemaId : Long) : Option[String]

  /**
   * Extract schemaId from record.
   * @param record current avro record
   * @return schemaId if managed to extract, none otherwise
   */
  def extractSchemaId(record: GenericRecord) : Option[Long]

  /**
   * Checks whether the schema repository contains the following schemaId
   * @param schemaId - the schemaId
   * @return true if found in repo, false otherwise.
   */
  def contains(schemaId: Long) : Boolean

  /**
   * Get schema from repo using schemaId as Schema type
   * @param schemaId - the schemaId
   * @return schema if found, none otherwise
   */
  def getSchema(schemaId : Long) : Option[Schema] = {
    getRawSchema(schemaId) match {
      case Some(s) => Some(new Schema.Parser().parse(s))
      case None => None

    }
  }

}

object SchemaRepo extends Logging {
  val SCHEMA_REPO_KEY = "spark.kryo.avro.schema.repo"

  /**
   * Create a schemaRepo using SparkConf
   * @param conf - spark conf used to configure the repo.
   * @return the initiated SchemaRepo or None if anything goes wrong
   */
  def apply(conf: SparkConf) : Option[SchemaRepo]= {
    try {
      conf.getOption(SCHEMA_REPO_KEY) match {
        case Some(clazz) => Some(Class.forName(clazz).getConstructor(classOf[SparkConf])
          .newInstance(conf).asInstanceOf[SchemaRepo])
        case None => None
      }
    } catch {
      case t: Throwable =>
        log.error(s"Failed to build schema repo. ", t)
        None
    }
  }
}

/**
 * A dummy empty schema repository.
 */
object EmptySchemaRepo extends SchemaRepo(null) {

  override def getRawSchema(schemaId: Long): Option[String] = None

  override def extractSchemaId(record: GenericRecord): Option[Long] = None

  override def contains(schemaId: Long): Boolean = false
}
