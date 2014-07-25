package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about an [[org.apache.spark.Accumulable]] modified during a task or stage.
 */
@DeveloperApi
class AccumulableInfo (
    val id: Long,
    val name: String,
    val update: Option[String], // represents a partial update within a task
    val value: String) { }

object AccumulableInfo {
  def apply(id: Long, name: String, update: Option[String], value: String) =
    new AccumulableInfo(id, name, update, value)

  def apply(id: Long, name: String, value: String) = new AccumulableInfo(id, name, None, value)
}
