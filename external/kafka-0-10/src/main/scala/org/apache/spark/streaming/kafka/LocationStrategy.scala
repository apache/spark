package org.apache.spark.streaming.kafka

import java.{ util => ju }

import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
 * Choice of how to schedule consumers for a given TopicPartition on an executor.
 * Kafka 0.10 consumers prefetch messages, so it's important for performance
 * to keep cached consumers on appropriate executors, not recreate them for every partition.
 * Choice of location is only a preference, not an absolute; partitions may be scheduled elsewhere.
 */
sealed trait LocationStrategy

/**
 * Use this only if your executors are on the same nodes as your Kafka brokers.
 */
case object PreferBrokers extends LocationStrategy {
  def instance = this
}

/**
 * Use this in most cases, it will consistently distribute partitions across all executors.
 */
case object PreferConsistent extends LocationStrategy {
  def instance = this
}

/**
 * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
 * Any TopicPartition not specified in the map will use a consistent location.
 */
case class PreferFixed(hostMap: ju.Map[TopicPartition, String]) extends LocationStrategy

object PreferFixed {
  def apply(hostMap: Map[TopicPartition, String]): PreferFixed = {
    PreferFixed(new ju.HashMap[TopicPartition, String](hostMap.asJava))
  }
}
