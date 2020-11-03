
package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}

/**
 * A [[org.apache.spark.broadcast.Broadcast]] implementation that uses a BitTorrent-like
 * protocol to do a distributed transfer of the broadcasted data to the executors. Refer to
 * [[org.apache.spark.broadcast.TorrentBroadcast]] for more details.
 */
private[spark] class TorrentBroadcastFactory extends BroadcastFactory {

  override def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) { }

  override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T] = {
    new TorrentBroadcast[T](value_, id)
  }

  override def stop() { }

  /**
   * Remove all persisted state associated with the torrent broadcast with the given ID.
   * @param removeFromDriver Whether to remove state from the driver.
   * @param blocking Whether to block until unbroadcasted
   */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver, blocking)
  }
}
