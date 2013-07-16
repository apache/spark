package spark.storage

import com.codahale.metrics.{Gauge,MetricRegistry}

import spark.metrics.source.Source
import spark.storage._

private[spark] class BlockManagerSource(val blockManager: BlockManager) extends Source {
  val metricRegistry = new MetricRegistry()
  val sourceName = "BlockManager"

  metricRegistry.register(MetricRegistry.name("memory", "maxMem", "MBytes"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val maxMem = storageStatusList.map(_.maxMem).reduce(_ + _)
      maxMem / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("memory", "remainingMem", "MBytes"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val remainingMem = storageStatusList.map(_.memRemaining).reduce(_ + _)
      remainingMem / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("memory", "memUsed", "MBytes"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val maxMem = storageStatusList.map(_.maxMem).reduce(_ + _)
      val remainingMem = storageStatusList.map(_.memRemaining).reduce(_ + _)
      (maxMem - remainingMem) / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("disk", "diskSpaceUsed", "MBytes"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val diskSpaceUsed = storageStatusList
      	.flatMap(_.blocks.values.map(_.diskSize))
      	.reduceOption(_ + _)
      	.getOrElse(0L)

      diskSpaceUsed / 1024 / 1024
    }
  })
}
