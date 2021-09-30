package org.apache.spark.sql.internal

import java.util.{Map => JMap}

import org.apache.spark.TaskContext
import org.apache.spark.internal.config.{ConfigEntry, ConfigProvider, ConfigReader}

/**
 * A readonly SQLConf that will be created by tasks running at the executor side. It reads the
 * configs from the local properties which are propagated from driver to executors.
 */
class ReadOnlySQLConf(context: TaskContext) extends SQLConf {

  @transient override val settings: JMap[String, String] = {
    context.getLocalProperties.asInstanceOf[JMap[String, String]]
  }

  @transient override protected val reader: ConfigReader = {
    new ConfigReader(new TaskContextConfigProvider(context))
  }

  override protected def setConfWithCheck(key: String, value: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def unsetConf(key: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  override def clone(): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }

  override def copy(entries: (ConfigEntry[_], Any)*): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }
}

class TaskContextConfigProvider(context: TaskContext) extends ConfigProvider {
  override def get(key: String): Option[String] = Option(context.getLocalProperty(key))
}
