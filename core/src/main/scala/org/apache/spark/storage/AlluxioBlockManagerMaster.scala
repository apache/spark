package org.apache.spark.storage

/**
  * Created by Chopin on 2018/3/27.
  */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging


/**
  * @see org.apache.spark.storage.TachyonBlockManager
  */
private[spark] class AlluxioBlockManagerMaster extends Logging {

  private var chroot: Path = _
  private var fs : FileSystem = _

  override def toString = "ExternalBlockStore-Alluxio"

  def init(conf: SparkConf): Unit = {

    val masterUrl = conf.get(ExternalBlockStore.MASTER_URL, "alluxio://localhost:19998")
    val storeDir = conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_alluxio")
    val subDirsPerAlluxio = conf.getInt(
      "spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR.toInt)
    val folderName = conf.get(ExternalBlockStore.FOLD_NAME)


    val master = new Path(masterUrl)
    chroot = new Path(master, s"$storeDir/$folderName/" + conf.getAppId )
    fs = master.getFileSystem(new Configuration)
    if(!fs.exists(chroot))
      fs.mkdirs(chroot)
    mkdir(subDirsPerAlluxio)
  }

  def delete(): Unit ={
    try{
      if(fs.exists(chroot)){
        logInfo(s"Test log: delete $chroot")
        fs.delete(chroot,true)
      }
    }catch {
      case _ =>
        logWarning(s"$chroot has been deleted")
    }finally
      fs.close()
  }

  def mkdir(n: Int): Unit = {
    for( i <- 0 until n ){
      val path = new Path(chroot,i.toString+s"/$i")
      if(!fs.exists(path))
        fs.mkdirs(path)
    }
  }

}