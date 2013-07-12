package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.SparkContext
import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.storage.StorageLevel
import spark.scheduler.cluster.Schedulable

/*
 * Interface for get pools seq showing on Index or pool detail page
 */

private[spark] trait PoolSource {
  def getPools: Seq[Schedulable]
}

/*
 * Pool source for FIFO scheduler algorithm on Index page
 */
private[spark] class FIFOSource() extends PoolSource{
  def getPools: Seq[Schedulable] = {
    Seq[Schedulable]()
  }
}

/*
 * Pool source for Fair scheduler algorithm on Index page
 */
private[spark] class FairSource(sc: SparkContext) extends PoolSource{
  def getPools: Seq[Schedulable] = {
    sc.getPoolsInfo.toSeq
  }
}

/*
 * specific pool info for pool detail page
 */
private[spark] class PoolDetailSource(sc: SparkContext, poolName: String) extends PoolSource{
  def getPools: Seq[Schedulable] = {
    val pools = HashSet[Schedulable]()
    pools += sc.getPoolNameToPool(poolName)
    pools.toSeq
  }
}

/** Table showing list of pools */
private[spark] class PoolTable(listener: JobProgressListener) {

  var poolSource: PoolSource = null
  var poolToActiveStages: HashMap[String, HashSet[Stage]] = listener.poolToActiveStages

  def toNodeSeq: Seq[Node] = {
    poolTable(poolRow, poolSource.getPools)
  }

  def setPoolSource(poolSource: PoolSource) {
    this.poolSource = poolSource
  }

  //pool tables
  def poolTable(makeRow: (Schedulable, HashMap[String, HashSet[Stage]]) => Seq[Node], rows: Seq[Schedulable]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <td>Active Stages</td>
        <td>Running Tasks</td>
        <td>SchedulingMode</td>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, poolToActiveStages))}
      </tbody>
    </table>
  }

  def poolRow(p: Schedulable, poolToActiveStages: HashMap[String, HashSet[Stage]]): Seq[Node] = {
    <tr>
      <td><a href={"/stages/pool?poolname=%s".format(p.name)}>{p.name}</a></td>
      <td>{p.minShare}</td>
      <td>{p.weight}</td>
      <td>{poolToActiveStages.getOrElseUpdate(p.name, new HashSet[Stage]()).size}</td>
      <td>{p.runningTasks}</td>
      <td>{p.schedulingMode}</td>
    </tr>
  }
}

